#!/usr/bin/env python
import os
import pickle
import sys
import time
import socket
import random
from optparse import OptionParser
from threading import Thread
import subprocess
from operator import itemgetter
import logging
import signal
import zmq

import mesos
import mesos_pb2

ctx = zmq.Context()

class Task:
    def __init__(self, id):
        self.id = id
        self.tried = 0

REFUSE_FILTER = mesos_pb2.Filters()
REFUSE_FILTER.refuse_seconds = -1

def parse_mem(m):
    try:
        return float(m)
    except ValueError:
        number, unit = float(m[:-1]), m[-1].lower()
        if unit == 'g':
            number *= 1024
        elif unit == 'k':
            number /= 1024
        return number

class SubmitScheduler(mesos.Scheduler):
    def __init__(self, options, command):
        self.framework_name = '[drun@%s] ' % socket.gethostname() + ' '.join(sys.argv[1:])
        self.cpus = options.cpus
        self.mem = parse_mem(options.mem)
        self.options = options
        self.command = command
        self.total_tasks = list(reversed([Task(i)
            for i in range(options.start, options.tasks)]))
        self.task_launched = {}
        self.slaveTasks = {}
        self.started = False
        self.stopped = False
        self.next_try = 0

    def getExecutorInfo(self):
        frameworkDir = os.path.abspath(os.path.dirname(sys.argv[0]))
        executorPath = os.path.join(frameworkDir, "executor")
        execInfo = mesos_pb2.ExecutorInfo()
        execInfo.executor_id.value = "default"
        execInfo.uri = executorPath
        return execInfo

    def create_port(self, output):
        sock = ctx.socket(zmq.PULL)
        host = socket.gethostname()
        port = sock.bind_to_random_port("tcp://0.0.0.0")

        def redirect():
            while True:
                line = sock.recv()
                output.write(line)

        t = Thread(target=redirect)
        t.daemon = True
        t.start()
        return "tcp://%s:%d" % (host, port)

    def registered(self, driver, fid):
        logging.debug("Registered with Mesos, FID = %s" % fid.value)
        self.fid = fid.value
        self.std_port = self.create_port(sys.stdout)
        self.err_port = self.create_port(sys.stderr)

    def getResource(self, offer):
        cpus, mem = 0, 0
        for r in offer.resources:
            if r.name == 'cpus':
                cpus = float(r.scalar.value)
            elif r.name == 'mem':
                mem = float(r.scalar.value)
        return cpus, mem

    def getAttributes(self, offer):
        attrs = {}
        for a in offer.attributes:
            attrs[a.name] = a.text.value
        return attrs

    def resourceOffers(self, driver, offers):
        tpn = self.options.task_per_node
        random.shuffle(offers)
        for offer in offers:
            attrs = self.getAttributes(offer)
            if self.options.group and attrs.get('group', 'None') not in self.options.group:
                driver.launchTasks(offer.id, [], REFUSE_FILTER)
                continue
            
            cpus, mem = self.getResource(offer)
            logging.debug("got resource offer %s: cpus:%s, mem:%s at %s", 
                offer.id.value, cpus, mem, offer.hostname)
            sid = offer.slave_id.value
            tasks = []
            while (self.total_tasks and cpus >= self.cpus and mem >= self.mem
                and (tpn ==0 or
                     tpn > 0 and len(self.slaveTasks.get(sid,set())) < tpn)):
                logging.debug("Accepting slot on slave %s (%s)",
                    offer.slave_id.value, offer.hostname)
                t = self.total_tasks.pop()
                task = self.create_task(offer, t)
                tasks.append(task)
                self.task_launched[t.id] = t
                self.slaveTasks.setdefault(sid, set()).add(t.id)
                cpus -= self.cpus
                mem -= self.mem
                if not self.total_tasks:
                    break
            driver.launchTasks(offer.id, tasks, REFUSE_FILTER)
        self.started = True

    def create_task(self, offer, t):
        task = mesos_pb2.TaskDescription()
        task.task_id.value = str(t.id)
        task.slave_id.value = offer.slave_id.value
        task.name = "task %s/%d" % (t.id, self.options.tasks)
        env = dict(os.environ)
        env['DRUN_RANK'] = str(t.id)
        env['DRUN_SIZE'] = str(self.options.tasks)
        if self.options.expand:
            for i, x in enumerate(self.command):
                self.command[i] = x % {'RANK': t.id, 'SIZE': self.options.tasks}
        task.data = pickle.dumps([os.getcwd(), self.command, env, self.options.shell, self.std_port, self.err_port])

        cpu = task.resources.add()
        cpu.name = "cpus"
        cpu.type = 0 # mesos_pb2.Value.SCALAR
        cpu.scalar.value = self.cpus

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = 0 # mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem
        return task

    def statusUpdate(self, driver, update):
        logging.debug("Task %s in state %d" % (update.task_id.value, update.state))
        if update.state >= mesos_pb2.TASK_FINISHED:
            tid = int(update.task_id.value)
            if tid not in self.task_launched:
                return
            t = self.task_launched.pop(tid)
            slave = None
            for s in self.slaveTasks:
                if tid in self.slaveTasks[s]:
                    slave = s
                    self.slaveTasks[s].remove(tid)
                    break
            if update.state >= mesos_pb2.TASK_FAILED:
                if t.tried < self.options.retry:
                    t.tried += 1
                    logging.warning("task %d failed with %d, retry %d", t.id, update.state, t.tried)
                    if not self.total_tasks:
                        driver.reviveOffers() # request more offers again
                    self.total_tasks.append(t) # try again
                else:
                    logging.error("task %d failed with %d on %s", t.id, update.state, slave)
            if not self.task_launched and not self.total_tasks:
                self.stop(driver) # all done

    def offerRescinded(self, driver, offers):
        for o in offers:
            logging.error("resource rescinded: %s", o)

    def slaveLost(self, driver, slave):
        logging.warning("slave %s lost", slave.value)

    def error(self, driver, code, message):
        logging.error("Error from Mesos: %s (error code: %d)" % (message, code))

    def stop(self, driver):
        driver.stop(False)
        self.stopped = True
        logging.debug("scheduler stopped")


class MPIScheduler(SubmitScheduler):
    def resourceOffers(self, driver, offers):
        for offer in offers:
            cpus, mem = self.getResource(offer)
            logging.debug("got resource offer %s: cpus:%s, mem:%s at %s", 
                offer.id.value, cpus, mem, offer.hostname)
        
        if not self.total_tasks:
            for o in offers:
                driver.launchTasks(o.id, [], REFUSE_FILTER)
            return

        random.shuffle(offers)
        used_offers = []
        need = self.options.tasks
        for offer in offers:
            attrs = self.getAttributes(offer)
            if self.options.group and attrs.get('group', 'None') not in self.options.group:
                continue
            
            cpus, mem = self.getResource(offer)
            slots = min(cpus/self.cpus, mem/self.mem)
            if self.options.task_per_node:
                slots = min((slots, self.options.task_per_node))
            if slots >= 1 :
                used_offers.append((offer, slots))
        if sum(s for o,s in used_offers) < need:
            logging.warning('not enough offers: need %d offer %d', need, sum(s for o,s in used_offers))
            for o in offers:
                driver.launchTasks(o.id, [])
            self.next_try = time.time() + 60    
            return

        hosts = []
        c = 0
        for offer, slots in sorted(used_offers, key=itemgetter(1), reverse=True):
            k = min(need - c, slots)
            hosts.append((offer, k))
            c += k
            if c >= need:
                break
        
        try:
            slaves = self.start_mpi(command, self.options.tasks, hosts)
        except Exception:
            for o in offers:
                driver.launchTasks(o.id, [])
            self.next_try = time.time() + 5 
            return
            
        tasks = {}
        for i, ((offer, k), slave) in enumerate(zip(hosts,slaves)):
            t = Task(i)
            self.task_launched[t.id] = t
            tasks[offer.id.value] = [self.create_task(offer, t, slave.split(' '), k)]

        for o in offers:
            driver.launchTasks(o.id, tasks.get(o.id.value, []), REFUSE_FILTER)
        self.total_tasks = []
        self.started = True

    def create_task(self, offer, t, command, k):
        task = mesos_pb2.TaskDescription()
        task.task_id.value = str(t.id)
        task.slave_id.value = offer.slave_id.value
        task.name = "task %s" % t.id
        env = dict(os.environ)
        task.data = pickle.dumps([os.getcwd(), command, env, self.options.shell, self.std_port, self.err_port])

        cpu = task.resources.add()
        cpu.name = "cpus"
        cpu.type = 0 #mesos_pb2.Value.SCALAR
        cpu.scalar.value = self.cpus * k

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = 0 #mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem * k

        return task

    def start_mpi(self, command, tasks, offers):
        hosts = ','.join("%s:%d" % (offer.hostname, slots) for offer, slots in offers)
        logging.debug("choosed hosts: %s", hosts)
        cmd = ['mpirun', '-prepend-rank', '-launcher', 'none', '-hosts', hosts, '-np', str(tasks)] + command
        self.p = p = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        slaves = []
        prefix = 'HYDRA_LAUNCH: '
        while True:
            line = p.stdout.readline()
            if not line: break
            if line.startswith(prefix):
                slaves.append(line[len(prefix):-1].strip())
            if line == 'HYDRA_LAUNCH_END\n':
                break
        if len(slaves) != len(offers):
            logging.error("offers: %s, slaves: %s", offers, slaves)
            raise Exception("slaves not match with offers")
        def output(f):
            while True:
                line = f.readline()
                if not line: break
                sys.stdout.write(line)
        self.tout = t = Thread(target=output, args=[p.stdout])
        t.deamon = True
        t.start()
        self.terr = t = Thread(target=output, args=[p.stderr])
        t.deamon = True
        t.start()
        return slaves

    def stop(self, driver):
        if self.started:
            self.p.wait()
            self.tout.join()
            self.terr.join()
        driver.stop(False)
        self.stopped = True
        logging.debug("scheduler stopped")

if __name__ == "__main__":
    parser = OptionParser(usage="Usage: %prog [options] <command>")
    parser.allow_interspersed_args=False
    parser.add_option("-s", "--master", type="string",
                default="zoo://zk1:2181,zk2:2181,zk3:2181,zk4:2181,zk5:2181/mesos_master",
                        help="url of master (default: zookeeper")
    parser.add_option("-i", "--mpi", action="store_true",
                        help="run MPI tasks")

    parser.add_option("-n", "--tasks", type="int", default=1,
                        help="number task to launch (default: 1)")
    parser.add_option("-b", "--start", type="int", default=0,
                        help="which task to start (default: 0)")
    parser.add_option("-p", "--task_per_node", type="int", default=0,
                        help="max number of tasks on one node (default: 0)")
    parser.add_option("-r","--retry", type="int", default=0,
                        help="retry times when failed (default: 0)")

    parser.add_option("-c","--cpus", type="float", default=1,
            help="number of CPUs per task (default: 1)")
    parser.add_option("-m","--mem", type="string", default='100m',
            help="MB of memory per task (default: 100m)")
    parser.add_option("-g","--group", type="string", default='',
            help="which group to run (default: ''")


    parser.add_option("--expand", action="store_true",
                        help="expand expression in command line")
    parser.add_option("--shell", action="store_true",
                      help="using shell re-intepret the cmd args")
#    parser.add_option("--kill", type="string", default="",
#                        help="kill a job with frameword id")

    parser.add_option("-q", "--quiet", action="store_true",
                        help="be quiet", )
    parser.add_option("-v", "--verbose", action="store_true",
                        help="show more useful log", )

    (options, command) = parser.parse_args()
    
#    if options.kill:
#        sched = MPIScheduler(options, command)
#        fid = mesos_pb2.FrameworkID()
#        fid.value =  options.kill
#        driver = mesos.MesosSchedulerDriver(sched, sched.framework_name, 
#            sched.getExecutorInfo(), options.master, fid)
#        driver.start()
#        driver.stop(False)
#        os._exit(0)

    if not command:
        parser.print_help()
        exit(2)

    logging.basicConfig(format='[drun] %(asctime)-15s %(message)s',
                    level=options.quiet and logging.WARNING
                        or options.verbose and logging.DEBUG
                        or logging.INFO)

    if options.mpi:
        if options.retry > 0:
            logging.error("MPI application can not retry")
            options.retry = 0
        sched = MPIScheduler(options, command)
    else:
        sched = SubmitScheduler(options, command)

    logging.debug("Connecting to mesos master %s", options.master)
    driver = mesos.MesosSchedulerDriver(sched, sched.framework_name,
        sched.getExecutorInfo(), options.master)

    driver.start()
    def handler(signm, frame):
        logging.warning("got signal %d, exit now", signm)
        sched.stop(driver)
        driver.join()
        sys.exit(1)
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGHUP, handler)
    signal.signal(signal.SIGABRT, handler)
    signal.signal(signal.SIGQUIT, handler)

    try:
        while not sched.stopped:
            time.sleep(1)
            if not sched.started and sched.next_try > 0 and time.time() > sched.next_try:
                driver.reviveOffers()
    except KeyboardInterrupt:
        sched.stop(driver)
        driver.join()
        logging.warning('stopped by KeyboardInterrupt')

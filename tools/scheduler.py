#!/usr/bin/env python
import os
import pickle
import sys
import time
import socket
import random
from optparse import OptionParser
import threading
import subprocess
from operator import itemgetter
import logging
import signal

import zmq
ctx = zmq.Context()

import pymesos as mesos
from mesos.interface import mesos_pb2
import dpark.conf as conf
from dpark.util import getuser
from dpark import moosefs

logger = logging.getLogger('dpark.scheduler')

class Task:
    def __init__(self, id):
        self.id = id
        self.tried = 0
        self.state = -1
        self.state_time = 0

REFUSE_FILTER = mesos_pb2.Filters()
REFUSE_FILTER.refuse_seconds = 10*60 # 10 mins

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

def safe(f):
    def _(self, *a, **kw):
        with self.lock:
            r = f(self, *a, **kw)
        return r
    return _


class SubmitScheduler(object):
    def __init__(self, options, command):
        self.framework_id = None
        self.executor = None
        self.framework = mesos_pb2.FrameworkInfo()
        self.framework.user = getuser()
        if self.framework.user == 'root':
            raise Exception("drun is not allowed to run as 'root'")
        name = '[drun] ' + ' '.join(sys.argv[1:])
        if len(name) > 256:
            name = name[:256] + '...'
        self.framework.name = name
        self.framework.hostname = socket.gethostname()
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
        self.status = 0
        self.next_try = 0
        self.lock = threading.RLock()
        self.last_offer_time = time.time()

    def getExecutorInfo(self):
        frameworkDir = os.path.abspath(os.path.dirname(sys.argv[0]))
        executorPath = os.path.join(frameworkDir, "executor.py")
        execInfo = mesos_pb2.ExecutorInfo()
        execInfo.executor_id.value = "default"
        execInfo.command.value = executorPath
        if hasattr(execInfo, 'framework_id'):
            execInfo.framework_id.value = str(self.framework_id)
        return execInfo

    def create_port(self, output):
        sock = ctx.socket(zmq.PULL)
        host = socket.gethostname()
        port = sock.bind_to_random_port("tcp://0.0.0.0")

        def redirect():
            poller = zmq.Poller()
            poller.register(sock, zmq.POLLIN)
            while True:
                socks = poller.poll(100)
                if not socks:
                    if self.stopped:
                        break
                    continue
                line = sock.recv()
                output.write(line)

        t = threading.Thread(target=redirect)
        t.daemon = True
        t.start()
        return t, "tcp://%s:%d" % (host, port)

    @safe
    def registered(self, driver, fid, masterInfo):
        logger.debug("Registered with Mesos, FID = %s" % fid.value)
        self.framework_id = fid.value
        self.executor = self.getExecutorInfo()
        self.std_t, self.std_port = self.create_port(sys.stdout)
        self.err_t, self.err_port = self.create_port(sys.stderr)

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

    @safe
    def resourceOffers(self, driver, offers):
        tpn = self.options.task_per_node
        random.shuffle(offers)
        self.last_offer_time = time.time()
        for offer in offers:
            attrs = self.getAttributes(offer)
            if self.options.group and attrs.get('group', 'None') not in self.options.group:
                driver.launchTasks(offer.id, [], REFUSE_FILTER)
                continue

            cpus, mem = self.getResource(offer)
            logger.debug("got resource offer %s: cpus:%s, mem:%s at %s",
                offer.id.value, cpus, mem, offer.hostname)
            sid = offer.slave_id.value
            tasks = []
            while (self.total_tasks and cpus+1e-4 >= self.cpus and mem >= self.mem
                    and (tpn ==0 or tpn > 0 and len(self.slaveTasks.get(sid,set())) < tpn)):
                logger.debug("Accepting slot on slave %s (%s)",
                    offer.slave_id.value, offer.hostname)
                t = self.total_tasks.pop()
                task = self.create_task(offer, t, cpus)
                tasks.append(task)
                t.state = mesos_pb2.TASK_STARTING
                t.state_time = time.time()
                self.task_launched[t.id] = t
                self.slaveTasks.setdefault(sid, set()).add(t.id)
                cpus -= self.cpus
                mem -= self.mem
                if not self.total_tasks:
                    break

            logger.debug("dispatch %d tasks to slave %s", len(tasks), offer.hostname)
            driver.launchTasks(offer.id, tasks, REFUSE_FILTER)

    def create_task(self, offer, t, cpus):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = "%d-%d" % (t.id, t.tried)
        task.slave_id.value = offer.slave_id.value
        task.name = "task %s/%d" % (t.id, self.options.tasks)
        task.executor.MergeFrom(self.executor)
        env = dict(os.environ)
        env['DRUN_RANK'] = str(t.id)
        env['DRUN_SIZE'] = str(self.options.tasks)
        command = self.command[:]
        if self.options.expand:
            for i, x in enumerate(command):
                command[i] = x % {'RANK': t.id, 'SIZE': self.options.tasks}
        task.data = pickle.dumps([os.getcwd(), command, env, self.options.shell,
            self.std_port, self.err_port, None])

        cpu = task.resources.add()
        cpu.name = "cpus"
        cpu.type = 0 # mesos_pb2.Value.SCALAR
        cpu.scalar.value = min(self.cpus, cpus)

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = 0 # mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem
        return task

    @safe
    def statusUpdate(self, driver, update):
        logger.debug("Task %s in state %d" % (update.task_id.value, update.state))
        tid = int(update.task_id.value.split('-')[0])
        if tid not in self.task_launched:
            # check failed after launched
            for t in self.total_tasks:
                if t.id == tid:
                    self.task_launched[tid] = t
                    self.total_tasks.remove(t)
                    break
            else:
                logger.debug("Task %d is finished, ignore it", tid)
                return

        t = self.task_launched[tid]
        t.state = update.state
        t.state_time = time.time()

        if update.state == mesos_pb2.TASK_RUNNING:
            self.started = True

        elif update.state == mesos_pb2.TASK_LOST:
            logger.warning("Task %s was lost, try again", tid)
            if not self.total_tasks:
                driver.reviveOffers() # request more offers again
            t.tried += 1
            t.state = -1
            self.task_launched.pop(tid)
            self.total_tasks.append(t)

        elif update.state in (mesos_pb2.TASK_FINISHED, mesos_pb2.TASK_FAILED):
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
                    logger.warning("task %d failed with %d, retry %d", t.id, update.state, t.tried)
                    if not self.total_tasks:
                        driver.reviveOffers() # request more offers again
                    self.total_tasks.append(t) # try again
                else:
                    logger.error("task %d failed with %d on %s", t.id, update.state, slave)
                    self.stop(1)

            if not self.task_launched and not self.total_tasks:
                self.stop(0)

    @safe
    def check(self, driver):
        now = time.time()
        for tid, t in self.task_launched.items():
            if t.state == mesos_pb2.TASK_STARTING and t.state_time + 30 < now:
                logger.warning("task %d lauched failed, assign again", tid)
                if not self.total_tasks:
                    driver.reviveOffers() # request more offers again
                t.tried += 1
                t.state = -1
                self.task_launched.pop(tid)
                self.total_tasks.append(t)
            # TODO: check run time

    @safe
    def offerRescinded(self, driver, offer):
        logger.debug("resource rescinded: %s", offer)
        # task will retry by checking

    @safe
    def slaveLost(self, driver, slave):
        logger.warning("slave %s lost", slave.value)

    @safe
    def error(self, driver, code, message):
        logger.error("Error from Mesos: %s (error code: %d)" % (message, code))

    @safe
    def stop(self, status):
        if self.stopped:
            return
        self.stopped = True
        self.status = status
        self.std_t.join()
        self.err_t.join()
        logger.debug("scheduler stopped")


class MPIScheduler(SubmitScheduler):
    def __init__(self, options, command):
        SubmitScheduler.__init__(self, options, command)
        self.used_hosts = {}
        self.used_tasks = {}
        self.id = 0
        self.p = None
        self.publisher = ctx.socket(zmq.PUB)
        port = self.publisher.bind_to_random_port('tcp://0.0.0.0')
        host = socket.gethostname()
        self.publisher_port = 'tcp://%s:%d' % (host, port)

    def start_task(self, driver, offer, k):
        t = Task(self.id)
        self.id += 1
        self.task_launched[t.id] = t
        self.used_tasks[t.id] = (offer.hostname, k)
        task = self.create_task(offer, t, k)
        logger.debug("lauching %s task with offer %s on %s, slots %d", t.id,
                     offer.id.value, offer.hostname, k)
        driver.launchTasks(offer.id, [task], REFUSE_FILTER)

    @safe
    def resourceOffers(self, driver, offers):
        random.shuffle(offers)
        launched = sum(self.used_hosts.values())
        self.last_offer_time = time.time()

        for offer in offers:
            cpus, mem = self.getResource(offer)
            logger.debug("got resource offer %s: cpus:%s, mem:%s at %s",
                offer.id.value, cpus, mem, offer.hostname)
            if launched >= self.options.tasks or offer.hostname in self.used_hosts:
                driver.launchTasks(offer.id, [], REFUSE_FILTER)
                continue

            attrs = self.getAttributes(offer)
            if self.options.group and attrs.get('group', 'None') not in self.options.group:
                driver.launchTasks(offer.id, [], REFUSE_FILTER)
                continue

            slots = int(min(cpus/self.cpus, mem/self.mem) + 1e-5)
            if self.options.task_per_node:
                slots = min(slots, self.options.task_per_node)
            slots = min(slots, self.options.tasks - launched)
            if slots >= 1:
                self.used_hosts[offer.hostname] = slots
                launched += slots
                self.start_task(driver, offer, slots)
            else:
                driver.launchTasks(offer.id, [], REFUSE_FILTER)

        if launched < self.options.tasks:
            logger.warning('not enough offers: need %d offer %d, waiting more resources',
                            self.options.tasks, launched)

    @safe
    def statusUpdate(self, driver, update):
        logger.debug("Task %s in state %d" % (update.task_id.value, update.state))
        tid = int(update.task_id.value.split('-')[0])
        if tid not in self.task_launched:
            logger.error("Task %d not in task_launched", tid)
            return

        t = self.task_launched[tid]
        t.state = update.state
        t.state_time = time.time()
        hostname, slots = self.used_tasks[tid]

        if update.state == mesos_pb2.TASK_RUNNING:
            launched = sum(self.used_hosts.values())
            ready = all(t.state == mesos_pb2.TASK_RUNNING for t in self.task_launched.values())
            if launched == self.options.tasks and ready:
                logger.debug("all tasks are ready, start to run")
                self.start_mpi()

        elif update.state in (mesos_pb2.TASK_LOST, mesos_pb2.TASK_FAILED):
            if not self.started:
                logger.warning("Task %s was lost, try again", tid)
                driver.reviveOffers() # request more offers again
                t.tried += 1
                t.state = -1
                self.used_hosts.pop(hostname)
                self.used_tasks.pop(tid)
                self.task_launched.pop(tid)
            else:
                logger.error("Task %s failed, cancel all tasks", tid)
                self.stop(1)

        elif update.state == mesos_pb2.TASK_FINISHED:
            if not self.started:
                logger.warning("Task %s has not started, ignore it %s", tid, update.state)
                return

            t = self.task_launched.pop(tid)
            if not self.task_launched:
                self.stop(0)

    @safe
    def check(self, driver):
        now = time.time()
        for tid, t in self.task_launched.items():
            if t.state == mesos_pb2.TASK_STARTING and t.state_time + 30 < now:
                logger.warning("task %d lauched failed, assign again", tid)
                driver.reviveOffers() # request more offers again
                t.tried += 1
                t.state = -1
                hostname, slots = self.used_tasks[tid]
                self.used_hosts.pop(hostname)
                self.used_tasks.pop(tid)
                self.task_launched.pop(tid)

    def create_task(self, offer, t, k):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = "%s-%s" % (t.id, t.tried)
        task.slave_id.value = offer.slave_id.value
        task.name = "task %s" % t.id
        task.executor.MergeFrom(self.executor)
        env = dict(os.environ)
        task.data = pickle.dumps([os.getcwd(), None, env, self.options.shell, self.std_port, self.err_port, self.publisher_port])

        cpus, mem = self.getResource(offer)
        cpu = task.resources.add()
        cpu.name = "cpus"
        cpu.type = 0 #mesos_pb2.Value.SCALAR
        cpu.scalar.value = min(self.cpus * k, cpus)

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = 0 #mesos_pb2.Value.SCALAR
        mem.scalar.value = min(self.mem * k, mem)

        return task

    def start_mpi(self):
        try:
            slaves = self.try_to_start_mpi(self.command, self.options.tasks, self.used_hosts.items())
        except Exception:
            self.broadcast_command({})
            self.next_try = time.time() + random.randint(5,10)
            return

        commands = dict(zip(self.used_hosts.keys(), slaves))
        self.broadcast_command(commands)
        self.started = True

    def broadcast_command(self, command):
        def repeat_pub():
            for i in xrange(10):
                self.publisher.send(pickle.dumps(command))
                time.sleep(1)
                if self.stopped: break

        t = threading.Thread(target=repeat_pub)
        t.deamon = True
        t.start()
        return t

    def try_to_start_mpi(self, command, tasks, items):
        if self.p:
            try: self.p.kill()
            except: pass
        hosts = ','.join("%s:%d" % (hostname, slots) for hostname, slots in items)
        logger.debug("choosed hosts: %s", hosts)
        cmd = ['mpirun', '-prepend-rank', '-launcher', 'none', '-hosts', hosts, '-np', str(tasks)] + command
        self.p = p = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE)
        slaves = []
        prefix = 'HYDRA_LAUNCH: '
        while True:
            line = p.stdout.readline()
            if not line: break
            if line.startswith(prefix):
                slaves.append(line[len(prefix):-1].strip())
            if line == 'HYDRA_LAUNCH_END\n':
                break
        if len(slaves) != len(items):
            logger.error("hosts: %s, slaves: %s", items, slaves)
            raise Exception("slaves not match with hosts")
        def output(f):
            while True:
                line = f.readline()
                if not line: break
                sys.stdout.write(line)
        self.tout = t = threading.Thread(target=output, args=[p.stdout])
        t.deamon = True
        t.start()
        return slaves

    @safe
    def stop(self, status):
        if self.started:
            try:
                self.p.kill()
                self.p.wait()
            except: pass
            self.tout.join()
        self.publisher.close()
        super(MPIScheduler, self).stop(status)


if __name__ == "__main__":
    parser = OptionParser(usage="Usage: drun [options] <command>")
    parser.allow_interspersed_args=False
    parser.add_option("-s", "--master", type="string",
                default="mesos",
                        help="url of master (default: mesos)")
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
    parser.add_option("-t", "--timeout", type="int", default=3600*24,
                        help="timeout of job in seconds (default: 86400)")

    parser.add_option("-c","--cpus", type="float", default=1.0,
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

    if 'DPARK_CONF' in os.environ:
        conf.load_conf(os.environ['DPARK_CONF'])
    elif os.path.exists('/etc/dpark.conf'):
        conf.load_conf('/etc/dpark.conf')

    conf.__dict__.update(os.environ)
    moosefs.MFS_PREFIX = conf.MOOSEFS_MOUNT_POINTS
    moosefs.master.ENABLE_DCACHE = conf.MOOSEFS_DIR_CACHE

    if options.master == 'mesos':
        options.master = conf.MESOS_MASTER
    elif options.master.startswith('mesos://'):
        if '@' in options.master:
            options.master = options.master[options.master.rfind('@')+1:]
        else:
            options.master = options.master[options.master.rfind('//')+2:]
    elif options.master.startswith('zoo://'):
        options.master = 'zk' + options.master[3:]

    if ':' not in options.master:
        options.master += ':5050'

#    if options.kill:
#        sched = MPIScheduler(options, command)
#        fid = mesos_pb2.FrameworkID()
#        fid.value =  options.kill
#        driver = mesos.MesosSchedulerDriver(sched, sched.framework,
#            options.master, fid)
#        driver.start()
#        driver.stop(False)
#        sys.exit(0)

    if not command:
        parser.print_help()
        sys.exit(2)

    logging.basicConfig(format='[drun] %(threadName)s %(asctime)-15s %(message)s',
                    level=options.quiet and logging.ERROR
                        or options.verbose and logging.DEBUG
                        or logging.WARNING)

    if options.mpi:
        if options.retry > 0:
            logger.error("MPI application can not retry")
            options.retry = 0
        sched = MPIScheduler(options, command)
    else:
        sched = SubmitScheduler(options, command)

    logger.debug("Connecting to mesos master %s", options.master)
    driver = mesos.MesosSchedulerDriver(sched, sched.framework,
        options.master)

    driver.start()
    def handler(signm, frame):
        logger.warning("got signal %d, exit now", signm)
        sched.stop(3)
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGHUP, handler)
    signal.signal(signal.SIGABRT, handler)
    signal.signal(signal.SIGQUIT, handler)

    try:
        from rfoo.utils import rconsole
        rconsole.spawn_server(locals(), 0)
    except ImportError:
        pass

    start = time.time()
    try:
        while not sched.stopped:
            time.sleep(0.1)

            now = time.time()
            sched.check(driver)
            if not sched.started and sched.next_try > 0 and now > sched.next_try:
                sched.next_try = 0
                driver.reviveOffers()

            if not sched.started and now > sched.last_offer_time + 60 + random.randint(0,5):
                logger.warning("too long to get offer, reviving...")
                sched.last_offer_time = now
                driver.reviveOffers()

            if now - start > options.timeout:
                logger.warning("job timeout in %d seconds", options.timeout)
                sched.stop(2)
                break

    except KeyboardInterrupt:
        logger.warning('stopped by KeyboardInterrupt')
        sched.stop(4)

    driver.stop(False)
    ctx.term()
    sys.exit(sched.status)

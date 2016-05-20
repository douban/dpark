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
import logging
import signal

import zmq
ctx = zmq.Context()

import pymesos as mesos
from mesos.interface import mesos_pb2
import dpark.conf as conf
from dpark.util import getuser, memory_str_to_mb
from dpark import moosefs

logger = logging.getLogger('dpark.scheduler')


class Task:

    def __init__(self, id):
        self.id = id
        self.tried = 0
        self.state = -1
        self.state_time = 0

REFUSE_FILTER = mesos_pb2.Filters()
REFUSE_FILTER.refuse_seconds = 10 * 60  # 10 mins
EXECUTOR_CPUS = 0.01
EXECUTOR_MEMORY = 64  # cache


def safe(f):
    def _(self, *a, **kw):
        with self.lock:
            r = f(self, *a, **kw)
        return r
    return _


class BaseScheduler(object):

    def __init__(self, name, options, command):
        self.framework_id = None
        self.executor = None
        self.framework = mesos_pb2.FrameworkInfo()
        self.framework.user = getuser()
        if self.framework.user == 'root':
            raise Exception("drun is not allowed to run as 'root'")

        self.framework.name = name
        self.framework.hostname = socket.gethostname()
        self.cpus = options.cpus
        self.mem = memory_str_to_mb(options.mem)
        self.options = options
        self.command = command
        self.started = False
        self.stopped = False
        self.status = 0
        self.next_try = 0
        self.lock = threading.RLock()
        self.last_offer_time = time.time()
        self.task_launched = {}
        self.slaveTasks = {}

    def getExecutorInfo(self):
        frameworkDir = os.path.abspath(os.path.dirname(sys.argv[0]))
        executorPath = os.path.join(frameworkDir, "executor.py")
        execInfo = mesos_pb2.ExecutorInfo()
        execInfo.executor_id.value = "default"

        execInfo.command.value = executorPath
        v = execInfo.command.environment.variables.add()
        v.name = 'UID'
        v.value = str(os.getuid())
        v = execInfo.command.environment.variables.add()
        v.name = 'GID'
        v.value = str(os.getgid())

        mem = execInfo.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = EXECUTOR_MEMORY
        cpus = execInfo.resources.add()
        cpus.name = 'cpus'
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = EXECUTOR_CPUS

        if hasattr(execInfo, 'framework_id'):
            execInfo.framework_id.value = str(self.framework_id)

        if self.options.image and hasattr(execInfo, 'container'):
            execInfo.container.type = mesos_pb2.ContainerInfo.DOCKER
            execInfo.container.docker.image = self.options.image

            for path in ['/etc/passwd', '/etc/group']:
                v = execInfo.container.volumes.add()
                v.host_path = v.container_path = path
                v.mode = mesos_pb2.Volume.RO

            for path in conf.MOOSEFS_MOUNT_POINTS:
                v = execInfo.container.volumes.add()
                v.host_path = v.container_path = path
                v.mode = mesos_pb2.Volume.RW

            if self.options.volumes:
                for volume in self.options.volumes.split(','):
                    fields = volume.split(':')
                    if len(fields) == 3:
                        host_path, container_path, mode = fields
                        mode = mesos_pb2.Volume.RO if mode.lower() == 'ro' else mesos_pb2.Volume.RW
                    elif len(fields) == 2:
                        host_path, container_path = fields
                        mode = mesos_pb2.Volume.RW
                    elif len(fields) == 1:
                        container_path, = fields
                        host_path = ''
                        mode = mesos_pb2.Volume.RW
                    else:
                        raise Exception("cannot parse volume %s", volume)

                    try:
                        os.makedirs(host_path)
                    except OSError:
                        pass

                v = execInfo.container.volumes.add()
                v.container_path = container_path
                v.mode = mode
                if host_path:
                    v.host_path = host_path

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

    def kill_task(self, driver, t):
        task_id = mesos_pb2.TaskID()
        task_id.value = "%s-%s" % (t.id, t.tried)
        driver.killTask(task_id)

    @safe
    def registered(self, driver, fid, masterInfo):
        logger.debug("Registered with Mesos, FID = %s" % fid.value)
        self.framework_id = fid.value
        self.executor = self.getExecutorInfo()
        self.std_t, self.std_port = self.create_port(sys.stdout)
        self.err_t, self.err_port = self.create_port(sys.stderr)

    @safe
    def offerRescinded(self, driver, offer):
        logger.debug("resource rescinded: %s", offer)

    @safe
    def frameworkMessage(self, driver, executorId, slaveId, data):
        logger.warning("[slave %s] %s", slaveId.value, data)

    @safe
    def executorLost(self, driver, executorId, slaveId, status):
        logger.warning(
            "executor at %s %s lost: %s",
            slaveId.value,
            executorId.value,
            status)
        self.slaveLost(driver, slaveId)

    @safe
    def slaveLost(self, driver, slaveId):
        logger.warning("slave %s lost", slaveId.value)
        sid = slaveId.value
        if sid in self.slaveTasks:
            for tid in self.slaveTasks[sid]:
                if tid in self.task_launched:
                    logger.warning("Task %d killed for slave lost", tid)
                    self.kill_task(driver, self.task_launched[tid])

    @safe
    def error(self, driver, code, message):
        logger.error("Error from Mesos: %s (error code: %d)" % (message, code))

    @safe
    def check(self, driver):
        now = time.time()
        for t in self.task_launched.values():
            if t.state == mesos_pb2.TASK_STARTING and t.state_time + 30 < now:
                logger.warning("task %d lauched failed, assign again", t.id)
                self.kill_task(driver, t)

    @safe
    def stop(self, status):
        if self.stopped:
            return
        self.stopped = True
        self.status = status
        self.std_t.join()
        self.err_t.join()
        logger.debug("scheduler stopped")


class SubmitScheduler(BaseScheduler):

    def __init__(self, options, command):
        name = '[drun] ' + ' '.join(sys.argv[1:])
        if len(name) > 256:
            name = name[:256] + '...'

        super(SubmitScheduler, self).__init__(name, options, command)
        self.total_tasks = list(reversed([Task(i)
                                          for i in range(options.start, options.tasks)]))

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
            while (self.total_tasks and cpus >= self.cpus + EXECUTOR_CPUS and mem >= self.mem + EXECUTOR_MEMORY
                    and (tpn == 0 or tpn > 0 and len(self.slaveTasks.get(sid, set())) < tpn)):
                logger.debug("Accepting slot on slave %s (%s)",
                             offer.slave_id.value, offer.hostname)
                t = self.total_tasks.pop()
                task = self.create_task(offer, t)
                tasks.append(task)
                t.state = mesos_pb2.TASK_STARTING
                t.state_time = time.time()
                self.task_launched[t.id] = t
                self.slaveTasks.setdefault(sid, set()).add(t.id)
                cpus -= self.cpus
                mem -= self.mem
                if not self.total_tasks:
                    break

            logger.debug(
                "dispatch %d tasks to slave %s",
                len(tasks),
                offer.hostname)
            driver.launchTasks(offer.id, tasks, REFUSE_FILTER)

    def create_task(self, offer, t):
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
        cpu.type = mesos_pb2.Value.SCALAR
        cpu.scalar.value = self.cpus

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem
        return task

    @safe
    def statusUpdate(self, driver, update):
        logger.debug(
            "Task %s in state %d" %
            (update.task_id.value, update.state))
        tid = int(update.task_id.value.split('-')[0])
        if tid not in self.task_launched:
            for t in self.total_tasks:
                logger.error("Task %d not in task_launched", tid)
                return

        t = self.task_launched[tid]
        t.state = update.state
        t.state_time = time.time()

        if update.state == mesos_pb2.TASK_RUNNING:
            self.started = True
            return

        del self.task_launched[tid]
        sid = next(
            (sid for sid in self.slaveTasks if tid in self.slaveTasks[sid]),
            None)
        if sid:
            self.slaveTasks[sid].remove(tid)

        if update.state != mesos_pb2.TASK_FINISHED:
            message = getattr(update, 'message', '')
            if t.tried < self.options.retry:
                logger.warning("Task %d %s with %d, retry %d: %s", t.id,
                               'Lost' if update.state == mesos_pb2.TASK_LOST else "Failed",
                               update.state, t.tried, message)
                t.tried += 1
                t.state = -1
                self.total_tasks.append(t)  # try again
            else:
                logger.error("Task %d %s with %d on %s: %s", t.id,
                             'Lost' if update.state == mesos_pb2.TASK_LOST else "Failed",
                             update.state, sid, message)
                self.stop(1)
                return

        if self.total_tasks:
            driver.reviveOffers()  # request more offers again

        if not self.task_launched and not self.total_tasks:
            self.stop(0)


class MPIScheduler(BaseScheduler):

    def __init__(self, options, command):
        name = '[mrun] ' + ' '.join(sys.argv[1:])
        if len(name) > 256:
            name = name[:256] + '...'

        super(MPIScheduler, self).__init__(name, options, command)
        self.used_tasks = {}
        self.id = 0
        self.p = None
        self.publisher = ctx.socket(zmq.PUB)
        port = self.publisher.bind_to_random_port('tcp://0.0.0.0')
        host = socket.gethostname()
        self.publisher_port = 'tcp://%s:%d' % (host, port)

    def start_task(self, driver, offer, k):
        t = Task(self.id)
        sid = offer.slave_id.value
        self.id += 1
        self.task_launched[t.id] = t
        self.used_tasks[t.id] = (offer.hostname, k)
        self.slaveTasks.setdefault(sid, set()).add(t.id)
        task = self.create_task(offer, t, k)
        logger.debug("lauching %s task with offer %s on %s, slots %d", t.id,
                     offer.id.value, offer.hostname, k)
        driver.launchTasks(offer.id, [task], REFUSE_FILTER)

    @safe
    def resourceOffers(self, driver, offers):
        random.shuffle(offers)
        self.last_offer_time = time.time()
        launched = 0
        used_hosts = set()
        for hostname, slots in self.used_tasks.itervalues():
            used_hosts.add(hostname)
            launched += slots

        for offer in offers:
            cpus, mem = self.getResource(offer)
            logger.debug("got resource offer %s: cpus:%s, mem:%s at %s",
                         offer.id.value, cpus, mem, offer.hostname)
            if launched >= self.options.tasks or offer.hostname in used_hosts:
                driver.launchTasks(offer.id, [], REFUSE_FILTER)
                continue

            attrs = self.getAttributes(offer)
            if self.options.group and attrs.get('group', 'None') not in self.options.group:
                driver.launchTasks(offer.id, [], REFUSE_FILTER)
                continue

            slots = int(min((cpus - EXECUTOR_CPUS) / self.cpus, (mem - EXECUTOR_MEMORY) / self.mem))
            if self.options.task_per_node:
                slots = min(slots, self.options.task_per_node)
            slots = min(slots, self.options.tasks - launched)
            if slots >= 1:
                launched += slots
                used_hosts.add(offer.hostname)
                self.start_task(driver, offer, slots)
            else:
                driver.launchTasks(offer.id, [], REFUSE_FILTER)

        if launched < self.options.tasks:
            logger.warning('not enough offers: need %d offer %d, waiting more resources',
                           self.options.tasks, launched)

    @safe
    def statusUpdate(self, driver, update):
        logger.debug(
            "Task %s in state %d" %
            (update.task_id.value, update.state))
        tid = int(update.task_id.value.split('-')[0])
        if tid not in self.task_launched:
            logger.error("Task %d not in task_launched", tid)
            return

        t = self.task_launched[tid]
        t.state = update.state
        t.state_time = time.time()
        if update.state == mesos_pb2.TASK_RUNNING:
            launched = sum(
                slots for hostname, slots in self.used_tasks.values())
            ready = all(
                t.state == mesos_pb2.TASK_RUNNING for t in self.task_launched.values())
            if launched == self.options.tasks and ready:
                logger.debug("all tasks are ready, start to run")
                self.start_mpi()

            return

        del self.task_launched[tid]
        sid = next(
            (sid for sid in self.slaveTasks if tid in self.slaveTasks[sid]),
            None)
        if sid:
            self.slaveTasks[sid].remove(tid)

        if update.state != mesos_pb2.TASK_FINISHED:
            if not self.started:
                logger.warning("Task %d %s with %d, retry %d", t.id,
                               'Lost' if update.state == mesos_pb2.TASK_LOST else "Failed",
                               update.state, t.tried)
                driver.reviveOffers()  # request more offers again
                self.used_tasks.pop(tid)

            else:
                logger.error("Task %s failed, cancel all tasks", tid)
                self.stop(1)

        else:
            if not self.started:
                logger.warning(
                    "Task %s has not started, ignore it %s",
                    tid,
                    update.state)
                return

            if not self.task_launched:
                self.stop(0)

    def create_task(self, offer, t, k):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = "%s-%s" % (t.id, t.tried)
        task.slave_id.value = offer.slave_id.value
        task.name = "task %s" % t.id
        task.executor.MergeFrom(self.executor)
        env = dict(os.environ)
        task.data = pickle.dumps([os.getcwd(),
                                  None,
                                  env,
                                  self.options.shell,
                                  self.std_port,
                                  self.err_port,
                                  self.publisher_port])

        cpu = task.resources.add()
        cpu.name = "cpus"
        cpu.type = mesos_pb2.Value.SCALAR
        cpu.scalar.value = self.cpus * k

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem * k

        return task

    def start_mpi(self):
        try:
            commands = self.try_to_start_mpi(
                self.command, self.options.tasks, self.used_tasks.values())
        except Exception:
            logger.exception("Failed to start mpi, retry")
            self.broadcast_command({})
            self.next_try = time.time() + random.randint(5, 10)
            return

        self.broadcast_command(commands)
        self.started = True

    def broadcast_command(self, command):
        def repeat_pub():
            for i in xrange(10):
                self.publisher.send(pickle.dumps(command))
                time.sleep(1)
                if self.stopped:
                    break

        t = threading.Thread(target=repeat_pub)
        t.deamon = True
        t.start()
        return t

    def try_to_start_mpi(self, command, tasks, items):
        if self.p:
            try:
                self.p.kill()
            except:
                pass

        hosts = ','.join("%s:%d" % (hostname, slots)
                         for hostname, slots in items)
        logger.debug("choosed hosts: %s", hosts)
        info = subprocess.check_output(["mpirun", "--version"])
        for line in info.splitlines():
            if 'Launchers available' in line and ' none ' in line:
                #MPICH2 1.x
                cmd = ['mpirun', '-prepend-rank', '-launcher', 'none',
                       '-hosts', hosts, '-np', str(tasks)] + command
                break

        else:
            #MPICH2 3.x
            cmd = ['mpirun', '-prepend-rank', '-launcher', 'manual',
                   '-rmk', 'user', '-hosts', hosts, '-np', str(tasks)] \
                    + command

        self.p = p = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE)
        slaves = []
        prefix = 'HYDRA_LAUNCH: '
        while True:
            line = p.stdout.readline()
            if not line:
                break
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
                if not line:
                    break
                sys.stdout.write(line)
        self.tout = t = threading.Thread(target=output, args=[p.stdout])
        t.deamon = True
        t.start()
        return dict(zip((hostname for hostname, slots in items), slaves))

    @safe
    def stop(self, status):
        if self.started:
            try:
                self.p.kill()
                self.p.wait()
            except:
                pass
            self.tout.join()
        self.publisher.close()
        super(MPIScheduler, self).stop(status)


if __name__ == "__main__":
    parser = OptionParser(usage="Usage: drun [options] <command>")
    parser.allow_interspersed_args = False
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
    parser.add_option("-r", "--retry", type="int", default=0,
                      help="retry times when failed (default: 0)")
    parser.add_option("-t", "--timeout", type="int", default=3600 * 24,
                      help="timeout of job in seconds (default: 86400)")

    parser.add_option("-c", "--cpus", type="float", default=1.0,
                      help="number of CPUs per task (default: 1)")
    parser.add_option("-m", "--mem", type="string", default='100m',
                      help="MB of memory per task (default: 100m)")
    parser.add_option("-g", "--group", type="string", default='',
                      help="which group to run (default: ''")

    parser.add_option("-I", "--image", type="string",
                      help="image name for Docker")
    parser.add_option("-V", "--volumes", type="string",
                      help="volumes to mount into Docker")

    parser.add_option("--expand", action="store_true",
                      help="expand expression in command line")
    parser.add_option("--shell", action="store_true",
                      help="using shell re-intepret the cmd args")

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
            options.master = options.master[options.master.rfind('@') + 1:]
        else:
            options.master = options.master[options.master.rfind('//') + 2:]
    elif options.master.startswith('zoo://'):
        options.master = 'zk' + options.master[3:]

    if ':' not in options.master:
        options.master += ':5050'

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

            if not sched.started and now > sched.last_offer_time + \
                    60 + random.randint(0, 5):
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

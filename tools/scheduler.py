#!/usr/bin/env python
from __future__ import absolute_import
import os
import sys
import time
import pickle
import random
import signal
import socket
import logging
import threading
import subprocess
import json

import zmq
import six
from six.moves import range
from six.moves import zip

ctx = zmq.Context()

from addict import Dict
from optparse import OptionParser
from pymesos import MesosSchedulerDriver, encode_data
import dpark.conf as conf
from dpark.utils import getuser, memory_str_to_mb, sec2nanosec
from dpark.utils.debug import spawn_rconsole
from dpark.utils.log import add_loghub, create_logger

logger = logging.getLogger('dpark.scheduler')


class Task:

    def __init__(self, id):
        self.id = id
        self.tried = 0
        self.state = -1
        self.state_time = 0


REFUSE_FILTER = Dict()
REFUSE_FILTER.refuse_seconds = 10 * 60  # 10 mins
EXECUTOR_CPUS = 0.01
EXECUTOR_MEMORY = 64  # cache

EXIT_UNEXPECTED = -1
EXIT_NOMAL = 0
EXIT_TASKFAIL = 1
EXIT_TIMEOUT = 2
EXIT_SIGNAL = 3
EXIT_KEYBORAD = 4
EXIT_EXCEPTION = 5


def safe(f):
    def _(self, *a, **kw):
        with self.lock:
            r = f(self, *a, **kw)
        return r

    return _


def safejoin(t):
    if t:
        t.join()


class BaseScheduler(object):

    def __init__(self, name, options, command, role=None):
        self.framework_id = None
        self.executor = None
        self.framework = Dict()
        self.framework.user = getuser()
        if self.framework.user == 'root':
            raise Exception('drun is not allowed to run as \'root\'')

        self.framework.name = name
        self.framework.hostname = socket.gethostname()
        if role is not None:
            self.framework.role = role
        self.cpus = options.cpus
        self.gpus = options.gpus
        self.mem = memory_str_to_mb(options.mem)
        self.options = options
        self.command = command
        self.started = False
        self.stopped = False
        self.ec = EXIT_UNEXPECTED
        self.next_try = 0
        self.lock = threading.RLock()
        self.last_offer_time = time.time()
        self.task_launched = {}
        self.agentTasks = {}

        # threads
        self.stdout_t = None
        self.stderr_t = None

        self.loghub_dir = None
        self.stats = {'ec': EXIT_UNEXPECTED,
                      'init_time': time.time(),
                      'submit_times': {},  # bind offers
                      'start_time': None,  # mrun only: get all resources
                      'slots': {},  # empty for drun
                      'finish_times': {},
                      'stop_time': None,
                      'num_task': options.tasks,
                      'mem_mb': self.mem,
                      'scheduler': self.short_name,
                      'cmd': os.path.abspath(sys.argv[0]) + ' ' + ' '.join(sys.argv[1:])
                      }

    @property
    def short_name(self):
        return None

    def getExecutorInfo(self):
        frameworkDir = os.path.abspath(os.path.dirname(sys.argv[0]))
        executorPath = os.path.join(frameworkDir, 'executor.py')
        execInfo = Dict()
        execInfo.executor_id.value = 'default'

        execInfo.command.value = '%s %s' % (sys.executable, executorPath)
        execInfo.command.environment.variables = variables = []

        v = Dict()
        variables.append(v)
        v.name = 'UID'
        v.value = str(os.getuid())

        v = Dict()
        variables.append(v)
        v.name = 'GID'
        v.value = str(os.getgid())

        execInfo.resources = resources = []

        mem = Dict()
        resources.append(mem)
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = EXECUTOR_MEMORY

        cpus = Dict()
        resources.append(cpus)
        cpus.name = 'cpus'
        cpus.type = 'SCALAR'
        cpus.scalar.value = EXECUTOR_CPUS

        execInfo.framework_id.value = str(self.framework_id)
        if self.options.image:
            execInfo.container.type = 'DOCKER'
            execInfo.container.docker.image = self.options.image

            execInfo.container.docker.parameters = parameters = []
            p = Dict()
            p.key = 'memory-swap'
            p.value = '-1'
            parameters.append(p)

            execInfo.container.volumes = volumes = []

            for path in ['/etc/passwd', '/etc/group']:
                v = Dict()
                volumes.append(v)
                v.host_path = v.container_path = path
                v.mode = 'RO'

            for path in conf.MOOSEFS_MOUNT_POINTS:
                v = Dict()
                volumes.append(v)
                v.host_path = v.container_path = path
                v.mode = 'RW'

            if self.options.volumes:
                for volume in self.options.volumes.split(','):
                    fields = volume.split(':')
                    if len(fields) == 3:
                        host_path, container_path, mode = fields
                        mode = mode.upper()
                        assert mode in ('RO', 'RW')
                    elif len(fields) == 2:
                        host_path, container_path = fields
                        mode = 'RW'
                    elif len(fields) == 1:
                        container_path, = fields
                        host_path = ''
                        mode = 'RW'
                    else:
                        raise Exception('cannot parse volume %s', volume)

                    try:
                        os.makedirs(host_path)
                    except OSError:
                        pass
                    v = Dict()
                    volumes.append(v)
                    v.container_path = container_path
                    v.mode = mode
                    if host_path:
                        v.host_path = host_path

        return execInfo

    def create_port(self, logger):
        sock = ctx.socket(zmq.PULL)
        host = socket.gethostname()
        port = sock.bind_to_random_port('tcp://0.0.0.0')

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
                if not six.PY2:
                    line = line.decode('utf-8')
                line = line.rstrip()
                logger.info(line)

        t = threading.Thread(target=redirect, name="redirect")
        t.daemon = True
        t.start()
        return t, 'tcp://%s:%d' % (host, port)

    def getResource(self, offer):
        cpus, mem, gpus = 0, 0, 0
        for r in offer.resources:
            if r.name == 'cpus':
                cpus = float(r.scalar.value)
            elif r.name == 'mem':
                mem = float(r.scalar.value)
            elif r.name == 'gpus':
                gpus = int(r.scalar.value)
        return cpus, mem, gpus

    def getAttributes(self, offer):
        attrs = {}
        for a in offer.attributes:
            attrs[a.name] = a.text.value
        return attrs

    def kill_task(self, driver, t):
        task_id = Dict()
        task_id.value = '%s-%s' % (t.id, t.tried)
        driver.killTask(task_id)

    def finish_task(self, tid):
        self.stats['finish_times'][tid] = time.time()
        del self.task_launched[tid]

    @safe
    def registered(self, driver, fid, master_info):
        logger.debug('Registered with Mesos, FID = %s' % fid.value)
        self.framework_id = fid.value
        f_handler, self.loghub_dir = add_loghub(self.framework_id)
        self.executor = self.getExecutorInfo()
        self.logger_stdout = create_logger(sys.stdout)
        self.logger_stderr = create_logger(sys.stderr, f_handler)

        self.stdout_t, self.stdout_port = self.create_port(self.logger_stdout)
        self.stderr_t, self.stderr_port = self.create_port(self.logger_stderr)

    @safe
    def reregistered(self, driver, master_info):
        logger.debug('Registered with Mesos')

    @safe
    def disconnected(self, driver):
        logger.debug("framework is disconnected")

    @safe
    def offerRescinded(self, driver, offer):
        logger.debug('resource rescinded: %s', offer)

    @safe
    def frameworkMessage(self, driver, executor_id, agent_id, data):
        logger.warning('[agent %s] %s', agent_id.value, data)

    @safe
    def executorLost(self, driver, executor_id, agent_id, status):
        logger.warning(
            'executor at %s %s lost: %s',
            agent_id.value,
            executor_id.value,
            status)
        self.slaveLost(driver, agent_id)

    @safe
    def slaveLost(self, driver, agent_id):
        logger.warning('agent %s lost', agent_id.value)
        sid = agent_id.value
        if sid in self.agentTasks:
            for tid in self.agentTasks[sid]:
                if tid in self.task_launched:
                    logger.warning('Task %d killed for agent lost', tid)
                    self.kill_task(driver, self.task_launched[tid])

    @safe
    def error(self, driver, message):
        logger.error('Error from Mesos: %s' % (message,))
        self.stop(EXIT_EXCEPTION)

    @safe
    def check(self, driver):
        now = time.time()
        for t in self.task_launched.values():
            if t.state == 'TASK_STARTING' and t.state_time + 30 < now:
                logger.warning('task %d lauched failed, assign again', t.id)
                self.kill_task(driver, t)

    def stop(self, ec):
        if self.stopped:
            return
        self.stopped = True
        self.ec = ec
        logger.debug('scheduler stopped')

    def cleanup(self):
        safejoin(self.stdout_t)
        safejoin(self.stderr_t)

    def run(self, driver):
        start = time.time()
        while not self.stopped:
            time.sleep(0.1)

            now = time.time()
            self.check(driver)
            if (not self.started and self.next_try > 0 and
                    now > sched.next_try):
                self.next_try = 0
                driver.reviveOffers()

            if not self.started and now > self.last_offer_time + \
                    60 + random.randint(0, 5):
                logger.warning('too long to get offer, reviving...')
                self.last_offer_time = now
                driver.reviveOffers()

            if now - start > options.timeout:
                logger.warning('job timeout in %d seconds, exit now',
                               options.timeout)
                self.stop(EXIT_TIMEOUT)
                break

    def processHeartBeat(self):
        # no need in dpark now, just for compatibility with pymesos
        pass

    def dump_stats(self):
        st = self.stats
        st['ec'] = self.ec
        st['stop_time'] = time.time()
        path = os.path.join(self.loghub_dir, "stats.json")
        with open(path, 'w') as f:
            json.dump(st, f, indent=4)


class SubmitScheduler(BaseScheduler):

    def __init__(self, options, command):
        name = '[drun] ' + ' '.join(sys.argv[1:])
        if len(name) > 256:
            name = name[:256] + '...'

        super(SubmitScheduler, self).__init__(name, options, command)
        self.total_tasks = list(reversed([
            Task(i)
            for i in range(options.start, options.tasks)
        ]))

    @property
    def short_name(self):
        return "drun"

    @safe
    def resourceOffers(self, driver, offers):
        tpn = self.options.task_per_node
        random.shuffle(offers)
        self.last_offer_time = time.time()
        if not self.total_tasks:
            driver.suppressOffers()
            for o in offers:
                driver.declineOffer(o.id)

            return

        for offer in offers:
            try:
                if conf.ban(offer.hostname):
                    logger.debug("skip offer on banned node: %s", offer.hostname)
                    continue
            except:
                logger.exception("bad ban() func in dpark.conf")

            unavailability = offer.get('unavailability')
            if (unavailability is not None and
                    sec2nanosec(time.time() + conf.DEFAULT_TASK_TIME) >= unavailability['start']['nanoseconds']):
                logger.debug('the host %s plan to maintain, so skip it', offer.hostname)
                driver.declineOffer(offer.id, filters=Dict(refuse_seconds=600))
                continue
            attrs = self.getAttributes(offer)
            group = attrs.get('group', 'None')
            if (self.options.group or group.startswith(
                    '_')) and group not in self.options.group:
                driver.declineOffer(offer.id, REFUSE_FILTER)
                continue

            cpus, mem, gpus = self.getResource(offer)
            logger.debug('got resource offer %s: cpus:%s, mem:%s, gpus:%s at %s',
                         offer.id.value, cpus, mem, gpus, offer.hostname)
            sid = offer.agent_id.value
            tasks = []
            while (self.total_tasks and cpus >= self.cpus + EXECUTOR_CPUS and
                   mem >= self.mem + EXECUTOR_MEMORY and gpus >= self.gpus and (
                           tpn == 0 or tpn > 0 and
                           len(self.agentTasks.get(sid, set())) < tpn
                   )):
                logger.debug('Accepting slot on agent %s (%s)',
                             offer.agent_id.value, offer.hostname)
                t = self.total_tasks.pop()
                task = self.create_task(offer, t)
                tasks.append(task)
                t.state = 'TASK_STARTING'
                self.stats['submit_times'][t.id] = t.state_time = time.time()
                self.task_launched[t.id] = t
                self.agentTasks.setdefault(sid, set()).add(t.id)
                cpus -= self.cpus
                mem -= self.mem
                gpus -= self.gpus
                if not self.total_tasks:
                    break

            logger.debug(
                'dispatch %d tasks to agent %s',
                len(tasks),
                offer.hostname)
            driver.launchTasks(offer.id, tasks, REFUSE_FILTER)

    @safe
    def inverseOffers(self, driver, offers):
        for o in offers:
            driver.acceptInverseOffers(o.id)

    def create_task(self, offer, t):
        task = Dict()
        task.task_id.value = '%d-%d' % (t.id, t.tried)
        task.agent_id.value = offer.agent_id.value
        task.name = 'task %s/%d' % (t.id, self.options.tasks)
        task.executor = self.executor
        env = dict(os.environ)
        env['DRUN_RANK'] = str(t.id)
        env['DRUN_SIZE'] = str(self.options.tasks)
        command = self.command[:]
        if self.options.expand:
            for i, x in enumerate(command):
                command[i] = x % {'RANK': t.id, 'SIZE': self.options.tasks}

        task.data = encode_data(pickle.dumps([
            os.getcwd(), command, env, self.options.shell,
            self.stdout_port, self.stderr_port, None
        ]))

        task.resources = resources = []

        cpu = Dict()
        resources.append(cpu)
        cpu.name = 'cpus'
        cpu.type = 'SCALAR'
        cpu.scalar.value = self.cpus

        mem = Dict()
        resources.append(mem)
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = self.mem

        if self.gpus > 0:
            gpu = Dict()
            resources.append(gpu)
            gpu.name = 'gpus'
            gpu.type = 'SCALAR'
            gpu.scalar.value = self.gpus

        return task

    @safe
    def statusUpdate(self, driver, update):
        logger.debug(
            'Task %s in state %s' %
            (update.task_id.value, update.state))
        tid = int(update.task_id.value.split('-')[0])
        if tid not in self.task_launched:
            for t in self.total_tasks:
                logger.error('Task %d not in task_launched', tid)
                return

        t = self.task_launched[tid]
        t.state = update.state
        t.state_time = time.time()

        if update.state == 'TASK_RUNNING':
            self.started = True
            return

        self.finish_task(tid)
        sid = next(
            (sid for sid in self.agentTasks if tid in self.agentTasks[sid]),
            None)
        if sid:
            self.agentTasks[sid].remove(tid)

        if update.state != 'TASK_FINISHED':
            message = getattr(update, 'message', '')
            if t.tried < self.options.retry:
                logger.warning('Task %d %s, retry %d: %s',
                               t.id, update.state, t.tried, message)
                t.tried += 1
                t.state = -1
                self.total_tasks.append(t)  # try again
            else:
                logger.error('Task %d %s on %s: %s, exit now',
                             t.id, update.state, sid, message)
                self.stop(EXIT_TASKFAIL)
                return

        if self.total_tasks:
            driver.reviveOffers()  # request more offers again

        if not self.task_launched and not self.total_tasks:
            self.stop(EXIT_NOMAL)


class MPIScheduler(BaseScheduler):

    def __init__(self, options, command):
        name = '[mrun] ' + ' '.join(sys.argv[1:])
        if len(name) > 256:
            name = name[:256] + '...'

        super(MPIScheduler, self).__init__(name, options, command, role=conf.MESOS_MPI_ROLE)
        self.used_tasks = {}
        self.id = 0
        self.p = None
        self.publisher = ctx.socket(zmq.PUB)
        port = self.publisher.bind_to_random_port('tcp://0.0.0.0')
        host = socket.gethostname()
        self.publisher_port = 'tcp://%s:%d' % (host, port)
        self.mpiout_t = None

    @property
    def short_name(self):
        return "mrun"

    def start_task(self, driver, offer, k):
        t = Task(self.id)
        sid = offer.agent_id.value
        self.id += 1
        self.task_launched[t.id] = t
        self.used_tasks[t.id] = (offer.hostname, k)
        self.stats['submit_times'][t.id] = time.time()
        self.stats['slots'][t.id] = k
        self.agentTasks.setdefault(sid, set()).add(t.id)
        task = self.create_task(offer, t, k)
        logger.debug('lauching task %d with %d slots, offer %s on %s',
                     t.id, k, offer.id.value, offer.hostname)
        driver.launchTasks(offer.id, [task], REFUSE_FILTER)

    @safe
    def resourceOffers(self, driver, offers):
        random.shuffle(offers)
        self.last_offer_time = time.time()
        launched = 0
        used_hosts = set()

        if self.started:
            driver.suppressOffers()
            for o in offers:
                driver.declineOffer(o.id)

            return

        for hostname, slots in six.itervalues(self.used_tasks):
            used_hosts.add(hostname)
            launched += slots

        for offer in offers:
            cpus, mem, gpus = self.getResource(offer)
            logger.debug('got resource offer %s: cpus:%s, mem:%s, gpus:%s at %s',
                         offer.id.value, cpus, mem, gpus, offer.hostname)
            unavailability = offer.get('unavailability')
            if (unavailability is not None and
                    sec2nanosec(time.time() + conf.DEFAULT_TASK_TIME) >= unavailability['start']['nanoseconds']):
                logger.debug('the host %s plan to maintain, so skip it', offer.hostname)
                driver.declineOffer(offer.id, filters=Dict(refuse_seconds=600))
                continue
            if launched >= self.options.tasks:
                driver.declineOffer(offer.id, REFUSE_FILTER)
                continue

            if offer.hostname in used_hosts:
                driver.declineOffer(offer.id)
                continue

            attrs = self.getAttributes(offer)
            group = attrs.get('group', 'None')
            if (self.options.group or group.startswith(
                    '_')) and group not in self.options.group:
                driver.declineOffer(offer.id, REFUSE_FILTER)
                continue

            slots = int(min((cpus - EXECUTOR_CPUS) / self.cpus,
                            (mem - EXECUTOR_MEMORY) / self.mem))
            if self.gpus > 0:
                slots = int(min(slots, gpus / self.gpus))

            if self.options.task_per_node:
                slots = min(slots, self.options.task_per_node)
            slots = min(slots, self.options.tasks - launched)
            if slots >= 1:
                launched += slots
                used_hosts.add(offer.hostname)
                self.start_task(driver, offer, slots)
            else:
                driver.declineOffer(offer.id, REFUSE_FILTER)

        if launched < self.options.tasks:
            logger.warning('not enough offers: need %d offer %d, '
                           'waiting more resources',
                           self.options.tasks, launched)

    @safe
    def inverseOffers(self, driver, offers):
        for o in offers:
            driver.acceptInverseOffers(o.id)

    @safe
    def statusUpdate(self, driver, update):
        logger.debug(
            'Task %s in state %s' %
            (update.task_id.value, update.state))
        tid = int(update.task_id.value.split('-')[0])
        if tid not in self.task_launched:
            logger.error('Task %d not in task_launched', tid)
            return

        t = self.task_launched[tid]
        t.state = update.state
        t.state_time = time.time()
        if update.state == 'TASK_RUNNING':
            launched = sum(
                slots for hostname, slots in self.used_tasks.values())
            ready = all(
                t.state == 'TASK_RUNNING' for t in self.task_launched.values())
            if launched == self.options.tasks and ready:
                logger.debug('all tasks are ready, start to run')
                self.start_mpi()

            return

        self.finish_task(tid)

        sid = next(
            (sid for sid in self.agentTasks if tid in self.agentTasks[sid]),
            None)
        if sid:
            self.agentTasks[sid].remove(tid)

        if update.state != 'TASK_FINISHED':
            if not self.started:
                logger.warning('Task %d %s, retry %d',
                               t.id, update.state, t.tried)
                driver.reviveOffers()  # request more offers again
                self.used_tasks.pop(tid)

            else:
                logger.error('Task %s failed, cancel all tasks, exit now', tid)
                self.stop(EXIT_TASKFAIL)

        else:
            if not self.started:
                logger.warning(
                    'Task %s has not started, ignore it %s',
                    tid,
                    update.state)
                return

            if not self.task_launched:
                self.stop(EXIT_NOMAL)

    def create_task(self, offer, t, k):
        task = Dict()
        task.task_id.value = '%s-%s' % (t.id, t.tried)
        task.agent_id.value = offer.agent_id.value
        task.name = 'task %s' % t.id
        task.executor = self.executor
        env = dict(os.environ)
        task.data = encode_data(pickle.dumps([
            os.getcwd(), None, env, self.options.shell,
            self.stdout_port, self.stderr_port, self.publisher_port
        ]))

        task.resources = resources = []
        cpu = Dict()
        resources.append(cpu)
        cpu.name = 'cpus'
        cpu.type = 'SCALAR'
        cpu.scalar.value = self.cpus * k

        mem = Dict()
        resources.append(mem)
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = self.mem * k

        if self.gpus > 0:
            gpu = Dict()
            resources.append(gpu)
            gpu.name = 'gpus'
            gpu.type = 'SCALAR'
            gpu.scalar.value = self.gpus * k

        return task

    def start_mpi(self):
        try:
            self.stats['start_time'] = time.time()
            commands = self.try_to_start_mpi(
                self.command, self.options.tasks, list(self.used_tasks.values()))
        except Exception:
            logger.exception('Failed to start mpi, retry')
            self.broadcast_command({})
            self.next_try = time.time() + random.randint(5, 10)
            return

        self.broadcast_command(commands)
        self.started = True

    def broadcast_command(self, command):
        def repeat_pub():
            for i in range(10):
                self.publisher.send(pickle.dumps(command))
                time.sleep(1)
                if self.stopped:
                    break

        t = threading.Thread(target=repeat_pub, name="broadcast_command")
        t.deamon = True
        t.start()
        return t

    def try_to_start_mpi(self, command, tasks, items):
        MPI_MPICH_OLD, MPI_MPICH, MPI_OPENMPI = range(3)

        def guess_mpi_impl():
            info = subprocess.check_output(['mpirun', '--version'])
            for line in info.splitlines():
                if b'Launchers available' in line:
                    if b' none ' in line:
                        return MPI_MPICH_OLD
                    elif b' manual ' in line:
                        return MPI_MPICH
                elif b'Open MPI' in line:
                    return MPI_OPENMPI

            else:
                raise RuntimeError('Unknown MPI implementation')

        if self.p:
            try:
                self.p.kill()
            except:
                pass

        mpi_impl = guess_mpi_impl()
        hosts = ','.join('%s:%d' % (hostname, slots)
                         for i, (hostname, slots) in enumerate(items))

        if mpi_impl == MPI_MPICH_OLD:
            # MPICH 1.x
            prefix = b'HYDRA_LAUNCH: '
            postfix = b'HYDRA_LAUNCH_END\n'
            cmd = ['mpirun', '-prepend-rank', '-launcher', 'none',
                   '-hosts', hosts, '-np', str(tasks)] + command
        elif mpi_impl == MPI_MPICH:
            # MPICH 3.x
            prefix = b'HYDRA_LAUNCH: '
            postfix = b'HYDRA_LAUNCH_END\n'
            cmd = ['mpirun', '-prepend-rank', '-launcher', 'manual',
                   '-rmk', 'user', '-hosts', hosts, '-np', str(tasks)] \
                  + command
        elif mpi_impl == MPI_OPENMPI:
            # OpenMPI
            hosts = ','.join('__DUMMY__%s:%d' % (i, slots)
                             for i, (hostname, slots) in enumerate(items))
            prefix = b'__DUMMY__'
            postfix = None
            cmd = ['mpirun', '-tag-output', '-mca', 'plm_rsh_agent', 'echo',
                   '-host', hosts, '-np', str(tasks)] + command

        else:
            assert False

        self.p = p = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE)
        agents = []
        while True:
            line = p.stdout.readline()
            if not line:
                break

            if line.startswith(prefix):
                agent = line[len(prefix):-1].split(b'; ')[-1].strip()
                agents.append(agent)

            if postfix is None and len(agents) == len(items):
                break

            if line == postfix:
                break

        if len(agents) != len(items):
            logger.error('hosts: %s, agents: %s', items, agents)
            raise Exception('agents not match with hosts')

        def output(f):
            while True:
                line = f.readline()
                if not line:
                    break
                if not six.PY2:
                    line = line.decode('utf-8')

                sys.stdout.write(line)

        self.mpiout_t = t = threading.Thread(target=output,
                                             args=[p.stdout],
                                             name="collect_mpirun_out")
        t.deamon = True
        t.start()
        return dict(list(zip((hostname for hostname, slots in items), agents)))

    def cleanup(self):
        if self.started:
            try:
                self.p.kill()
                self.p.wait()
            except:
                pass
            safejoin(self.mpiout_t)
        self.publisher.close()
        super(MPIScheduler, self).cleanup()


if __name__ == '__main__':
    parser = OptionParser(usage='Usage: drun [options] <command>')
    parser.allow_interspersed_args = False
    parser.add_option('-s', '--master', type='string',
                      default='mesos',
                      help='url of master (default: mesos)')
    parser.add_option('-i', '--mpi', action='store_true',
                      help='run MPI tasks')

    parser.add_option('-n', '--tasks', type='int', default=1,
                      help='number task to launch (default: 1)')
    parser.add_option('-b', '--start', type='int', default=0,
                      help='which task to start (default: 0)')
    parser.add_option('-p', '--task_per_node', type='int', default=0,
                      help='max number of tasks on one node (default: 0)')
    parser.add_option('-r', '--retry', type='int', default=0,
                      help='retry times when failed (default: 0)')
    parser.add_option('-t', '--timeout', type='int', default=3600 * 24,
                      help='timeout of job in seconds (default: 86400)')

    parser.add_option('-c', '--cpus', type='float', default=1.0,
                      help='number of CPUs per task (default: 1)')
    parser.add_option('-G', '--gpus', type='int', default=0,
                      help='number of GPUs per task (default: 0)')
    parser.add_option('-m', '--mem', type='string', default='100m',
                      help='MB of memory per task (default: 100m)')
    parser.add_option('-g', '--group', type='string', default='',
                      help='which group to run (default: ''')

    parser.add_option('-I', '--image', type='string',
                      help='image name for Docker')
    parser.add_option('-V', '--volumes', type='string',
                      help='volumes to mount into Docker')

    parser.add_option('--expand', action='store_true',
                      help='expand expression in command line')
    parser.add_option('--shell', action='store_true',
                      help='using shell re-intepret the cmd args')

    parser.add_option('-q', '--quiet', action='store_true',
                      help='be quiet', )
    parser.add_option('-v', '--verbose', action='store_true',
                      help='show more useful log', )

    (options, command) = parser.parse_args()

    conf.__dict__.update(os.environ)

    # by name
    if options.master == 'mesos':
        options.master = conf.MESOS_MASTER
    else:
        options.master = conf.MESOS_MASTERS.get(options.master, options.master)

    # parse
    if options.master.startswith('mesos://'):
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

    logging.basicConfig(
        format='[drun] %(threadName)s %(asctime)-15s %(message)s',
        level=options.quiet and logging.ERROR
              or options.verbose and logging.DEBUG
              or logging.INFO
    )

    if options.mpi:
        if options.retry > 0:
            logger.error('MPI application can not retry')
            options.retry = 0
        sched = MPIScheduler(options, command)
    else:
        sched = SubmitScheduler(options, command)

    logger.debug('Connecting to mesos master %s', options.master)
    driver = MesosSchedulerDriver(
        sched, sched.framework, options.master, use_addict=True
    )


    def handler(signm, frame):
        logger.warning('got signal %d, exit now', signm)
        sched.stop(EXIT_SIGNAL)


    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGHUP, handler)
    signal.signal(signal.SIGABRT, handler)
    signal.signal(signal.SIGQUIT, handler)

    spawn_rconsole(locals())

    try:
        driver.start()
        sched.run(driver)
    except KeyboardInterrupt:
        logger.warning('stopped by KeyboardInterrupt')
        sched.stop(EXIT_KEYBORAD)
    except Exception as e:
        import traceback

        logger.warning('catch unexpected Exception, exit now. %s',
                       traceback.format_exc())
        sched.stop(EXIT_EXCEPTION)
    finally:
        try:
            sched.dump_stats()
        except:
            logger.exception("dump stats fail, ignore it.")
        # sched.lock may be in WRONG status.
        # if any thread of sched may use lock or call driver, join it first
        driver.stop(False)
        driver.join()
        # mesos resources are released, and no racer for lock any more
        sched.cleanup()
        ctx.term()
        sys.exit(sched.ec)

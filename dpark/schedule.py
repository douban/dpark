import os
import sys
import socket
import marshal
import cPickle
import threading
import Queue
import time
import random
import urllib
import weakref
import multiprocessing
import zmq

import pymesos as mesos
from mesos.interface import mesos_pb2

from dpark.util import (
    compress, decompress, spawn, getuser, mkdir_p, get_logger
)
from dpark.dependency import ShuffleDependency
from dpark.accumulator import Accumulator
from dpark.task import ResultTask, ShuffleMapTask
from dpark.job import SimpleJob
from dpark.env import env
from dpark.mutable_dict import MutableDict
import dpark.conf as conf

logger = get_logger(__name__)

EXECUTOR_CPUS = 0.01
EXECUTOR_MEMORY = 64  # cache
POLL_TIMEOUT = 0.1
RESUBMIT_TIMEOUT = 60
MAX_IDLE_TIME = 60 * 30

class TaskEndReason:
    def __hash__(self):
        return hash(str(self))

    def __eq__(self, o):
        return str(self) == str(o)

class Success(TaskEndReason):
    pass

class FetchFailed(TaskEndReason):
    def __init__(self, serverUri, shuffleId, mapId, reduceId):
        self.serverUri = serverUri
        self.shuffleId = shuffleId
        self.mapId = mapId
        self.reduceId = reduceId

    def __str__(self):
        return '<FetchFailed(%s, %d, %d, %d)>' % (self.serverUri,
                self.shuffleId, self.mapId, self.reduceId)

class OtherFailure(TaskEndReason):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return '<OtherFailure %s>' % self.message

class Stage:
    def __init__(self, rdd, shuffleDep, parents):
        self.id = self.newId()
        self.rdd = rdd
        self.shuffleDep = shuffleDep
        self.parents = parents
        self.numPartitions = len(rdd)
        self.outputLocs = [[] for i in range(self.numPartitions)]

    def __str__(self):
        return '<Stage(%d) for %s>' % (self.id, self.rdd)

    def __getstate__(self):
        raise Exception("should not pickle stage")

    @property
    def isAvailable(self):
        if not self.parents and self.shuffleDep == None:
            return True
        return all(self.outputLocs)

    def addOutputLoc(self, partition, host):
        self.outputLocs[partition].append(host)

#    def removeOutput(self, partition, host):
#        prev = self.outputLocs[partition]
#        self.outputLocs[partition] = [h for h in prev if h != host]

    def removeHost(self, host):
        becameUnavailable = False
        for ls in self.outputLocs:
            if host in ls:
                ls.remove(host)
                becameUnavailable = True
        if becameUnavailable:
            logger.info("%s is now unavailable on host %s", self, host)

    nextId = 0
    @classmethod
    def newId(cls):
        cls.nextId += 1
        return cls.nextId


class Scheduler:
    def start(self): pass
    def runJob(self, rdd, func, partitions, allowLocal): pass
    def clear(self): pass
    def stop(self): pass
    def defaultParallelism(self):
        return 2

class CompletionEvent:
    def __init__(self, task, reason, result, accumUpdates):
        self.task = task
        self.reason = reason
        self.result = result
        self.accumUpdates = accumUpdates

def walk_dependencies(rdd, func):
    visited = set()
    to_visit = [rdd]
    while to_visit:
        r = to_visit.pop(0)
        if r.id in visited:
            continue
        visited.add(r.id)
        for dep in r.dependencies:
            if func(r, dep):
                to_visit.append(dep.rdd)

class DAGScheduler(Scheduler):

    def __init__(self):
        self.completionEvents = Queue.Queue()
        self.idToStage = weakref.WeakValueDictionary()
        self.shuffleToMapStage = {}
        self.cacheLocs = {}
        self._shutdown = False

    def check(self):
        pass

    def clear(self):
        self.idToStage.clear()
        self.shuffleToMapStage.clear()
        self.cacheLocs.clear()
        self.cacheTracker.clear()

    def shutdown(self):
        self._shutdown = True

    @property
    def cacheTracker(self):
        return env.cacheTracker

    @property
    def mapOutputTracker(self):
        return env.mapOutputTracker

    def submitTasks(self, tasks):
        raise NotImplementedError

    def taskEnded(self, task, reason, result, accumUpdates):
        self.completionEvents.put(CompletionEvent(task, reason, result, accumUpdates))

    def getCacheLocs(self, rdd):
        return self.cacheLocs.get(rdd.id, [[] for i in range(len(rdd))])

    def updateCacheLocs(self):
        self.cacheLocs = self.cacheTracker.getLocationsSnapshot()

    def newStage(self, rdd, shuffleDep):
        stage = Stage(rdd, shuffleDep, self.getParentStages(rdd))
        self.idToStage[stage.id] = stage
        logger.debug("new stage: %s", stage)
        return stage

    def getParentStages(self, rdd):
        parents = set()
        def _(r, dep):
            if r.shouldCache:
                self.cacheTracker.registerRDD(r.id, len(r))
            if isinstance(dep, ShuffleDependency):
                parents.add(self.getShuffleMapStage(dep))
                return False

            return True

        walk_dependencies(rdd, _)
        return list(parents)

    def getShuffleMapStage(self, dep):
        stage = self.shuffleToMapStage.get(dep.shuffleId, None)
        if stage is None:
            stage = self.newStage(dep.rdd, dep)
            self.shuffleToMapStage[dep.shuffleId] = stage
        return stage

    def getMissingParentStages(self, stage):
        missing = set()
        def _(r, dep):
            if r.shouldCache and all(self.getCacheLocs(r)):
                return False

            if isinstance(dep, ShuffleDependency):
                stage = self.getShuffleMapStage(dep)
                if not stage.isAvailable:
                    missing.add(stage)
                return False

            return True

        walk_dependencies(stage.rdd, _)
        return list(missing)

    def runJob(self, finalRdd, func, partitions, allowLocal):
        outputParts = list(partitions)
        numOutputParts = len(partitions)
        finalStage = self.newStage(finalRdd, None)
        results = [None]*numOutputParts
        finished = [None]*numOutputParts
        lastFinished = 0
        numFinished = 0

        waiting = set()
        running = set()
        failed = set()
        pendingTasks = {}
        lastFetchFailureTime = 0

        self.updateCacheLocs()

        logger.debug("Final stage: %s, %d", finalStage, numOutputParts)
        logger.debug("Parents of final stage: %s", finalStage.parents)
        logger.debug("Missing parents: %s", self.getMissingParentStages(finalStage))
        def onStageFinished(stage):
            def _(r, dep):
                return r._do_checkpoint()

            MutableDict.merge()
            walk_dependencies(stage.rdd, _)

        if allowLocal and (not finalStage.parents or not self.getMissingParentStages(finalStage)) and numOutputParts == 1:
            split = finalRdd.splits[outputParts[0]]
            yield func(finalRdd.iterator(split))
            onStageFinished(finalStage)
            return

        def submitStage(stage):
            logger.debug("submit stage %s", stage)
            if stage not in waiting and stage not in running:
                missing = self.getMissingParentStages(stage)
                if not missing:
                    submitMissingTasks(stage)
                    running.add(stage)
                else:
                    for parent in missing:
                        submitStage(parent)
                    waiting.add(stage)

        def submitMissingTasks(stage):
            myPending = pendingTasks.setdefault(stage, set())
            tasks = []
            have_prefer = True
            if stage == finalStage:
                for i in range(numOutputParts):
                    if not finished[i]:
                        part = outputParts[i]
                        if have_prefer:
                            locs = self.getPreferredLocs(finalRdd, part)
                            if not locs:
                                have_prefer = False
                        else:
                            locs = []
                        tasks.append(ResultTask(finalStage.id, finalRdd,
                            func, part, locs, i))
            else:
                for p in range(stage.numPartitions):
                    if not stage.outputLocs[p]:
                        if have_prefer:
                            locs = self.getPreferredLocs(stage.rdd, p)
                            if not locs:
                                have_prefer = False
                        else:
                            locs = []
                        tasks.append(ShuffleMapTask(stage.id, stage.rdd,
                            stage.shuffleDep, p, locs))
            logger.debug("add to pending %s tasks", len(tasks))
            myPending |= set(t.id for t in tasks)
            self.submitTasks(tasks)

        submitStage(finalStage)

        while numFinished != numOutputParts:
            try:
                evt = self.completionEvents.get(False)
            except Queue.Empty:
                self.check()
                if self._shutdown:
                    sys.exit(1)

                if failed and time.time() > lastFetchFailureTime + RESUBMIT_TIMEOUT:
                    self.updateCacheLocs()
                    for stage in failed:
                        logger.info("Resubmitting failed stages: %s", stage)
                        submitStage(stage)
                    failed.clear()
                else:
                    time.sleep(0.1)
                continue

            task, reason = evt.task, evt.reason
            stage = self.idToStage[task.stageId]
            if stage not in pendingTasks: # stage from other job
                continue
            logger.debug("remove from pending %s from %s", task, stage)
            pendingTasks[stage].remove(task.id)
            if isinstance(reason, Success):
                Accumulator.merge(evt.accumUpdates)
                if isinstance(task, ResultTask):
                    finished[task.outputId] = True
                    numFinished += 1
                    results[task.outputId] = evt.result
                    while lastFinished < numOutputParts and finished[lastFinished]:
                        yield results[lastFinished]
                        results[lastFinished] = None
                        lastFinished += 1

                elif isinstance(task, ShuffleMapTask):
                    stage = self.idToStage[task.stageId]
                    stage.addOutputLoc(task.partition, evt.result)
                    if not pendingTasks[stage] and all(stage.outputLocs):
                        logger.debug("%s finished; looking for newly runnable stages", stage)
                        onStageFinished(stage)
                        running.remove(stage)
                        if stage.shuffleDep != None:
                            self.mapOutputTracker.registerMapOutputs(
                                    stage.shuffleDep.shuffleId,
                                    [l[-1] for l in stage.outputLocs])
                        self.updateCacheLocs()
                        newlyRunnable = set(stage for stage in waiting if not self.getMissingParentStages(stage))
                        waiting -= newlyRunnable
                        running |= newlyRunnable
                        logger.debug("newly runnable: %s, %s", waiting, newlyRunnable)
                        for stage in newlyRunnable:
                            submitMissingTasks(stage)
            elif isinstance(reason, FetchFailed):
                if stage in running:
                    waiting.add(stage)
                mapStage = self.shuffleToMapStage[reason.shuffleId]
                mapStage.removeHost(reason.serverUri)
                failed.add(mapStage)
                lastFetchFailureTime = time.time()
            else:
                logger.error("task %s failed: %s %s %s", task, reason, type(reason), reason.message)
                raise Exception(reason.message)

        onStageFinished(finalStage)
        assert not any(results)
        return

    def getPreferredLocs(self, rdd, partition):
        return rdd.preferredLocations(rdd.splits[partition])


def run_task(task, aid):
    logger.debug("Running task %r", task)
    try:
        Accumulator.clear()
        result = task.run(aid)
        accumUpdates = Accumulator.values()
        MutableDict.flush()
        return (task.id, Success(), result, accumUpdates)
    except Exception, e:
        logger.error("error in task %s", task)
        import traceback
        traceback.print_exc()
        return (task.id, OtherFailure("exception:" + str(e)), None, None)


class LocalScheduler(DAGScheduler):
    attemptId = 0
    def nextAttempId(self):
        self.attemptId += 1
        return self.attemptId

    def submitTasks(self, tasks):
        logger.debug("submit tasks %s in LocalScheduler", tasks)
        for task in tasks:
#            task = cPickle.loads(cPickle.dumps(task, -1))
            _, reason, result, update = run_task(task, self.nextAttempId())
            self.taskEnded(task, reason, result, update)

def run_task_in_process(task, tid, environ):
    from dpark.env import env
    workdir = environ.get('WORKDIR')
    environ['SERVER_URI'] = 'file://%s' % workdir[0]
    env.start(False, environ)

    logger.debug("run task in process %s %s", task, tid)
    try:
        return run_task(task, tid)
    except KeyboardInterrupt:
        sys.exit(0)

class MultiProcessScheduler(LocalScheduler):
    def __init__(self, threads):
        LocalScheduler.__init__(self)
        self.threads = threads
        self.tasks = {}
        self.pool = multiprocessing.Pool(self.threads or 2)

    def submitTasks(self, tasks):
        if not tasks:
            return

        logger.info("Got a job with %d tasks: %s", len(tasks), tasks[0].rdd)

        total, self.finished, start = len(tasks), 0, time.time()
        def callback(args):
            logger.debug("got answer: %s", args)
            tid, reason, result, update = args
            task = self.tasks.pop(tid)
            self.finished += 1
            logger.info("Task %s finished (%d/%d)        \x1b[1A",
                tid, self.finished, total)
            if self.finished == total:
                logger.info("Job finished in %.1f seconds" + " "*20,  time.time() - start)
            self.taskEnded(task, reason, result, update)

        for task in tasks:
            logger.debug("put task async: %s", task)
            self.tasks[task.id] = task
            self.pool.apply_async(run_task_in_process,
                [task, self.nextAttempId(), env.environ],
                callback=callback)

    def stop(self):
        self.pool.terminate()
        self.pool.join()
        logger.debug("process pool stopped")


def profile(f):
    def func(*args, **kwargs):
        path = '/tmp/worker-%s.prof' % os.getpid()
        import cProfile
        import pstats
        func = f
        cProfile.runctx('func(*args, **kwargs)',
            globals(), locals(), path)
        stats = pstats.Stats(path)
        stats.strip_dirs()
        stats.sort_stats('time', 'calls')
        stats.print_stats(20)
        stats.sort_stats('cumulative')
        stats.print_stats(20)
    return func

def safe(f):
    def _(self, *a, **kw):
        with self.lock:
            r = f(self, *a, **kw)
        return r
    return _

def int2ip(n):
    return "%d.%d.%d.%d" % (n & 0xff, (n>>8)&0xff, (n>>16)&0xff, n>>24)

class MesosScheduler(DAGScheduler):

    def __init__(self, master, options):
        DAGScheduler.__init__(self)
        self.master = master
        self.use_self_as_exec = options.self
        self.cpus = options.cpus
        self.mem = options.mem
        self.task_per_node = options.parallel or multiprocessing.cpu_count()
        self.group = options.group
        self.logLevel = options.logLevel
        self.options = options
        self.started = False
        self.last_finish_time = 0
        self.isRegistered = False
        self.executor = None
        self.driver = None
        self.out_logger = None
        self.err_logger = None
        self.lock = threading.RLock()
        self.init_job()

    def init_job(self):
        self.activeJobs = {}
        self.activeJobsQueue = []
        self.taskIdToJobId = {}
        self.taskIdToSlaveId = {}
        self.jobTasks = {}
        self.slaveTasks = {}

    def clear(self):
        DAGScheduler.clear(self)
        self.init_job()

    def start(self):
        if not self.out_logger:
            self.out_logger = self.start_logger(sys.stdout)
        if not self.err_logger:
            self.err_logger = self.start_logger(sys.stderr)

    def start_driver(self):
        name = '[dpark] ' + os.path.abspath(sys.argv[0]) + ' ' + ' '.join(sys.argv[1:])
        if len(name) > 256:
            name = name[:256] + '...'
        framework = mesos_pb2.FrameworkInfo()
        framework.user = getuser()
        if framework.user == 'root':
            raise Exception("dpark is not allowed to run as 'root'")
        framework.name = name
        framework.hostname = socket.gethostname()

        self.driver = mesos.MesosSchedulerDriver(self, framework,
                                                 self.master)
        self.driver.start()
        logger.debug("Mesos Scheudler driver started")

        self.started = True
        self.last_finish_time = time.time()
        def check():
            while self.started:
                now = time.time()
                if not self.activeJobs and now - self.last_finish_time > MAX_IDLE_TIME:
                    logger.info("stop mesos scheduler after %d seconds idle",
                            now - self.last_finish_time)
                    self.stop()
                    break
                time.sleep(1)

        spawn(check)

    def start_logger(self, output):
        sock = env.ctx.socket(zmq.PULL)
        port = sock.bind_to_random_port("tcp://0.0.0.0")

        def collect_log():
            while not self._shutdown:
                if sock.poll(1000, zmq.POLLIN):
                    line = sock.recv()
                    output.write(line)

        spawn(collect_log)

        host = socket.gethostname()
        addr = "tcp://%s:%d" % (host, port)
        logger.debug("log collecter start at %s", addr)
        return addr

    @safe
    def registered(self, driver, frameworkId, masterInfo):
        self.isRegistered = True
        logger.debug("connect to master %s:%s(%s), registered as %s",
            int2ip(masterInfo.ip), masterInfo.port, masterInfo.id,
            frameworkId.value)
        self.executor = self.getExecutorInfo(str(frameworkId.value))

    @safe
    def reregistered(self, driver, masterInfo):
        logger.warning("re-connect to mesos master %s:%s(%s)",
            int2ip(masterInfo.ip), masterInfo.port, masterInfo.id)

    @safe
    def disconnected(self, driver):
        logger.debug("framework is disconnected")

    @safe
    def getExecutorInfo(self, framework_id):
        info = mesos_pb2.ExecutorInfo()
        if hasattr(info, 'framework_id'):
            info.framework_id.value = framework_id

        if self.use_self_as_exec:
            info.command.value = os.path.abspath(sys.argv[0])
            info.executor_id.value = sys.argv[0]
        else:
            info.command.value = '%s %s' % (
                sys.executable,
                os.path.abspath(os.path.join(os.path.dirname(__file__), 'executor.py'))
            )
            info.executor_id.value = "default"

        v = info.command.environment.variables.add()
        v.name = 'UID'
        v.value = str(os.getuid())
        v = info.command.environment.variables.add()
        v.name = 'GID'
        v.value = str(os.getgid())


        if self.options.image and hasattr(info, 'container'):
            info.container.type = mesos_pb2.ContainerInfo.DOCKER
            info.container.docker.image = self.options.image

            for path in ['/etc/passwd', '/etc/group']:
                v = info.container.volumes.add()
                v.host_path = v.container_path = path
                v.mode = mesos_pb2.Volume.RO

            for path in conf.MOOSEFS_MOUNT_POINTS:
                v = info.container.volumes.add()
                v.host_path = v.container_path = path
                v.mode = mesos_pb2.Volume.RW

            for path in conf.DPARK_WORK_DIR.split(','):
                v = info.container.volumes.add()
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

                    mkdir_p(host_path)

                v = info.container.volumes.add()
                v.container_path = container_path
                v.mode = mode
                if host_path:
                    v.host_path = host_path

        mem = info.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = EXECUTOR_MEMORY
        cpus = info.resources.add()
        cpus.name = 'cpus'
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = EXECUTOR_CPUS

        Script = os.path.realpath(sys.argv[0])
        if hasattr(info, 'name'):
            info.name = Script

        info.data = marshal.dumps((Script, os.getcwd(), sys.path, dict(os.environ),
            self.task_per_node, self.out_logger, self.err_logger, self.logLevel, env.environ))
        return info

    @safe
    def submitTasks(self, tasks):
        if not tasks:
            return

        job = SimpleJob(self, tasks, self.cpus, tasks[0].rdd.mem or self.mem)
        self.activeJobs[job.id] = job
        self.activeJobsQueue.append(job)
        self.jobTasks[job.id] = set()
        logger.info("Got job %d with %d tasks: %s", job.id, len(tasks), tasks[0].rdd)

        need_revive = self.started
        if not self.started:
            self.start_driver()
        while not self.isRegistered:
            self.lock.release()
            time.sleep(0.01)
            self.lock.acquire()

        if need_revive:
            self.requestMoreResources()

    def requestMoreResources(self):
        logger.debug("reviveOffers")
        self.driver.reviveOffers()

    @safe
    def resourceOffers(self, driver, offers):
        rf = mesos_pb2.Filters()
        if not self.activeJobs:
            rf.refuse_seconds = 60 * 5
            for o in offers:
                driver.launchTasks(o.id, [], rf)
            return

        start = time.time()
        random.shuffle(offers)
        cpus = [self.getResource(o.resources, 'cpus') for o in offers]
        mems = [self.getResource(o.resources, 'mem')
                - (o.slave_id.value not in self.slaveTasks
                    and EXECUTOR_MEMORY or 0)
                for o in offers]
        logger.debug("get %d offers (%s cpus, %s mem), %d jobs",
            len(offers), sum(cpus), sum(mems), len(self.activeJobs))

        tasks = {}
        for job in self.activeJobsQueue:
            while True:
                launchedTask = False
                for i,o in enumerate(offers):
                    sid = o.slave_id.value
                    if self.group and (self.getAttribute(o.attributes, 'group') or 'none') not in self.group:
                        continue
                    if self.slaveTasks.get(sid, 0) >= self.task_per_node:
                        continue
                    if (mems[i] < self.mem + EXECUTOR_MEMORY
                            or cpus[i] < self.cpus + EXECUTOR_CPUS):
                        continue
                    t = job.slaveOffer(str(o.hostname), cpus[i], mems[i])
                    if not t:
                        continue
                    task = self.createTask(o, job, t)
                    tasks.setdefault(o.id.value, []).append(task)

                    logger.debug("dispatch %s into %s", t, o.hostname)
                    tid = task.task_id.value
                    self.jobTasks[job.id].add(tid)
                    self.taskIdToJobId[tid] = job.id
                    self.taskIdToSlaveId[tid] = sid
                    self.slaveTasks[sid] = self.slaveTasks.get(sid, 0)  + 1
                    cpus[i] -= min(cpus[i], t.cpus)
                    mems[i] -= t.mem
                    launchedTask = True

                if not launchedTask:
                    break

        used = time.time() - start
        if used > 10:
            logger.error("use too much time in slaveOffer: %.2fs", used)

        rf.refuse_seconds = 5
        for o in offers:
            driver.launchTasks(o.id, tasks.get(o.id.value, []), rf)

        logger.debug("reply with %d tasks, %s cpus %s mem left",
            sum(len(ts) for ts in tasks.values()), sum(cpus), sum(mems))

    @safe
    def offerRescinded(self, driver, offer_id):
        logger.debug("rescinded offer: %s", offer_id)
        if self.activeJobs:
            self.requestMoreResources()

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def getAttribute(self, attrs, name):
        for r in attrs:
            if r.name == name:
                return r.text.value

    def createTask(self, o, job, t):
        task = mesos_pb2.TaskInfo()
        tid = "%s:%s:%s" % (job.id, t.id, t.tried)
        task.name = "task %s" % tid
        task.task_id.value = tid
        task.slave_id.value = o.slave_id.value
        task.data = compress(cPickle.dumps((t, t.tried), -1))
        task.executor.MergeFrom(self.executor)
        if len(task.data) > 1000*1024:
            logger.warning("task too large: %s %d",
                t, len(task.data))

        cpu = task.resources.add()
        cpu.name = 'cpus'
        cpu.type = mesos_pb2.Value.SCALAR
        cpu.scalar.value = t.cpus
        mem = task.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = t.mem
        return task

    @safe
    def statusUpdate(self, driver, status):
        tid = status.task_id.value
        state = status.state
        logger.debug("status update: %s %s", tid, state)

        jid = self.taskIdToJobId.get(tid)
        _, task_id, tried = map(int, tid.split(':'))
        if state == mesos_pb2.TASK_RUNNING:
            if jid in self.activeJobs:
                job = self.activeJobs[jid]
                job.statusUpdate(task_id, tried, state)
            else:
                logger.debug('kill task %s as its job has gone', tid)
                self.driver.killTask(mesos_pb2.TaskID(value=tid))

            return

        self.taskIdToJobId.pop(tid, None)
        if jid in self.jobTasks:
            self.jobTasks[jid].remove(tid)
        if tid in self.taskIdToSlaveId:
            slave_id = self.taskIdToSlaveId[tid]
            if slave_id in self.slaveTasks:
                self.slaveTasks[slave_id] -= 1
            del self.taskIdToSlaveId[tid]

        if jid not in self.activeJobs:
            logger.debug('ignore task %s as its job has gone', tid)
            return

        job = self.activeJobs[jid]
        if state in (mesos_pb2.TASK_FINISHED, mesos_pb2.TASK_FAILED) and status.data:
            try:
                reason,result,accUpdate = cPickle.loads(status.data)
                if result:
                    flag, data = result
                    if flag >= 2:
                        try:
                            data = urllib.urlopen(data).read()
                        except IOError:
                            # try again
                            data = urllib.urlopen(data).read()
                        flag -= 2
                    data = decompress(data)
                    if flag == 0:
                        result = marshal.loads(data)
                    else:
                        result = cPickle.loads(data)
            except Exception, e:
                logger.warning("error when cPickle.loads(): %s, data:%s", e, len(status.data))
                state = mesos_pb2.TASK_FAILED
                return job.statusUpdate(task_id, tried, mesos_pb2.TASK_FAILED, 'load failed: %s' % e)
            else:
                return job.statusUpdate(task_id, tried, state,
                    reason, result, accUpdate)

        # killed, lost, load failed
        job.statusUpdate(task_id, tried, state, status.data)

    def jobFinished(self, job):
        logger.debug("job %s finished", job.id)
        if job.id in self.activeJobs:
            del self.activeJobs[job.id]
            self.activeJobsQueue.remove(job)
            for tid in self.jobTasks[job.id]:
                self.driver.killTask(mesos_pb2.TaskID(value=tid))
            del self.jobTasks[job.id]
            self.last_finish_time = time.time()

            if not self.activeJobs:
                self.slaveTasks.clear()

        for tid, jid in self.taskIdToJobId.iteritems():
            if jid not in self.activeJobs:
                logger.debug('kill task %s, because it is orphan', tid)
                self.driver.killTask(mesos_pb2.TaskID(value=tid))

    @safe
    def check(self):
        for job in self.activeJobs.values():
            if job.check_task_timeout():
                self.requestMoreResources()

    @safe
    def error(self, driver, message):
        logger.warning("Mesos error message: %s", message)

    #@safe
    def stop(self):
        if not self.started:
            return
        logger.debug("stop scheduler")
        self.started = False
        self.isRegistered = False
        self.driver.stop(False)
        self.driver = None

    def defaultParallelism(self):
        return 16

    def frameworkMessage(self, driver, executor, slave, data):
        logger.warning("[slave %s] %s", slave.value, data)

    def executorLost(self, driver, executorId, slaveId, status):
        logger.warning("executor at %s %s lost: %s", slaveId.value, executorId.value, status)
        self.slaveTasks.pop(slaveId.value, None)

    def slaveLost(self, driver, slaveId):
        logger.warning("slave %s lost", slaveId.value)
        self.slaveTasks.pop(slaveId.value, None)

    def killTask(self, job_id, task_id, tried):
        tid = mesos_pb2.TaskID()
        tid.value = "%s:%s:%s" % (job_id, task_id, tried)
        self.driver.killTask(tid)

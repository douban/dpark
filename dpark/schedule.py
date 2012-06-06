import os, sys
import socket
import logging
import marshal
import cPickle
import threading, Queue
import time
import random

import zmq
import mesos
import mesos_pb2

from dependency import NarrowDependency, ShuffleDependency 
from accumulator import Accumulator
from task import ResultTask, ShuffleMapTask
from job import SimpleJob
from env import env
from broadcast import Broadcast

logger = logging.getLogger("scheduler")

MAX_FAILED = 3
EXECUTOR_MEMORY = 2096 + 1024 # cache
POLL_TIMEOUT = 0.1
RESUBMIT_TIMEOUT = 60

class TaskEndReason: pass
class Success(TaskEndReason): pass
class FetchFailed:
    def __init__(self, serverUri, shuffleId, mapId, reduceId):
        self.serverUri = serverUri
        self.shuffleId = shuffleId
        self.mapId = mapId
        self.reduceId = reduceId
class OtherFailure:
    def __init__(self, message):
        self.message = message

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
    
    nextId = 0
    @classmethod
    def newId(cls):
        cls.nextId += 1
        return cls.nextId


class Scheduler:
    def start(self):pass
    def runJob(self, rdd, func, partitions, allowLocal): pass
    def stop(self): pass
    def defaultParallelism(self):
        return 2

class CompletionEvent:
    def __init__(self, task, reason, result, accumUpdates):
        self.task = task
        self.reason = reason
        self.result = result
        self.accumUpdates = accumUpdates


class DAGScheduler(Scheduler):
    
    def __init__(self):
        self.completionEvents = Queue.Queue()
        self.idToStage = {}
        self.shuffleToMapStage = {}
        self.cacheLocs = {}
        self._shutdown = False

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
        visited = set()
        def visit(r):
            if r.id in visited:
                return
            visited.add(r.id)
            if r.shouldCache:
                self.cacheTracker.registerRDD(r.id, len(r))
            for dep in r.dependencies:
                if isinstance(dep, ShuffleDependency):
                    parents.add(self.getShuffleMapStage(dep))
                else:
                    visit(dep.rdd)
        visit(rdd)
        return list(parents)

    def getShuffleMapStage(self, dep):
        stage = self.shuffleToMapStage.get(dep.shuffleId, None)
        if stage is None:
            stage = self.newStage(dep.rdd, dep)
            self.shuffleToMapStage[dep.shuffleId] = stage
        return stage

    def getMissingParentStages(self, stage):
        missing = set()
        visited = set()
        def visit(r):
            if r.id in visited:
                return
            visited.add(r.id)
            if r.shouldCache and all(self.getCacheLocs(r)):
                return

            for dep in r.dependencies:
                if isinstance(dep, ShuffleDependency):
                    stage = self.getShuffleMapStage(dep)
                    if not stage.isAvailable:
                        missing.add(stage)
                elif isinstance(dep, NarrowDependency):
                    visit(dep.rdd)

        visit(stage.rdd)
        return list(missing)

    def runJob(self, finalRdd, func, partitions, allowLocal):
        outputParts = list(partitions)
        numOutputParts = len(partitions)
        finalStage = self.newStage(finalRdd, None)
        results = [None]*numOutputParts
        finished = [None]*numOutputParts
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
       
        if allowLocal and (not finalStage.parents or not self.getMissingParentStages(finalStage)) and numOutputParts == 1:
            split = finalRdd.splits[outputParts[0]]
            return [func(finalRdd.iterator(split))]

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
                            locs = self.getPreferredLocs(finalRdd, p)
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
                if self._shutdown:
                    sys.exit(1)

                if failed and time.time() > lastFetchFailureTime + RESUBMIT_TIMEOUT:
                    logger.debug("Resubmitting failed stages")
                    self.updateCacheLocs()
                    for stage in failed:
                        submitStage(stage)
                    failed.clear()
                else:
                    time.sleep(0.01)
                continue
               
            task = evt.task
            stage = self.idToStage[task.stageId]
            logger.debug("remove from pedding %s from %s", task, stage)
            pendingTasks[stage].remove(task.id)
            if isinstance(evt.reason, Success):
                Accumulator.merge(evt.accumUpdates)
                if isinstance(task, ResultTask):
                    results[task.outputId] = evt.result
                    finished[task.outputId] = True
                    numFinished += 1
                elif isinstance(task, ShuffleMapTask):
                    stage = self.idToStage[task.stageId]
                    stage.addOutputLoc(task.partition, evt.result)
                    if not pendingTasks[stage]:
                        logger.debug("%s finished; looking for newly runnable stages", stage)
                        running.remove(stage)
                        if stage.shuffleDep != None:
                            self.mapOutputTracker.registerMapOutputs(
                                    stage.shuffleDep.shuffleId,
                                    [l[0] for l in stage.outputLocs])
                        self.updateCacheLocs()
                        newlyRunnable = set(stage for stage in waiting if not self.getMissingParentStages(stage))
                        waiting -= newlyRunnable
                        running |= newlyRunnable
                        logger.debug("newly runnable: %s, %s", waiting, newlyRunnable)
                        for stage in newlyRunnable:
                            submitMissingTasks(stage)
            else:
                logger.error("task %s failed", task)
                if isinstance(evt.reason, FetchFailed):
                    pass
                else:
                    logger.error("%s %s %s", evt.reason, type(evt.reason), evt.reason.message)
                    raise evt.reason

        return results

    def getPreferredLocs(self, rdd, partition):
        if rdd.shouldCache:
            cached = self.getCacheLocs(rdd)[partition]
            if cached:
                return cached
        return rdd.preferredLocations(rdd.splits[partition])

def run_task(task, aid):
    logger.debug("Running task %r", task)
    try:
        Accumulator.clear()
        result = task.run(aid)
        accumUpdates = Accumulator.values()
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
    def stop(self):
        pass

def run_task_in_process(task, tid, environ):
    from env import env
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
        from multiprocessing import Pool
        self.pool = Pool(self.threads or 2)

    def start(self):
        pass

    def submitTasks(self, tasks):
        def callback(args):
            logger.debug("got answer: %s", args)
            tid, reason, result, update = args
            task = self.tasks.pop(tid)
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
#        with self.lock:
        return f(self, *a, **kw)
        return r
    return _

class MesosScheduler(mesos.Scheduler, DAGScheduler):

    def __init__(self, master, options):
        DAGScheduler.__init__(self)
        self.master = master
        self.use_self_as_exec = options.self
        self.cpus = options.cpus
        self.mem = options.mem
        self.task_per_node = options.parallel or 8
        self.group = options.group
        self.logLevel = options.logLevel
        self.options = options
        self.isRegistered = False
        self.activeJobs = {}
        self.activeJobsQueue = []
        self.taskIdToJobId = {}
        self.taskIdToSlaveId = {}
        self.jobTasks = {}
        self.driver = None
        self.slaveTasks = {}
        self.slaveFailed = {}
        self.lock = threading.RLock()

    def start(self):
        self.out_logger = self.start_logger(sys.stdout) 
        self.err_logger = self.start_logger(sys.stderr)

        name = '[dpark@%s] ' % socket.gethostname()
        name += os.path.abspath(sys.argv[0]) + ' ' + ' '.join(sys.argv[1:])
        self.driver = mesos.MesosSchedulerDriver(self, name,
            self.getExecutorInfo(), self.master)
        self.driver.start()
        logger.debug("Mesos Scheudler driver started")

    def start_logger(self, output):
        ctx = zmq.Context()
        sock = ctx.socket(zmq.PULL)
        port = sock.bind_to_random_port("tcp://0.0.0.0")

        def collect_log():
            while True:
                line = sock.recv()
                output.write(line)

        t = threading.Thread(target=collect_log)
        t.daemon = True
        t.start()

        host = socket.gethostname()
        addr = "tcp://%s:%d" % (host, port)
        logger.debug("log collecter start at %s", addr)
        return addr

    @safe
    def registered(self, driver, fid):
        self.isRegistered = True
        logger.debug("registered as %s", fid)

    @safe
    def getExecutorInfo(self):
        info = mesos_pb2.ExecutorInfo()

        if self.use_self_as_exec:
            info.uri = os.path.abspath(sys.argv[0])
            info.executor_id.value = sys.argv[0]
        else:
            dir = os.path.dirname(__file__)
            info.uri = os.path.abspath(os.path.join(dir, 'executor.py'))
            info.executor_id.value = "default"
        
        mem = info.resources.add()
        mem.name = 'mem'
        mem.type = 0 #mesos_pb2.Value.SCALAR
        mem.scalar.value = EXECUTOR_MEMORY
        info.data = marshal.dumps((os.path.realpath(sys.argv[0]), os.getcwd(), sys.path, self.task_per_node,
            self.out_logger, self.err_logger, self.logLevel, env.environ))
        return info

    @safe
    def submitTasks(self, tasks):
        logger.info("Got a job with %d tasks", len(tasks))
        job = SimpleJob(self, tasks)
        self.activeJobs[job.id] = job
        self.activeJobsQueue.append(job)
        logger.debug("Adding job with ID %d", job.id)
        self.jobTasks[job.id] = set()
        
        while not self.isRegistered:
#            self.lock.release()
            time.sleep(0.01)
#            self.lock.acquire()

        self.requestMoreResources()

    def requestMoreResources(self): 
        logger.debug("reviveOffers")
        self.driver.reviveOffers()

    def jobFinished(self, job):
        logger.debug("job %s finished", job.id)
        if job.id in self.activeJobs:
            del self.activeJobs[job.id]
            self.activeJobsQueue.remove(job) 
            for id in self.jobTasks[job.id]:
                del self.taskIdToJobId[id]
                del self.taskIdToSlaveId[id]
            del self.jobTasks[job.id]

    @safe
    def resourceOffers(self, driver, offers):
        if not self.activeJobs:
            rf = mesos_pb2.Filters()
            rf.refuse_seconds = -1
            for o in offers:
                driver.launchTasks(o.id, [], rf)
            return

        random.shuffle(offers)
        cpus = [self.getResource(o.resources, 'cpus') for o in offers]
        mems = [self.getResource(o.resources, 'mem') 
                - (o.slave_id.value not in self.slaveTasks
                    and EXECUTOR_MEMORY or 0)
                for o in offers]
        logger.debug("get %d offers (%s cpus, %s mem), %d jobs", 
            len(offers), sum(cpus), sum(mems), len(self.activeJobs))

        tasks = {}
        launchedTask = False
        for job in self.activeJobsQueue:
            while True:
                launchedTask = False
                for i,o in enumerate(offers):
                    sid = o.slave_id.value
                    if self.group and (self.getAttribute(o.attributes, 'group') or 'none') not in self.group:
                        continue
                    if self.slaveFailed.get(sid, 0) >= MAX_FAILED:
                        continue
                    if self.slaveTasks.get(sid, 0) >= self.task_per_node:
                        continue
                    if mems[i] < self.mem or cpus[i] < self.cpus:
                        continue
                    t = job.slaveOffer(str(o.hostname), cpus[i])
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
                    cpus[i] -= self.cpus
                    mems[i] -= self.mem
                    launchedTask = True

                if not launchedTask:
                    break
        
        for o in offers:
            driver.launchTasks(o.id, tasks.get(o.id.value, []))
        logger.debug("reply with %d tasks, %s cpus %s mem left", 
            len(tasks), sum(cpus), sum(mems))

    @safe
    def offerRescinded(self, driver, offer_id):
        logger.warning("rescinded offer: %s", offer_id)
        self.requestMoreResources()

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value

    def getAttribute(self, attrs, name):
        for r in attrs:
            if r.name == name:
                return r.text.value

    def createTask(self, o, job, t):
        task = mesos_pb2.TaskDescription()
        tid = "%s:%s:%s" % (job.id, t.id, t.tried)
        task.name = "task %s" % tid
        task.task_id.value = tid
        task.slave_id.value = o.slave_id.value
        task.data = cPickle.dumps((t, 1), -1)
        if len(task.data) > 10*1024:
            logger.warning("task too large: %s %d", 
                t, len(task.data))

        cpu = task.resources.add()
        cpu.name = 'cpus'
        cpu.type = 0 #mesos_pb2.Value.SCALAR
        cpu.scalar.value = self.cpus
        mem = task.resources.add()
        mem.name = 'mem'
        mem.type = 0 #mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem
        return task

    def isFinished(self, state):
        # finished, failed, killed, lost
        return state >= mesos_pb2.TASK_FINISHED

    @safe
    def statusUpdate(self, driver, status):
        tid = status.task_id.value
        state = status.state
        logger.debug("status update: %s %s", tid, state)

        jid = self.taskIdToJobId.get(tid)
        if jid in self.activeJobs:
            if self.isFinished(state):
                del self.taskIdToJobId[tid]
                self.jobTasks[jid].remove(tid)
                slave_id = self.taskIdToSlaveId[tid]
                self.slaveTasks[slave_id] -= 1
                del self.taskIdToSlaveId[tid]
           
                if state in (mesos_pb2.TASK_FINISHED, mesos_pb2.TASK_FAILED) and status.data:
                    try:
                        tid,reason,result,accUpdate = cPickle.loads(status.data)
                        if result:
                            flag, data = result
                            if flag >= 2:
                                data = open(data).read()
                                flag -= 2
                            if flag == 0:
                                result = marshal.loads(data)
                            else:
                                result = cPickle.loads(data)
                        return self.activeJobs[jid].statusUpdate(tid, state, 
                            reason, result, accUpdate)
                    except EOFError, e:
                        logger.warning("error when cPickle.loads(): %s, data:%s", e, len(status.data))

                # killed, lost, load failed
                tid = int(tid.split(':')[1])
                self.activeJobs[jid].statusUpdate(tid, state, status.data)
                self.slaveFailed[slave_id] = self.slaveFailed.get(slave_id,0) + 1
        else:
            logger.debug("Ignoring update from TID %s " +
                "because its job is gone", tid)

    @safe
    def error(self, driver, code, message):
        logger.error("Mesos error: %s (code: %s)", message, code)
        self.requestMoreResources()
#        if self.activeJobs:
#            for id, job in self.activeJobs.items():
#                try:
#                    job.error(code, message)
#                except Exception:
#                    raise

    #@safe
    def stop(self):
        logger.debug("stop scheduler")
        if self.driver:
            self.driver.stop(False)
    #        self.lock.release()
            logger.debug("wait for join mesos driver thread")
    #        self.driver.join()
    #        self.lock.acquire()
    #         self.driver = None

    def defaultParallelism(self):
        return 16

    def frameworkMessage(self, driver, slave, executor, data):
        logger.warning("[slave %s] %s", slave.value, data)

    def slaveLost(self, driver, slave):
        logger.warning("slave %s lost", slave.value)
        self.slaveTasks.pop(slave.value, 0)
        self.slaveFailed[slave.value] = MAX_FAILED

    def killTask(self, task):
        jid = self.taskIdToJobId.get(task.id)
        if jid :
            tid = mesos_pb2.TaskID()
            tid.value = "%s:%s:%s" % (jid, task.id, task.tried)
            self.driver.killTask(tid)
        else:
            logger.warning("can not kill %s because of job had gone", task)

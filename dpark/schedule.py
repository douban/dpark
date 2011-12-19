import os, sys
import socket
import logging
import cPickle
import threading, Queue
import time
import random

import zmq
import mesos
import mesos_pb2

from dependency import *
from accumulator import *
from task import *
from job import *
from env import env
from broadcast import Broadcast

EXECUTOR_MEMORY = 512

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

POLL_TIMEOUT = 0.1
RESUBMIT_TIMEOUT = 60

class Stage:
    def __init__(self, rdd, shuffleDep, parents):
        self.id = self.newId()
        self.rdd = rdd
        self.shuffleDep = shuffleDep
        self.parents = parents
        self.numPartitions = len(rdd)
        self.outputLocs = [[]] * self.numPartitions

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
        logging.debug("new stage: %s", stage) 
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
        
        logging.debug("Final stage: %s, %d", finalStage, numOutputParts)
        logging.debug("Parents of final stage: %s", finalStage.parents)
        logging.debug("Missing parents: %s", self.getMissingParentStages(finalStage))
       
        if allowLocal and not finalStage.parents and numOutputParts == 1:
            split = finalRdd.splits[outputParts[0]]
            return [func(finalRdd.iterator(split))]

        def submitStage(stage):
            logging.debug("submit stage %s", stage)
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
            logging.debug("add to pending %s tasks", len(tasks))
            myPending |= set(t.id for t in tasks)
            self.submitTasks(tasks)

        submitStage(finalStage)

        while numFinished != numOutputParts:
           evt = self.completionEvents.get(0.1)
           if not evt:
               if failed and time.time() > lastFetchFailureTime + RESUBMIT_TIMEOUT:
                   logging.debug("Resubmitting failed stages")
                   self.updateCacheLocs()
                   for stage in failed:
                       submitStage(stage)
                   failed.clear()
               else:
                   time.sleep(0.2)
               continue
               
           task = evt.task
           stage = self.idToStage[task.stageId]
           logging.debug("remove from pedding %s", task)
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
                       logging.debug("%s finished; looking for newly runnable stages", stage)
                       running.remove(stage)
                       if stage.shuffleDep != None:
                           self.mapOutputTracker.registerMapOutputs(
                                   stage.shuffleDep.shuffleId,
                                   [l[0] for l in stage.outputLocs])
                       self.updateCacheLocs()
                       newlyRunnable = set(stage for stage in waiting if not self.getMissingParentStages(stage))
                       waiting -= newlyRunnable
                       running |= newlyRunnable
                       logging.debug("newly runnable: %s, %s", waiting, newlyRunnable)
                       for stage in newlyRunnable:
                           submitMissingTasks(stage)
           else:
               logging.error("task %s failed", task)
               if isinstance(evt.reason, FetchFailed):
                   pass
               else:
                   logging.error("%s %s %s", evt.reason, type(evt.reason), evt.reason.message)
                   raise evt.reason

        return results

    def getPreferredLocs(self, rdd, partition):
        if rdd.shouldCache:
            cached = self.getCacheLocs(rdd)[partition]
            if cached:
                return cached
        rddPrefs = rdd.preferredLocations(rdd.splits[partition])
        if rddPrefs:
            return rddPrefs
        for d in rdd.dependencies:
            if isinstance(d, NarrowDependency):
                for inPart in d.getParents(partition):
                    locs = self.getPreferredLocs(d.rdd, inPart)
                    if locs:
                        return locs
        return []


def run_task(task, aid):
    logging.debug("Running task %r", task)
    try:
        Accumulator.clear()
        result = task.run(aid)
        accumUpdates = Accumulator.values()
        return (task.id, Success(), result, accumUpdates)
    except Exception, e:
        logging.error("error in task %s", task)
        import traceback
        traceback.print_exc()
        return (task.id, OtherFailure("exception:" + str(e)), None, None)


class LocalScheduler(DAGScheduler):
    attemptId = 0
    def nextAttempId(self):
        self.attemptId += 1
        return self.attemptId

    def submitTasks(self, tasks):
        logging.debug("submit tasks %s in LocalScheduler", tasks)
        for task in tasks:
#            task = cPickle.loads(cPickle.dumps(task, -1))
            _, reason, result, update = run_task(task, self.nextAttempId())
            self.taskEnded(task, reason, result, update)
    def stop(self):
        pass

def run_task_in_process(task, tid, environ):
    logging.debug("run task in process %s %s %s",
        task, tid, environ)
    from env import env
    env.start(False, environ)
    return run_task(task, tid)

class MultiProcessScheduler(LocalScheduler):
    def __init__(self, threads):
        LocalScheduler.__init__(self)
        self.threads = threads
        from  multiprocessing import Pool
        self.pool = Pool(threads or 2)
        self.tasks = {}

    def start(self):
        pass

    def submitTasks(self, tasks):
        def callback(args):
            logging.debug("got answer: %s", args)
            tid, reason, result, update = args
            task = self.tasks.pop(tid)
            self.taskEnded(task, reason, result, update)

        for task in tasks:
            logging.debug("put task async: %s", task)
            self.tasks[task.id] = task
            self.pool.apply_async(run_task_in_process,
                [task, self.nextAttempId(), env.environ],
                callback=callback)

    def stop(self):
        self.pool.close()
        self.pool.join()
        logging.debug("process pool stopped")


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

class MesosScheduler(mesos.Scheduler, DAGScheduler):

    def __init__(self, master, options):
        DAGScheduler.__init__(self)
        self.master = master
        self.use_self_as_exec = options.self
        self.cpus = options.cpus
        self.mem = options.mem
        self.isRegistered = False
        self.activeJobs = {}
        self.activeJobsQueue = []
        self.taskIdToJobId = {}
        self.taskIdToSlaveId = {}
        self.jobTasks = {}
        self.driver = None
        self.slavesWithExecutors = set()
        self.lock = threading.RLock()

    def start(self):
        name = '[dpark@%s] ' % socket.gethostname()
        name += os.path.abspath(sys.argv[0]) + ' ' + ' '.join(sys.argv[1:])
        self.driver = mesos.MesosSchedulerDriver(self, name,
            self.getExecutorInfo(), self.master)
        self.driver.start()
        logging.debug("Mesos Scheudler driver started")

    def getExecutorInfo(self):
        info = mesos_pb2.ExecutorInfo()

        if self.use_self_as_exec:
            info.uri = os.path.abspath(sys.argv[0])
            info.executor_id.value = sys.argv[0]
        else:
            dir = os.path.dirname(__file__)
            info.uri = os.path.abspath(os.path.join(dir, 'executor'))
            info.executor_id.value = "default"
        
        mem = info.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Resource.SCALAR
        mem.scalar.value = EXECUTOR_MEMORY
        info.data = cPickle.dumps((os.getcwd(), sys.path,
            self.defaultParallelism(), env.environ), -1)
        return info

    def submitTasks(self, tasks):
        with self.lock:
            self._submitTasks(tasks)

    def _submitTasks(self, tasks):
        logging.info("Got a job with %d tasks", len(tasks))
        job = SimpleJob(self, tasks)
        self.activeJobs[job.id] = job
        self.activeJobsQueue.append(job)
        logging.debug("Adding job with ID %d", job.id)
        self.jobTasks[job.id] = set()
        
        self.waitForRegister()
        self.driver.reviveOffers()

    def jobFinished(self, job):
        logging.debug("job %s finished", job.id)
        del self.activeJobs[job.id]
        self.activeJobsQueue.remove(job) 
        for id in self.jobTasks[job.id]:
            del self.taskIdToJobId[id]
            del self.taskIdToSlaveId[id]
        del self.jobTasks[job.id]

    def registered(self, driver, fid):
        self.isRegistered = True
        logging.debug("is registered: %s", fid)

    def waitForRegister(self):
        while not self.isRegistered:
            time.sleep(0.01)

    def resourceOffers(self, driver, offers):
        if not self.activeJobs:
            for o in offers:
                driver.launchTasks(o.id, [])
            return

        random.shuffle(offers)
        cpus = [self.getResource(o.resources, 'cpus') for o in offers]
        mems = [self.getResource(o.resources, 'mem') 
                - (o.slave_id.value not in self.slavesWithExecutors
                    and EXECUTOR_MEMORY or 0)
                for o in offers]
        logging.debug("get %d offers (%s cpus, %s mem), %d jobs", 
            len(offers), sum(cpus), sum(mems), len(self.activeJobs))

        tasks = {}
        launchedTask = False
        for job in self.activeJobsQueue:
            while True:
                launchedTask = False
                for i,o in enumerate(offers):
                    if mems[i] < self.mem or cpus[i] < self.cpus:
                        continue
                    t = job.slaveOffer(str(o.hostname), cpus[i])
                    if not t:
                        continue
                    tasks.setdefault(o.id.value, []).append(self.createTask(o, job, t))

                    logging.debug("dispatch %s into %s", t, o.hostname)
                    self.jobTasks[job.id].add(t.id)
                    self.taskIdToJobId[t.id] = job.id
                    self.taskIdToSlaveId[t.id] = o.slave_id.value
                    cpus[i] -= self.cpus
                    mems[i] -= self.mem
                    self.slavesWithExecutors.add(o.slave_id.value)
                    launchedTask = True

                if not launchedTask:
                    break
        
        for o in offers:
            driver.launchTasks(o.id, tasks.get(o.id.value, []))
        logging.debug("reply with %d tasks, %s cpus %s mem left", 
            len(tasks), sum(cpus), sum(mems))

    def offerRescinded(self, driver, offers):
        for o in offers:
            logging.error("resource rescinded: %s", o)
        driver.reviveOffers()

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value

    def createTask(self, o, job, t):
        task = mesos_pb2.TaskDescription()
        tid = "%s:%s:%s" % (job.id, t.id, t.tried)
        task.name = "task %s" % tid
        task.task_id.value = tid
        task.slave_id.value = o.slave_id.value
        task.data = cPickle.dumps((t, 1), -1)
        if len(task.data) > 10*1024:
            logging.warning("task too large: %s %d", 
                t, len(task.data))

        cpu = task.resources.add()
        cpu.name = 'cpus'
        cpu.type = mesos_pb2.Resource.SCALAR
        cpu.scalar.value = self.cpus
        mem = task.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Resource.SCALAR
        mem.scalar.value = self.mem
        return task

    def isFinished(self, state):
        return state >= mesos_pb2.TASK_FINISHED

    def statusUpdate(self, driver, status):
        tid = status.task_id.value.split(':')
        if len(tid) != 3 or not tid[1].isdigit():
            logging.warning("invalid task id: %s", status.task_id.value)
            return
        tid = int(tid[1])
        state = status.state
        logging.debug("status update: %s %s", tid, state)

        if (state == mesos_pb2.TASK_LOST 
            and tid in self.taskIdToSlaveId 
            and self.taskIdToSlaveId[tid] in self.slavesWithExecutors):
            logging.warning("slave %s lost", self.taskIdToSlaveId[tid])
            self.slavesWithExecutors.remove(self.taskIdToSlaveId[tid])

        jid = self.taskIdToJobId.get(tid)
        if jid in self.activeJobs:
            if self.isFinished(state):
#                print self.taskIdToJobId
                del self.taskIdToJobId[tid]
                del self.taskIdToSlaveId[tid]
                if tid in self.jobTasks[jid]:
                    self.jobTasks[jid].remove(tid)
            if status.data:
                _,reason,result,accUpdate = cPickle.loads(status.data)
                self.activeJobs[jid].statusUpdate(tid, state, 
                    reason, result, accUpdate)
            else:
                self.activeJobs[jid].statusUpdate(tid, state)
        else:
            logging.debug("Ignoring update from TID %s " +
                "because its job is gone", tid)

    def error(self, driver, code, message):
        logging.error("Mesos error: %s (code: %s)", message, code)
        self.driver.reviveOffers()
#        if self.activeJobs:
#            for id, job in self.activeJobs.items():
#                try:
#                    job.error(code, message)
#                except Exception:
#                    raise

    def stop(self):
        if self.driver:
            #slave = mesos_pb2.SlaveID()
            #executor = mesos_pb2.ExecutorID()
            #executor.value = 'default'
            #for id in self.slavesWithExecutors:
            #    slave.value = id
            #    self.driver.sendFrameworkMessage(slave, executor, "shutdown")
            self.driver.stop(False)
            self.driver.join()

    def defaultParallelism(self):
        return 16

    def frameworkMessage(self, driver, slave, executor, data):
        logging.warning("got message from slave %s %s %s", 
                slave.value, executor.value, data)

    def slaveLost(self, driver, slave):
        logging.warning("slave %s lost", slave.value)
        if slave.value in self.slavesWithExecutors:
            self.slavesWithExecutors.remove(slave.value)

    def killTask(self, task):
        jid = self.taskIdToJobId.get(task.id)
        if jid :
            tid = mesos_pb2.TaskID()
            tid.value = "%s:%s:%s" % (jid, task.id, task.tried)
            self.driver.killTask(tid)
        else:
            logging.warning("can not kill %s because of job had gone", task)

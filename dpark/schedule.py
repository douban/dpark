import os, sys
import socket
import logging
import cPickle
import threading, Queue
import time

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
TASK_MEMORY = 50

class TaskEndReason:
    pass

class Success(TaskEndReason):
    pass

class FetchFailed:
    def __init__(self, serverUri, shuffleId, mapId, reduceId):
        self.serverUri = serverUri
        self.shuffleId = shuffleId
        self.mapId = mapId
        self.reduceId = reduceId

class OtherFailure:
    def __init__(self, message):
        self.message = message

POLL_TIMEOUT = 1
RESUBMIT_TIMEOUT = 60

class Stage:
    def __init__(self, id, rdd, shuffleDep, parents):
        self.id = id
        self.rdd = rdd
        self.shuffleDep = shuffleDep
        self.parents = parents
        self.isShuffleMap = shuffleDep != None
        self.numPartitions = len(rdd.splits)
        self.outputLocs = [[]] * self.numPartitions
        self.numAvailableOutputs = 0

    def __str__(self):
        return '<stage(%d) %s>' % (self.id, self.rdd)

    def __hash__(self):
        return self.id

    @property
    def isAvailable(self):
        if not self.parents and not self.isShuffleMap:
            return True
        return self.numAvailableOutputs == self.numPartitions

    def addOutputLoc(self, partition, host):
        prevList = self.outputLocs[partition]
        self.outputLocs[partition] = [host] + prevList
        if not prevList:
            self.numAvailableOutputs += 1
        
    def removeOutput(self, partition, host):
        prev = self.outputLocs[partition]
        self.outputLocs[partition] = [h for h in prev if h != host]
        if prev and not self.outputLocs[partition]:
            self.numAvailableOutputs -= 1


class Scheduler:
    def start(self):pass
    def waitForRegister(self): pass
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
    
    nextStageId = 0

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
    
    def newStageId(self):
        self.nextStageId += 1
        return self.nextStageId

    def getCacheLocs(self, rdd):
        return self.cacheLocs.get(rdd.id, [[] for i in range(len(rdd.splits))])

    def updateCacheLocs(self):
        self.cacheLocs = self.cacheTracker.getLocationsSnapshot()

    def newStage(self, rdd, shuffleDep):
        if rdd.shouldCache:
            self.cacheTracker.registerRDD(rdd.id, len(rdd.splits))
        id = self.newStageId()
        stage = Stage(id, rdd, shuffleDep, self.getParentStages(rdd))
        self.idToStage[id] = stage
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
                self.cacheTracker.registerRDD(r.id, len(r.splits))
            for dep in r.dependencies:
                if isinstance(dep, ShuffleDependency):
                    parents.add(self.getShuffleMapStage(dep))
                else:
                    visit(dep.rdd)
        visit(rdd)
        return list(parents)

    def getShuffleMapStage(self, shuf):
        stage = self.shuffleToMapStage.get(shuf.shuffleId, None)
        if stage is None:
            stage = self.newStage(shuf.rdd, shuf)
            self.shuffleToMapStage[shuf.shuffleId] = stage
        return stage

    def getMissingParentStages(self, stage):
        missing = set()
        visited = set()
        def visit(r):
            if r.id in visited:
                return
            visited.add(r.id)
            locs = self.getCacheLocs(r)
            for i in range(len(r.splits)):
                if not locs[i]:
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
            taskContext = TaskContext(finalStage.id, outputParts[0], 0)
            return [func(taskContext, finalRdd.iterator(split))]

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
            if stage == finalStage:
                for i in range(numOutputParts):
                    if not finished[i]:
                        part = outputParts[i]
                        locs = self.getPreferredLocs(finalRdd, part)
                        tasks.append(ResultTask(finalStage.id, finalRdd, func, part, locs, i))
            else:
                for p in range(stage.numPartitions):
                    if not stage.outputLocs[p]:
                        locs = self.getPreferredLocs(stage.rdd, p)
                        tasks.append(ShuffleMapTask(stage.id, stage.rdd, stage.shuffleDep, p, locs))
            logging.debug("add to pending %r", tasks)
            myPending |= set(t.id for t in tasks)
            self.submitTasks(tasks)

        submitStage(finalStage)

        while numFinished != numOutputParts:
           evt = self.completionEvents.get(POLL_TIMEOUT)
           if evt:
               task = evt.task
               stage = self.idToStage[task.stageId]
               logging.debug("remove from pedding %s %s", pendingTasks[stage], task)
               pendingTasks[stage].remove(task.id)
               if isinstance(evt.reason, Success):
                   # ended
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
                   logging.error("unexpected task %s", task)
                   if isinstance(evt.reason, FetchFailed):
                       pass
                   else:
                       logging.error("%s %s %s", evt.reason, type(evt.reason), isinstance(evt.reason, Success))
                       raise evt.reason
                       raise

           if failed and time.time() > lastFetchFailureTime + RESUBMIT_TIMEOUT:
               logging.debug("Resubmitting failed stages")
               self.updateCacheLocs()
               for stage in failed:
                   submitStage(stage)
               failed.clear()

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
        return (task, Success(), result, accumUpdates)
    except Exception, e:
        logging.error("error in task %s", task)
        import traceback
        traceback.print_exc()
        return (task, OtherFailure("exception:" + str(e)), None, None)


class LocalScheduler(DAGScheduler):
    attemptId = 0
    def nextAttempId(self):
        self.attemptId += 1
        return self.attemptId

    def submitTasks(self, tasks):
        logging.debug("submit tasks %s in LocalScheduler", tasks)
        for task in tasks:
            self.taskEnded(*run_task(task, self.nextAttempId()))
    def stop(self):
        pass

class MultiThreadScheduler(LocalScheduler):
    def __init__(self, threads):
        LocalScheduler.__init__(self)
        self.nthreads = threads
        self.queue = Queue.Queue()

    def start(self):
        def worker(queue):
            logging.debug("worker thread started")
            #env.start(False)
            while True:
                r = queue.get()
                if r is None:
                    self.queue.task_done()
                    break
                func, args = r
                func(*args)
                self.queue.task_done()
            env.stop()
            logging.debug("worker thread stopped")

        self.threads = []
        for i in range(self.nthreads):
            t = threading.Thread(target=worker, args=[self.queue])
            t.daemon = True
            t.start()
            self.threads.append(t)


    def submitTasks(self, tasks):
        logging.debug("submit tasks %s in MultiThreadScheduler", tasks)
        def func(task, aid):
            self.taskEnded(*run_task(task, aid))
        for task in tasks:
            self.queue.put((func, (task, self.nextAttempId())))

    def stop(self):
        for i in range(len(self.threads)*2):
            self.queue.put(None)
        #self.queue.join()
        for t in self.threads:
            t.join()
        logging.debug("all threads are stopped")

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
        self.pool = Pool(threads)

    def start(self):
        pass

    def submitTasks(self, tasks):
        def callback(args):
            logging.debug("got answer: %s", args)
            self.taskEnded(*args)
        for task in tasks:
            logging.debug("put task async: %s", task)
            self.pool.apply_async(run_task_in_process,
                [task, self.nextAttempId(), env.environ],
                callback=callback)

    def stop(self):
        self.pool.close()
        self.pool.join()
        logging.debug("process pool stopped")


class MesosScheduler(mesos.Scheduler, DAGScheduler):

    def __init__(self, master, name='dpark'):
        DAGScheduler.__init__(self)
        self.master = master
        self.name = name
        self.isRegistered = False
        self.activeJobs = {}
        self.activeJobsQueue = []
        self.taskIdToJobId = {}
        self.taskIdToSlaveId = {}
        self.jobTasks = {}
        self.driver = None
        self.slavesWithExecutors = set()

    def start(self):
        self.driver = mesos.MesosSchedulerDriver(self, self.master)
        self.driver.start()
        logging.debug("Mesos Scheudler driver started")

    def getFrameworkName(self, driver):
        return self.name

    def getExecutorInfo(self, driver):
        dir = os.path.dirname(__file__)
        path = os.path.abspath(os.path.join(dir, 'executor'))
        info = mesos_pb2.ExecutorInfo()
        info.executor_id.value = "default"
        info.uri = path
        mem = info.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Resource.SCALAR
        mem.scalar.value = EXECUTOR_MEMORY
        info.data = cPickle.dumps((os.getcwd(),
            self.defaultParallelism(), env.environ))
        return info

    def submitTasks(self, tasks):
        logging.debug("Got a job with %d tasks", len(tasks))
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

    def resourceOffer(self, driver, oid, offers):
        logging.debug("get %d offers, %d jobs", len(offers), len(self.activeJobs))
        for o in offers:
            logging.debug("offers: %s", o)
        if not self.activeJobs:
            return driver.replyToOffer(oid, [], {"timeout": "0.1"})
        
        tasks = []
        availableCpus = [self.getResource(o.resources, 'cpus')
                            for o in offers]
        enoughMem = [self.getResource(o.resources, 'mem')>EXECUTOR_MEMORY
                    or o.slave_id.value in self.slavesWithExecutors
                        for o in offers]
        launchedTask = False
        for job in self.activeJobsQueue:
            while True:
                launchedTask = False
                for i,o in enumerate(offers):
                    if not enoughMem[i]: continue
                    t = job.slaveOffer(o.hostname, availableCpus[i])
                    if not t: continue
                    task = mesos_pb2.TaskDescription()
                    task.name = "task %s:%s" % (job.id, t.id)
                    task.task_id.value = str(t.id)
                    task.slave_id.value = o.slave_id.value
                    task.data = cPickle.dumps((t, 1))
                    cpu = task.resources.add()
                    cpu.name = 'cpus'
                    cpu.type = mesos_pb2.Resource.SCALAR
                    cpu.scalar.value = 1
                    mem = task.resources.add()
                    mem.name = 'mem'
                    mem.type = mesos_pb2.Resource.SCALAR
                    mem.scalar.value = TASK_MEMORY
                    tasks.append(task)

                    logging.debug("dispatch %s into %s", t, o.hostname)
                    self.jobTasks[job.id].add(t.id)
                    self.taskIdToJobId[t.id] = job.id
                    self.taskIdToSlaveId[t.id] = o.slave_id.value
                    self.slavesWithExecutors.add(o.slave_id.value)
                    availableCpus[i] -= 1
                    launchedTask = True

                if not launchedTask:
                    break
        
        if not tasks:
            logging.debug("reply to %s with %d tasks", oid, len(tasks))
        driver.replyToOffer(oid, tasks, {"timeout": "1"})

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value

    def isFinished(self, state):
        return state >= mesos_pb2.TASK_FINISHED

    def statusUpdate(self, driver, status):
        tid = int(status.task_id.value)
        state = status.state
        logging.debug("status update: %s %s", tid, state)
        if (state == mesos_pb2.TASK_LOST 
            and tid in self.taskIdToSlaveId 
            and self.taskIdToSlaveId[tid] in self.slavesWithExecutors):
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
            logging.error("Ignoring update from TID %s " +
                "because its job is gone", tid)

    def error(self, driver, code, message):
        logging.error("Mesos error: %s (code: %s)", message, code)
        if self.activeJobs:
            for id, job in self.activeJobs.items():
                try:
                    job.error(code, message)
                except Exception:
                    raise

    def stop(self):
        if self.driver:
            #slave = mesos_pb2.SlaveID()
            #executor = mesos_pb2.ExecutorID()
            #executor.value = 'default'
            #for id in self.slavesWithExecutors:
            #    slave.value = id
            #    self.driver.sendFrameworkMessage(slave, executor, "shutdown")
            self.driver.stop()
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

    def offerRescinded(self, driver, offer):
        logging.warning("offer rescinded: %s", offer)


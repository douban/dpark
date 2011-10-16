import logging
import pickle
import threading, Queue

from dependency import *
from accumulator import *
from task import *
from env import env

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

    def __str__(self):
        return "Stage %d" % self.id

    def __hash__(self):
        return self.id


class Scheduler:
    def start(self):pass
    def waitForRegister(self): pass
    def runJob(self, rdd, func, partitions, allowLocal): pass
    def stop(self): pass
    def defaultParallelism(self):pass

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
        self.cacheTracker = env.cacheTracker
        self.mapOutputTracker = env.mapOutputTracker

    def submitTasks(self, tasks):
        raise NotImplementedError

    def taskEnded(self, task, reason, result, accumUpdates):
        self.completionEvents.put(CompletionEvent(task, reason, result, accumUpdates))
    
    def newStageId(self):
        self.nextStageId += 1
        return self.nextStageId

    def getCacheLocs(self, rdd):
        return self.cacheLocs.get(rdd.id, {})

    def updateCacheLocs(self):
        self.cacheLocs = self.cacheTracker.getLocationsSnapshot()

    def newStage(self, rdd, shuffleDep):
        self.cacheTracker.registerRDD(rdd.id, len(rdd.splits))
        id = self.newStageId()
        stage = Stage(id, rdd, shuffleDep, self.getParentStages(rdd))
        self.idToStage[id] = stage
        return stage

    def getParentStages(self, rdd):
        parents = set()
        visited = set()
        def visit(r):
            if r.id in visited:
                return
            visited.add(r.id)
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
                #if not locs[i]:
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
        
        logging.info("Final stage: %s, %d", finalStage, numOutputParts)
        logging.info("Parents of final stage: %s", finalStage.parents)
        logging.info("Missing parents: %s", self.getMissingParentStages(finalStage))
       
#        if not finalStage.parents:
#            rs = [func(TaskContext(finalStage.id, outputParts[i], i), 
#                         finalRdd.iterator(finalRdd.splits[outputParts[i]]))
#                    for i in range(numOutputParts)]
#            return rs
#
        if allowLocal and not finalStage.parents and numOutputParts == 1:
            split = finalRdd.Splits[outputParts[0]]
            taskContext = TaskContext(finalStage.id, outputParts[0], 0)
            return list(func(taskContext, finalRdd.iterator(split)))

        def submitStage(stage):
            logging.info("submit stage %s", stage)
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
            logging.info("add to pending %r", tasks)
            myPending |= set(t.id for t in tasks)
            self.submitTasks(tasks)

        submitStage(finalStage)

        while numFinished != numOutputParts:
           evt = self.completionEvents.get(POLL_TIMEOUT)
           # FIXME
           if evt:
               task = evt.task
               stage = self.idToStage[task.stageId]
               logging.info("remove from pedding %s %s", pendingTasks[stage], task)
               pendingTasks[stage].remove(task.id)
               if isinstance(evt.reason, Success):
                   # ended
                   Accumulator.add(evt.accumUpdates)
                   if isinstance(task, ResultTask):
                       results[task.outputId] = evt.result
                       finished[task.outputId] = True
                       numFinished += 1
                   elif isinstance(task, ShuffleMapTask):
                       stage = self.idToStage[task.stageId]
                       stage.addOutputLoc(task.partition, evt.result)
                       if not pendingTasks[stage]:
                           logging.info("%s finished; looking for newly runnable stages", stage)
                           running.remove(stage)
                           if stage.shuffleDep != None:
                               self.mapOutputTracker.registerMapOutputs(
                                       stage.shuffleDep.shuffleId,
                                       [l[0] for l in stage.outputLocs])
                           self.updateCacheLocs()
                           newlyRunnable = set(stage for stage in waiting if not self.getMissingParentStages(stage))
                           waiting -= newlyRunnable
                           running |= newlyRunnable
                           logging.info("newly runnable: %s, %s", waiting, newlyRunnable)
                           for stage in newlyRunnable:
                               submitMissingTasks(stage)
               else:
                   raise
                   if isinstance(evt.reason, FetchFailed):
                       pass
                   else:
                       raise

           if failed and time.time() > lastFetchFailureTime + RESUBMIT_TIMEOUT:
               logging.info("Resubmitting failed stages")
               self.updateCacheLocs()
               for stage in failed:
                   submitStage(stage)
               failed.clear()

        return results

    def getPreferredLocs(self, rdd, partition):
        cached = self.getCacheLocs(rdd)[partition]
        if cached is not None:
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



class ThreadPool:
    def __init__(self, nthreads):
        self.queue = Queue.Queue()

        def worker(queue):
            while True:
                r = queue.get()
                if r is None:
                    self.queue.task_done()
                    break
                func, args = r
                func(*args)
                self.queue.task_done()
        self.threads = []
        for i in range(nthreads):
            t = threading.Thread(target=worker, args=[self.queue])
            t.daemon = True
            t.start()
            self.threads.append(t)

    def submit(self, func, *args):
        self.queue.put((func, args))

    def stop(self):
        for i in range(len(self.threads)):
            self.queue.put(None)
        self.queue.join()
        for t in self.threads:
            t.join()
        logging.info("all threads are stopped")


class LocalScheduler(DAGScheduler):
    attemptId = 0
    def __init__(self, threads):
        DAGScheduler.__init__(self)
        self.pool = ThreadPool(threads)

    def nextAttempId(self):
        self.attemptId += 1
        return self.attemptId
    
    def submitTasks(self, tasks):
        logging.info("submit tasks %s", tasks)
        for task in tasks:
            def func(task, aid):
                logging.info("Running task %r", task)
                try:
                    Accumulator.clear()
                    result = task.run(aid)
                    accumUpdates = Accumulator.values()
                    self.taskEnded(task, Success(), result, accumUpdates)
                except Exception, e:
                    logging.info("error in task %s", task)
                    import traceback
                    traceback.print_exc()
                    raise
                    self.taskEnded(task, OtherFailure("exception:" + str(e)), None, None)

            aid = self.nextAttempId()
            #self.pool.submit(func, task, aid)
            func(task, aid)


    def stop(self):
        self.pool.stop()

def process_worker(task, aid):
    try:
        Accumulator.clear()
        logging.info("run task %s %d", task, aid)
        result = task.run(aid)
        accumUpdates = Accumulator.values()
        return (task, Success(), result, accumUpdates)
    except Exception, e:
        logging.info("error in task %s", task)
        import tracback
        traceback.print_exec()
        return ((task, OtherFailure("exception:" + str(e)), None, None))

class LocalProcessScheduler(LocalScheduler):
    def __init__(self, threads):
        DAGScheduler.__init__(self)
        from multiprocessing import Pool, Queue
        self.reply = Queue()
        self.pool = Pool(threads)
        
    def start(self):
        def read_reply():
            task, reason, result, update = self.reply.get()
            self.taskEnded(task, reason, result, update)
        self.t = threading.Thread(target=read_reply)
        self.t.daemon = True
        self.t.start()

    def submitTasks(self, tasks):
        def callback(args):
            self.taskEnded(*args)

        for task in tasks:
            aid = self.nextAttempId()
            logging.info("put task async %s", task)
            self.pool.apply_async(process_worker, (task, aid), {}, callback)

    def stop(self):
        self.pool.close()
        self.pool.join()


class MesosScheduler(DAGScheduler):

    nextJobId = 0
    nextTaskId = 0

    def __init__(self, sc, master, name='dpark'):
        DAGScheduler.__init__(self, master, name)
        self.sc = sc
        self.isRegistered = False
        self.activeJobs = {}
        self.activeJobsQueue = []
        self.taskIdToJobId = {}
        self.taskIdToSlaveId = {}
        self.jobTasks = {}
        self.driver = None
        self.slavesWithExecutors = {}

    @classmethod
    def newJobId(cls):
        cls.nextJobId += 1
        return cls.nextJobId
    
    @classmethod
    def newTaskId(cls):
        cls.nextTaskId += 1
        return cls.nextTaskId

    def start(self):
        def run():
            self.driver = MesosSchedulerDriver(self, self.master)
            try:
                ret = self.driver.run()
            except Exception:
                logging.info("run failed")
        t = Thread(target=run)
        t.daemon = True
        t.start()

    def getFrameworkName(self, driver):
        return self.name

    def getExecutorInfo(self, driver):
        pass

    def submitTasks(self, tasks):
        logging.info("Got a job with %d tasks", len(tasks))
        self.waitForRegister()
        jobId = self.newJobId()
        myJob = SimpleJob(self, tasks, jobId)
        self.activeJobs[jobId] = myJob
        self.activeJobsQueue.append(myJob)
        logging.info("Adding job with ID %d", jobId)
        self.jobTasks[jobId] = {}
        driver.reviveOffers()

    def jobFinished(self, job):
        del self.activeJobs[job.getId]
        #TODO

    def registered(self, driver, fid):
        self.isRegistered = True

    def waitForRegister(self):
        while not self.isRegistered:
            time.sleep(0.1)

    def resourceOffer(self, driver, oid, offers):
        # TODO
        pass

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scala.value

    def isFinished(self, state):
        return state == TaskState.TASK_FINISHED

    def statusUpdate(self, driver, status):
        pass # TODO

    def error(self, driver, code, message):
        pass # TODO

    def stop(self):
        self.driver.stop()

    def defaultParallelism(self):
        return 2

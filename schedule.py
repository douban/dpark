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

EXECUTOR_MEMORY = 512

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
        return self.cacheLocs.get(rdd.id, {})

    def updateCacheLocs(self):
        self.cacheLocs = self.cacheTracker.getLocationsSnapshot()

    def newStage(self, rdd, shuffleDep):
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
       
        if allowLocal and not finalStage.parents and numOutputParts == 1:
            split = finalRdd.splits[outputParts[0]]
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
           if evt:
               task = evt.task
               stage = self.idToStage[task.stageId]
               logging.info("remove from pedding %s %s", pendingTasks[stage], task)
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
                   logging.error("unexpected task %s", task)
                   if isinstance(evt.reason, FetchFailed):
                       pass
                   else:
                       logging.error("%s %s %s", evt.reason, type(evt.reason), isinstance(evt.reason, Success))
                       raise evt.reason
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


def run_task(task, aid):
    logging.debug("Running task %r", task)
    try:
        Accumulator.clear()
        result = task.run(aid)
        accumUpdates = Accumulator.values()
        return (task, Success(), result, accumUpdates)
    except Exception, e:
        logging.info("error in task %s", task)
        import traceback
        traceback.print_exc()
        raise
        return (task, OtherFailure("exception:" + str(e)), None, None)


class LocalScheduler(DAGScheduler):
    attemptId = 0
    def nextAttempId(self):
        self.attemptId += 1
        return self.attemptId

    def submitTasks(self, tasks):
        logging.info("submit tasks %s in LocalScheduler", tasks)
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
            logging.info("worker thread started")
            env.start(False)
            while True:
                r = queue.get()
                if r is None:
                    self.queue.task_done()
                    break
                func, args = r
                func(*args)
                self.queue.task_done()
            env.stop()
            logging.info("worker thread stopped")

        self.threads = []
        for i in range(self.nthreads):
            t = threading.Thread(target=worker, args=[self.queue])
            t.daemon = True
            t.start()
            self.threads.append(t)


    def submitTasks(self, tasks):
        logging.info("submit tasks %s in MultiThreadScheduler", tasks)
        def func(task, aid):
            self.taskEnded(*run_task(task, aid))
        for task in tasks:
            self.queue.put((func, (task, self.nextAttempId())))

    def stop(self):
        for i in range(len(self.threads)):
            self.queue.put(None)
        self.queue.join()
        for t in self.threads:
            t.join()
        logging.info("all threads are stopped")

def run_task_in_process(task, aid, *args):
    logging.debug("run_task_in_process %s %s %s %s %s", 
            task, aid, *args)
    env.start(False, *args)
    return run_task(task, aid) 

class MultiProcessScheduler(LocalScheduler):
    def __init__(self, threads):
        LocalScheduler.__init__(self)
        self.threads = threads
        from  multiprocessing import Pool
        self.pool = Pool(threads)

    def start(self):
        pass
#        logging.info("start processes")
#        for i in range(self.threads*2):
#            self.pool.apply_async(restart_env, [cacheAddr, outputAddr])
#        logging.info("apply async done")

    def submitTasks(self, tasks):
        def callback(args):
            logging.info("got answer: %s", args)
            self.taskEnded(*args)
        cacheAddr = env.cacheTracker.addr
        outputAddr = env.mapOutputTracker.addr
        for task in tasks:
            logging.info("put task async: %s", task)
            self.pool.apply_async(run_task_in_process, 
                [task, self.nextAttempId(), cacheAddr, outputAddr, env.shuffleDir],
                callback=callback)

    def stop(self):
        logging.info("try to stop process pool")
        self.pool.close()
        self.pool.join()
        logging.info("process pool stopped")

class MultiProcessScheduler2(LocalScheduler):
    def __init__(self, threads):
        LocalScheduler.__init__(self)
        self.threads = threads

    def start(self):
        import subprocess
        ctx = zmq.Context()
        self.cmd = ctx.socket(zmq.PUSH)
        self.cmd_port = self.cmd.bind_to_random_port("tcp://0.0.0.0")
        self.result_port = None

        def read_reply():
            result = ctx.socket(zmq.PUB)
            self.result_port = result.bind_to_random_port("tcp://0.0.0.0")
            task, reason, result, update = cPickle.loads(result.recv())
            self.taskEnded(task, reason, result, update)

        self.t = threading.Thread(target=read_reply)
        self.t.daemon = True
        self.t.start()
        while self.result_port is None:
            time.sleep(0.01)

        env = dict(os.environ)
        env['COMMAND_ADDR'] = "tcp://%s:%d" % (socket.gethostname(), self.cmd_port)
        env['RESULT_ADDR'] = "tcp://%s:%d" % (socket.gethostname(), self.result_port)
        self.ps = [subprocess.Popen(["./executor.py"], stdin=sys.stdin, stdout=sys.stderr, stderr=sys.stderr, env=env)
                    for i in range(self.threads)]

    def submitTasks(self, tasks):
        time.sleep(1)
        for task in tasks:
            aid = self.nextAttempId()
            logging.info("put task async %s %s", task, self.cmd_port)
            self.cmd.send(cPickle.dumps((task, aid)))
            logging.info("put task async completed %s", task)

    def stop(self):
        for i in range(self.threads*2):
            self.cmd.send("")
        for p in self.ps:
            p.wait()
            logging.info("stop child %s", p)


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
        shuffleDir = env.shuffleDir
        def run():
            try:
                env.start(False, shuffleDir=shuffleDir)
                logging.info("driver thread started")
                self.driver = mesos.MesosSchedulerDriver(self, self.master)
                ret = self.driver.run()
            except Exception, e:
                logging.info("run failed: %s", e)
                raise
                os._exit(1)
        t = threading.Thread(target=run)
        t.daemon = True
        t.start()

    def getFrameworkName(self, driver):
        return self.name

    def getExecutorInfo(self, driver):
        path = os.path.abspath('./executor')
        info = mesos_pb2.ExecutorInfo()
        info.executor_id.value = "default"
        info.uri = path
        mem = info.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Resource.SCALAR
        mem.scalar.value = EXECUTOR_MEMORY
        info.data = cPickle.dumps((os.getcwd(),
            (env.cacheTracker.addr, env.mapOutputTracker.addr, env.shuffleDir)))
        return info

    def submitTasks(self, tasks):
        logging.info("Got a job with %d tasks", len(tasks))
        job = SimpleJob(self, tasks)
        self.activeJobs[job.id] = job
        self.activeJobsQueue.append(job)
        logging.info("Adding job with ID %d", job.id)
        self.jobTasks[job.id] = set()
        
        self.waitForRegister()
        self.driver.reviveOffers()

    def jobFinished(self, job):
        logging.info("job %s finished", job.id)
        del self.activeJobs[job.id]
        self.activeJobsQueue.remove(job) 
        for id in self.jobTasks[job.id]:
            del self.taskIdToJobId[id]
            del self.taskIdToSlaveId[id]
        del self.jobTasks[job.id]

    def registered(self, driver, fid):
        self.isRegistered = True
        logging.info("is registered")

    def waitForRegister(self):
        while not self.isRegistered:
            time.sleep(0.1)

    def resourceOffer(self, driver, oid, offers):
        logging.info("get  %d offers, %d jobs", len(offers), len(self.activeJobs))
        while not len(self.activeJobs):
            time.sleep(0.1)
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
                    mem.scalar.value = EXECUTOR_MEMORY
                    tasks.append(task)

                    logging.info("dispatch %s into %s", t, o.hostname)
                    self.jobTasks[job.id].add(t.id)
                    self.taskIdToJobId[t.id] = job.id
                    self.taskIdToSlaveId[t.id] = o.slave_id.value
                    self.slavesWithExecutors.add(o.slave_id.value)
                    availableCpus[i] -= 1
                    launchedTask = True

                if not launchedTask:
                    break
        
        logging.info("reply to %s with %d tasks", oid, len(tasks))
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
        logging.info("status update: %s %s", tid, state)
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
        else:
            os._exit(1)

    def stop(self):
        if self.driver:
            self.driver.stop()
            self.driver.join()

    def defaultParallelism(self):
        return 16

    def frameworkMessage(self, driver, slave, executor, data):
        logging.info("got message from slave %s %s %s", 
                slave.value, executor.name, data)

    def slaveLost(self, driver, slave):
        self.slavesWithExecutors.remove(slave.value)

    def offerRescinded(self, driver, offer):
        pass

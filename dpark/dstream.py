import os
import time
import math
import itertools
import logging
import random

from dpark.util import spawn
from dpark.dependency import Partitioner, HashPartitioner, Aggregator
from dpark.context import DparkContext
from dpark.rdd import CoGroupedRDD

logger = logging.getLogger(__name__)

class Interval(object):
    def __init__(self, beginTime, endTime):
        self.begin = beginTime
        self.end = endTime
    @property    
    def duration(self):
        return self.end - self.begin
    def __add__(self, d):
        return Interval(self.begin + d, self.end + d)
    def __sub__(self, d):
        return Interval(self.begin - d, self.end - d)
    def __le__(self, that):
        assert self.duration == that.duration
        return self.begin < that.begin
    def __ge__(self, that):
        return not self < that
    def __str__(self):
        return '[%s, %s]' % (self.begin, self.end)
    @classmethod
    def current(cls, duration):
        now = int(time.time())
        ss = int(duration)
        begin = now / ss * ss
        return cls(begin, begin + duration)


class DStreamGraph(object):
    def __init__(self):
        self.inputStreams = []
        self.outputStreams = []
        self.zeroTime = None
        self.batchDuration = None
        self.rememberDuration = None
        self.checkpointInProgress = False

    def start(self, time):
        assert self.zeroTime is None
        time = int(time / self.batchDuration) * self.batchDuration
        self.zeroTime = time
        for out in self.outputStreams:
            out.initialize(time)
            out.remember(self.rememberDuration)
            out.validate()
        for ins in self.inputStreams:
            ins.start()

    def stop(self):
        for ins in self.inputStreams:
            ins.stop()

    def setContext(self, ssc):
        for out in self.outputStreams:
            out.setContext(ssc)

    def setBatchDuration(self, duration):
        assert self.batchDuration is None
        self.batchDuration = duration

    def remember(self, duration):
        self.rememberDuration = duration

    def addInputStream(self, input):
        input.setGraph(self)
        self.inputStreams.append(input)

    def addOutputStream(self, output):
        output.setGraph(self)
        self.outputStreams.append(output)

    def generateRDDs(self, time):
        #print 'generateRDDs', self, time
        return filter(None, [out.generateJob(time) for out in self.outputStreams])

    def forgetOldRDDs(self, time):
        for out in self.outputStreams:
            out.forgetOldRDDs(time)

    def updateCheckpointData(self, time):
        for out in self.outputStreams:
            out.updateCheckpointData(time)

    def restoreCheckpointData(self, time):
        for out in self.outputStreams:
            out.restoreCheckpointData(time)

    def validate(self):
        assert self.batchDuration is not None
        assert self.outputStreams, "No output streams registered"

    def __setstate__(self, v):
        self.__dict__.update(v)

    def __getstate__(self):
        return dict(self.__dict__)

class StreamingContext(object):
    def __init__(self, sc, batchDuration):
        if isinstance(sc, str):
            sc = DparkContext(sc)
        self.sc = sc
        self.batchDuration = batchDuration
        self.graph = DStreamGraph()
        self.graph.setBatchDuration(batchDuration)
        self.checkpointDir = None
        self.checkpointDuration = None
        self.networkInputTracker = None
        self.scheduler = None
        self.receiverJobThread = None

    def load(self, cp):
        if isinstance(cp, str):
            cp = Checkpoint.read(cp)
        self.cp = cp
        self.sc = DparkContext(cp.master)
        self.graph = cp.graph
        self.graph.setContext(self)
        self.graph.restoreCheckpointData()
        #self.sc.setCheckpointDir(cp.checkpointDir, True)
        self.checkpointDir = cp.checkpointDir
        self.checkpointDuration = cp.checkpointDuration

    def remember(self, duration):
        self.graph.remember(duration)

    def checkpoint(self, directory, interval):
        #if directory:
        #    self.sc.setCheckpointDir(directory)
        self.checkpointDir = directory
        self.checkpointDuration = interval

    def getInitialCheckpoint(self):
        return self.cp

    def registerInputStream(self, ds):
        return self.graph.addInputStream(ds)

    def registerOutputStream(self, ds):
        return self.graph.addOutputStream(ds)

    def networkTextStream(self, hostname, port):
        ds = SocketInputDStream(self, hostname, port)
        self.registerInputStream(ds)
        return ds

    def fileStream(self, directory, filter=None, newFilesOnly=True):
        ds = FileInputDStream(self, directory)
        self.registerInputStream(ds)
        return ds

    def textFileStream(self, directory, filter=None, newFilesOnly=True):
        return self.fileStream(directory, filter, newFilesOnly).map(lambda (k,v):v)

    def makeStream(self, rdd):
        return ConstantInputDStream(self, rdd)

    def queueStream(self, queue, oneAtTime=True, defaultRDD=None):
        ds = QueueInputDStream(self, queue, oneAtTime, defaultRDD)
        self.registerInputStream(ds)
        return ds

    def union(self, streams):
        return UnionDStream(streams)

    def validate(self):
        self.graph.validate()
        assert self.checkpointDir is not None or self.checkpointDuration is not None

    def start(self, t=None):
        if not self.checkpointDuration and self.graph:
            self.checkpointDuration = self.graph.batchDuration
        self.validate()
        
        # TODO
        #nis = [ds for ds in self.graph.inputStreams 
        #            if isinstance(ds, NetworkInputDStream)]
        #if nis:
        #    self.networkInputTracker = NetworkInputTracker(self, nis)
        #    self.networkInputTracker.start()

        self.scheduler = Scheduler(self)
        self.scheduler.start(t)

    def stop(self):
        if self.scheduler:
            self.scheduler.stop()
        if self.networkInputTracker:
            self.networkInputTracker.stop()
        if self.receiverJobThread:
            self.receiverJobThread.stop()
        self.sc.stop()
        logger.info("StreamingContext stopped successfully")

    def getSparkCheckpointDir(self, dir):
        return os.path.join(dir, str(random.randint(0, 1000)))

    @classmethod
    def createContext(cls, master, name=''):
        return DparkContext(master, name)

class Checkpoint(object):
    def __init__(self, ssc, time):
        self.ssc = ssc
        self.time = time
        self.master = ssc.sc.master
        self.framework = ssc.sc.jobName
        self.graph = ssc.graph
        self.checkpointDir = ssc.checkpointDir
        self.checkpointDuration = ssc.checkpointDuration

    def validate(self):
        assert self.ssc.sc.master

    def write(self, dir):
        pass

    @classmethod
    def read(cls, dir):
        pass

class Job(object):
    def __init__(self, time, func):
        self.time = time
        self.func = func

    def run(self):
        start = time.time()
        self.func()
        end = time.time()
        return end - start

    def __str__(self):
        return '<Job %s at %s>' % (self.func, self.time)

class JobManager(object):
    def __init__(self, numThreads):
        pass

    def runJob(self, job):
        used = job.run()
        delayed = time.time() - job.time
        logger.info("job used %s, delayed %s", used, delayed)

class RecurringTimer(object):
    def __init__(self, period, callback):
        self.period = int(period)
        self.callback = callback
        self.thread = None
        self.stopped = False
        
    def start(self, start=None):
        if start is None:
            start = time.time()
        self.nextTime = int(math.ceil(start / self.period) * self.period)
        self.stopped = False
        self.thread = spawn(self.run)

    def stop(self):
        self.stopped = True
        self.thread.join()

    def restart(self, start):
        self.stop()
        self.start(start)

    def run(self):
        while not self.stopped:
            now = time.time()
            if now >= self.nextTime:
                self.callback(self.nextTime)
                self.nextTime += self.period
            else:
                time.sleep(max(min(self.nextTime - now, 1), 0.1))

class Scheduler(object):
    def __init__(self, ssc):
        self.ssc = ssc
        self.graph = ssc.graph
        self.concurrentJobs = 1
        self.jobManager = JobManager(1)
        #self.checkpointWriter = CheckpointWriter(ssc.checkpointDir) if ssc.checkpointDir else None
        self.timer = RecurringTimer(ssc.graph.batchDuration, self.generateRDDs)

    def start(self, t=None):
        #if not self.ssc.isCheckpointPresent:
        firstTime = int(t or time.time())
        self.graph.start(firstTime - self.ssc.graph.batchDuration)
        self.timer.start(t)
        logger.info("Scheduler started")

    def stop(self):
        self.graph.stop()

    def generateRDDs(self, time):
        for job in self.graph.generateRDDs(time):
            self.jobManager.runJob(job)
        self.graph.forgetOldRDDs(time)
        self.doCheckpoint(time)

    def doCheckpoint(self, time):
        return
        if self.ssc.checkpointDuration and (time-self.graph.zeroTime):
            startTime = time.time()
            self.ssc.graph.updateCheckpointData()
            Checkpoint(self.ssc, time).write(self.ssc.checkpointDir)
            stopTime = time.time()
            logger.info("Checkpointing the graph took %.0f ms", (stopTime - startTime)*1000)


class DStream(object):
    """
 * A Discretized Stream (DStream), the basic abstraction in Spark Streaming, is a continuous
 * sequence of RDDs (of the same type) representing a continuous stream of data (see [[spark.RDD]]
 * for more details on RDDs). DStreams can either be created from live data (such as, data from
 * HDFS, Kafka or Flume) or it can be generated by transformation existing DStreams using operations
 * such as `map`, `window` and `reduceByKeyAndWindow`. While a Spark Streaming program is running, each
 * DStream periodically generates a RDD, either from live data or by transforming the RDD generated
 * by a parent DStream.
 *
 * This class contains the basic operations available on all DStreams, such as `map`, `filter` and
 * `window`. In addition, [[spark.streaming.PairDStreamFunctions]] contains operations available
 * only on DStreams of key-value pairs, such as `groupByKeyAndWindow` and `join`. These operations
 * are automatically available on any DStream of the right type (e.g., DStream[(Int, Int)] through
 * implicit conversions when `spark.streaming.StreamingContext._` is imported.
 *
 * DStreams internally is characterized by a few basic properties:
 *  - A list of other DStreams that the DStream depends on
 *  - A time interval at which the DStream generates an RDD
 *  - A function that is used to generate an RDD after each time interval
    """
    def __init__(self, ssc):
        self.ssc = ssc
        self.slideDuration = None
        self.dependencies = []

        self.generatedRDDs = {}
        self.zeroTime = None
        self.rememberDuration = None
        self.mustCheckpoint = False
        self.checkpointDuration = None
        self.checkpointData = []
        self.graph = None
#        self.isInitialized = False

    @property
    def parentRememberDuration(self):
        return self.rememberDuration

    def checkpoint(self, interval):
        self.checkpointDuration = interval

    def initialize(self, time):
#        if self.isInitialized:
#            return
#        self.isInitialized = True
#
        self.zeroTime = time
        if self.mustCheckpoint and not self.checkpointDuration:
            self.checkpointDuration = max(10, self.slideDuration)

        for dep in self.dependencies:
            dep.initialize(time)

    def validate(self):
        #assert 
        for dep in self.dependencies:
            dep.validate()

    def setContext(self, ssc):
        self.ssc = ssc
        for dep in self.dependencies:
            dep.setContext(ssc)

    def setGraph(self, g):
        self.graph = g
        for dep in self.dependencies:
            dep.setGraph(g)

    def remember(self, duration):
        if duration and duration > self.rememberDuration:
            self.rememberDuration = duration
        for dep in self.dependencies:
            dep.remember(self.parentRememberDuration)

    def isTimeValid(self, t):
        d = (t - self.zeroTime)
        dd = d / self.slideDuration * self.slideDuration
        return d == dd
        if dd != d:
            print 'invalid time', d, dd
            return False
        return True

    def compute(self, time):
        raise NotImplementedError

    def getOrCompute(self, time):
        old = self.generatedRDDs.get(time)
        if old: return old
        if self.isTimeValid(time):
            rdd = self.compute(time)
            if rdd:
                # do checkpoint TODO
                #print 'compute', self, time, rdd.collect()
                self.generatedRDDs[time] = rdd
                return rdd
        #else:
            #print 'invalid time', time, (time - self.zeroTime) / self.slideDuration * self.slideDuration

    def generateJob(self, time):
        rdd = self.getOrCompute(time)
        if rdd:
            return Job(time, lambda : self.ssc.sc.runJob(rdd, lambda x:{}))

    def forgetOldRDDs(self, time):
        oldest = time - (self.rememberDuration or 0)
        for k in self.generatedRDDs.keys():
            if k < oldest:
                self.generatedRDDs.pop(k)
        for dep in self.dependencies:
            dep.forgetOldRDDs(time)

    def updateCheckpointData(self, time):
        newRdds = [(t, rdd.getCheckpointFile) for t, rdd in self.generatedRDDs.items()
                    if rdd.getCheckpointFile]
        oldRdds = self.checkpointData
        if newRdds:
            self.checkpointData = dict(newRdds)
        for dep in self.dependencies:
            dep.updateCheckpointData(time)
        ns = dict(newRdds)
        for t, p in oldRdds:
            if t not in ns:
                os.unlink(p)
                logger.info("remove %s %s", t, p)
        logger.info("updated checkpoint data for time %s (%d)", time, len(newRdds))

    def restoreCheckpointData(self):
        for t, path in self.checkpointData:
            self.generatedRDDs[t] = self.ssc.sc.checkpointFile(path)
        for dep in self.dependencies:
            dep.restoreCheckpointData()
        logger.info("restoreCheckpointData")    

    def slice(self, beginTime, endTime):
        rdds = []
        t = endTime # - self.slideDuration
        while t > self.zeroTime and t > beginTime:
            rdd = self.getOrCompute(t)
            if rdd:
                rdds.append(rdd)
            t -= self.slideDuration
        return rdds

    def register(self):
        self.ssc.registerOutputStream(self)

    #  DStream Operations
    def union(self, that):
        return UnionDStream([self, that])

    def map(self, func):
        return MappedDStream(self, func)

    def flatMap(self, func):
        return FlatMappedDStream(self, func)

    def filter(self, func):
        return FilteredDStream(self, func)

    def glom(self):
        return GlommedDStream(self)

    def mapPartitions(self, func, preserve=False):
        return MapPartitionedDStream(self, func, preserve)

    def reduce(self, func):
        return self.map(lambda x:(None, x)).reduceByKey(func, 1).map(lambda (x,y):y)

    def count(self):
        return self.map(lambda x:1).reduce(lambda x,y:x+y)

    def foreach(self, func):
        out = ForEachDStream(self, func)
        self.ssc.registerOutputStream(out)
        return out

    def transform(self, func):
        return TransformedDStream(self, func)

    def show(self):
        def forFunc(rdd, t):
            some = rdd.take(11)
            print "-" * 80
            print "Time:", time.asctime(time.localtime(t))
            print "-" * 80
            for i in some[:10]:
                print i
            if len(some) > 10:
                print '...'
            print
        return self.foreach(forFunc)

    def window(self, duration, slideDuration=None):
        if slideDuration is None:
            slideDuration = self.slideDuration
        return WindowedDStream(self, duration, slideDuration)

    def tumble(self, batch):
        return self.window(batch, batch)

    def reduceByWindow(self, func, window, slideDuration=None, invFunc=None):
        if invFunc is not None:
            return self.map(lambda x:(1, x)).reduceByKeyAndWindow(func, invFunc, 
                window, slideDuration, 1).map(lambda (x,y): y)
        return self.window(window, slideDuration).reduce(func)

    def countByWindow(self, windowDuration, slideDuration=None):
        return self.map(lambda x:1).reduceByWindow(lambda x,y:x+y, 
            windowDuration, slideDuration, lambda x,y:x-y)

    def defaultPartitioner(self, part=None):
        if part is None:
            part = self.ssc.sc.defaultParallelism
        return HashPartitioner(part)

    def groupByKey(self, numPart=None):
        createCombiner = lambda x: [x]
        mergeValue = lambda l,x: l.append(x) or l
        mergeCombiner = lambda x,y: x.extend(y) or x
        if not isinstance(numPart, Partitioner):
            numPart = self.defaultPartitioner(numPart)
        return self.combineByKey(createCombiner, mergeValue, mergeCombiner, numPart)

    def reduceByKey(self, func, part=None):
        if not isinstance(part, Partitioner):
            part = self.defaultPartitioner(part)
        return self.combineByKey(lambda x:x, func, func, part)

    def combineByKey(self, createCombiner, mergeValue, mergeCombiner, partitioner):
        agg = Aggregator(createCombiner, mergeValue, mergeCombiner)
        return ShuffledDStream(self, agg, partitioner)

    def countByKey(self, numPartitions=None):
        return self.map(lambda (k,_):(k, 1)).reduceByKey(lambda x,y:x+y, numPartitions)

    def groupByKeyAndWindow(self, window, slideDuration=None, numPartitions=None):
        return self.window(window, slideDuration).groupByKey(numPartitions)

    def reduceByKeyAndWindow(self, func, invFunc, windowDuration, slideDuration=None, partitioner=None):
        if invFunc is None:
            return self.window(windowDuration, slideDuration).reduceByKey(func, partitioner)
        if slideDuration is None:
            slideDuration = self.slideDuration
        if not isinstance(partitioner, Partitioner):
            partitioner = self.defaultPartitioner(partitioner)
        return ReducedWindowedDStream(self, func, invFunc, windowDuration, slideDuration, partitioner)

    def countByKeyAndWindow(self, windowDuration, slideDuration=None, numPartitions=None):
        return self.map(lambda (k,_):(k, 1)).reduceByKeyAndWindow(
                lambda x,y:x+y, lambda x,y:x-y, windowDuration, slideDuration, numPartitions)

    def updateStateByKey(self, func, partitioner=None, remember=True):
        if not isinstance(partitioner, Partitioner):
            partitioner = self.defaultPartitioner(partitioner)
        def newF(it):
            for k, (vs, r) in it:
                nr = func(vs, r)
                if nr is not None:
                    yield (k, nr)
        return StateDStream(self, newF, partitioner, remember)

    def mapValues(self, func):
        return MapValuedDStream(self, func)

    def flatMapValues(self, func):
        return FlatMapValuedDStream(self, func)

    def cogroup(self, other, partitioner=None):
        if not isinstance(partitioner, Partitioner):
            partitioner = self.defaultPartitioner(partitioner)
        return CoGroupedDStream([self, other], partitioner)

    def join(self, other, partitioner=None):
        return self.cogroup(other, partitioner).flatMapValues(lambda (x,y): itertools.product(x,y))



class DerivedDStream(DStream):
    transformer = None
    def __init__(self, parent, func=None):
        DStream.__init__(self, parent.ssc)
        self.parent = parent
        self.func = func
        self.dependencies = [parent]
        self.slideDuration = parent.slideDuration

    def compute(self, t):
        rdd = self.parent.getOrCompute(t)
        if rdd:
            return getattr(rdd, self.transformer)(self.func)

class MappedDStream(DerivedDStream):
    transformer = 'map'

class FlatMappedDStream(DerivedDStream):
    transformer = 'flatMap'

class FilteredDStream(DerivedDStream):
    transformer = 'filter'

class MapValuedDStream(DerivedDStream):
    transformer = 'mapValue'

class FlatMapValuedDStream(DerivedDStream):
    transformer = 'flatMapValue'

class GlommedDStream(DerivedDStream):
    transformer = 'glom'
    def compute(self, t):
        rdd = self.parent.getOrCompute(t)
        if rdd:
            return rdd.glom() 

class MapPartitionedDStream(DerivedDStream):
    def __init__(self, parent, func, preserve=True):
        DerivedDStream.__init__(self, parent, func)
        self.preserve = preserve

    def compute(self, t):
        rdd = self.parent.getOrCompute(t)
        if rdd:
            return rdd.mapPartitions(self.func) # TODO preserve

class TransformedDStream(DerivedDStream):
    def compute(self, t):
        rdd = self.parent.getOrCompute(t)
        if rdd:
            return self.func(rdd, t)

class ForEachDStream(DerivedDStream):
    def compute(self, t):
        return self.parent.getOrCompute(t)

    def generateJob(self, time):
        rdd = self.getOrCompute(time)
        if rdd:
            return Job(time, lambda :self.func(rdd, time))

class StateDStream(DerivedDStream):
    def __init__(self, parent, updateFunc, partitioner, preservePartitioning=True):
        DerivedDStream.__init__(self, parent, updateFunc)
        self.partitioner = partitioner
        self.preservePartitioning = preservePartitioning
        self.mustCheckpoint = True
        self.rememberDuration = self.slideDuration # FIXME

    def compute(self, t):
        if t <= self.zeroTime:
            #print 'less', t, self.zeroTime
            return
        prevRDD = self.getOrCompute(t - self.slideDuration)
        parentRDD = self.parent.getOrCompute(t)
        updateFuncLocal = self.func
        if prevRDD:
            if parentRDD:
                cogroupedRDD = parentRDD.cogroup(prevRDD)
                return cogroupedRDD.mapValue(lambda (vs, rs):(vs, rs and rs[0] or None)).mapPartitions(updateFuncLocal) # preserve TODO
            else:
                return prevRDD
        else:
            if parentRDD:
                groupedRDD = parentRDD.groupByKey(self.partitioner)
                return groupedRDD.mapValue(lambda v:(v, None)).mapPartitions(updateFuncLocal)

class UnionDStream(DStream):
    def __init__(self, parents):
        DStream.__init__(self, parents[0].ssc)
        self.parents = parents
        self.dependencies = parents
        self.slideDuration = parents[0].slideDuration
    
    def compute(self, t):
        rdds = filter(None, [p.getOrCompute(t) for p in self.parents])
        if rdds:
            return self.ssc.sc.union(rdds)

class WindowedDStream(DStream):
    def __init__(self, parent, windowDuration, slideDuration):
        DStream.__init__(self, parent.ssc)
        self.parent = parent
        self.windowDuration = windowDuration
        self.slideDuration = slideDuration
        self.dependencies = [parent]

    @property        
    def parentRememberDuration(self):
        if self.rememberDuration:
            return self.rememberDuration + self.windowDuration

    def compute(self, t):
        currentWindow = Interval(t - self.windowDuration, t)
        rdds = self.parent.slice(currentWindow.begin, currentWindow.end)
        if rdds:
            return self.ssc.sc.union(rdds)


class CoGroupedDStream(DStream):
    def __init__(self, parents, partitioner):
        DStream.__init__(self, parents[0].ssc)
        self.parents = parents
        self.partitioner = partitioner
        assert len(set([p.slideDuration for p in parents])) == 1, "the slideDuration must be same"
        self.dependencies = parents
        self.slideDuration = parents[0].slideDuration

    def compute(self, t):
        rdds = filter(None, [p.getOrCompute(t) for p in self.parents])
        if rdds:
            return CoGroupedRDD(rdds, self.partitioner)


class ShuffledDStream(DerivedDStream):
    def __init__(self, parent, agg, partitioner):
        assert isinstance(parent, DStream)
        DerivedDStream.__init__(self, parent, agg)
        self.agg = agg
        self.partitioner = partitioner

    def compute(self, t):
        rdd = self.parent.getOrCompute(t)
        if rdd:
            return rdd.combineByKey(self.agg, self.partitioner)

class ReducedWindowedDStream(DerivedDStream):
    def __init__(self, parent, func, invReduceFunc, 
            windowDuration, slideDuration, partitioner):
        DerivedDStream.__init__(self, parent, func)
        self.invfunc = invReduceFunc
        self.windowDuration = windowDuration
        self.slideDuration = slideDuration
        self.partitioner = partitioner
        self.reducedStream = parent.reduceByKey(func, partitioner)
        self.dependencies = [self.reducedStream]
        self.mustCheckpoint = True

    @property    
    def parentRememberDuration(self):   
        if self.rememberDuration:
            return self.windowDuration + self.rememberDuration
        # persist

    def compute(self, t):
        if t <= self.zeroTime:
            return
        reduceF = self.func
        invReduceF = self.invfunc
        currWindow = Interval(t - self.windowDuration, t)
        prevWindow = currWindow - self.slideDuration

        oldRDDs = self.reducedStream.slice(prevWindow.begin, currWindow.begin)
        newRDDs = self.reducedStream.slice(prevWindow.end, currWindow.end)
        prevWindowRDD = self.getOrCompute(prevWindow.end) or self.ssc.sc.makeRDD([])
        allRDDs = [prevWindowRDD] + oldRDDs + newRDDs
        cogroupedRDD = CoGroupedRDD(allRDDs, self.partitioner)

        nOld = len(oldRDDs)
        nNew = len(newRDDs)
        def mergeValues(values):
            #print values, nOld, nNew
            assert len(values) == 1+nOld+nNew
            oldValues = [values[i][0] for i in range(1, nOld+1) if values[i]]
            newValues = [values[i][0] for i in range(1+nOld, nOld+1+nNew) if values[i]]
            if not values[0]:
                if newValues:
                    return reduce(reduceF, newValues)
            else:
                tmp = values[0][0]
                if oldValues:
                    tmp = invReduceF(tmp, reduce(reduceF, oldValues))
                if newValues:
                    tmp = reduceF(tmp, reduce(reduceF, newValues))
                return tmp
        return cogroupedRDD.mapValue(mergeValues)
                    

class InputDStream(DStream):
    def __init__(self, ssc):
        DStream.__init__(self, ssc)
        self.dependencies = []
        self.slideDuration = ssc.graph.batchDuration

    def start(self):
        pass

    def stop(self):
        pass

class ConstantInputDStream(InputDStream):
    def __init__(self, ssc, rdd):
        InputDStream.__init__(self, ssc)
        self.rdd = rdd
    def compute(self, validTime):
        return self.rdd

def defaultFilter(path):
    return not path.startswith('.')

# TODO file range
class ModTimeFilter(object):
    def __init__(self, lastModTime, filter):
        self.lastModTime = lastModTime
        self.filter = filter
        self.latestModTime = 0
        self.latestModFiles = set()

    def __call__(self, path):
        if not self.filter(path):
            return False
        return True

class FileInputDStream(InputDStream):
    def __init__(self, ssc, directory, filter=defaultFilter, newFilesOnly=True):
        InputDStream.__init__(self, ssc)
        self.directory = directory
        self.filter = filter
        self.newFilesOnly = newFilesOnly
        self.lastModTime = time.time() if self.newFilesOnly else 0
        self.lastTimeFiles = set()

    def compute(self, validTime):
        path_filter = ModTimeFilter(self.lastModTime, self.filter)
        files = [os.path.join(self.directory, n) for n in os.listdir(self.directory)
                    if path_filter(n)]
        if files:
            if path_filter.latestModTime > self.lastModTime:
                self.lastModTime = path_filter.latestModTime
                self.lastTimeFiles.clear()
            self.lastTimeFiles |= path_filter.latestModFiles
        return self.ssc.sc.union(files)

class QueueInputDStream(InputDStream):
    def __init__(self, ssc, queue, oneAtAtime=True, defaultRDD=None):
        InputDStream.__init__(self, ssc)
        self.queue = queue
        self.oneAtAtime = oneAtAtime
        self.defaultRDD = defaultRDD

    def compute(self, t):
        if self.queue:
            if self.oneAtAtime:
                return self.queue.pop(0)
            else:
                r = self.ssc.sc.union(self.queue)
                self.queue = []
                return r
        elif self.defaultRDD:
            return self.defaultRDD

class NetworkReceiverMessage: pass
class StopReceiver(NetworkReceiverMessage):
    def __init__(self, msg):
        self.msg = msg
class ReportBlock(NetworkReceiverMessage):
    def __init__(self, blockId, metadata):
        self.blockId = blockId
        self.metadata = metadata
class ReportError(NetworkReceiverMessage):
    def __init__(self, msg):
        self.msg = msg

class NetworkReceiver(object):
    "TODO"

class NetworkInputDStream(InputDStream):
    def __init__(self, ssc):
        InputDStream.__init__(self, ssc)
        self.id = ssc.getNewNetworkStreamId()
    def createReceiver(self):
        return NetworkReceiver()
    def compute(self, t):
        blockIds = self.ssc.networkInputTracker.getBlockIds(self.id, t)
        #return [BlockRDD(self.ssc.sc, blockIds)]


class SocketReceiver(NetworkReceiver):
    pass

class SocketInputDStream(NetworkInputDStream):
    pass


class RawNetworkReceiver:
    "TODO"

class RawInputDStream(NetworkInputDStream):
    def createReceiver(self):
        return RawNetworkReceiver()

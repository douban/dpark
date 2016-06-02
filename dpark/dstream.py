import os
import time
import socket
import shutil
import itertools
import threading
import random
from functools import reduce
from collections import deque
try:
    import cPickle as pickle
except ImportError:
    import pickle

from dpark.util import spawn, atomic_file, get_logger
from dpark.serialize import load_func, dump_func
from dpark.dependency import Partitioner, HashPartitioner, Aggregator
from dpark.context import DparkContext
from dpark.rdd import CoGroupedRDD, CheckpointRDD
from dpark.moosefs import open_file
from dpark.moosefs.utils import Error

logger = get_logger(__name__)


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

    def __init__(self, batchDuration):
        self.inputStreams = []
        self.outputStreams = []
        self.zeroTime = None
        self.batchDuration = batchDuration
        self.rememberDuration = None

    def start(self, time):
        self.zeroTime = int(time / self.batchDuration) * self.batchDuration
        for out in self.outputStreams:
            out.remember(self.rememberDuration)
        for ins in self.inputStreams:
            ins.start()

    def stop(self):
        for ins in self.inputStreams:
            ins.stop()

    def setContext(self, ssc):
        for ins in self.inputStreams:
            ins.setContext(ssc)

        for out in self.outputStreams:
            out.setContext(ssc)

    def remember(self, duration):
        self.rememberDuration = duration

    def addInputStream(self, input):
        input.setGraph(self)
        self.inputStreams.append(input)

    def addOutputStream(self, output):
        output.setGraph(self)
        self.outputStreams.append(output)

    def generateRDDs(self, time):
        return filter(None, [out.generateJob(time)
                             for out in self.outputStreams])

    def forgetOldRDDs(self, time):
        for out in self.outputStreams:
            out.forgetOldRDDs(time)

    def updateCheckpointData(self, time):
        for out in self.outputStreams:
            out.updateCheckpointData(time)

    def restoreCheckpointData(self):
        for out in self.outputStreams:
            out.restoreCheckpointData()


class StreamingContext(object):

    def __init__(self, batchDuration, sc=None, graph=None, batchCallback=None):
        if isinstance(sc, str) or not sc:  # None
            sc = DparkContext(sc)
        self.sc = sc
        batchDuration = int(batchDuration)
        self.batchDuration = batchDuration
        self.graph = graph or DStreamGraph(batchDuration)
        self.checkpointDir = None
        self.checkpointDuration = None
        self.scheduler = None
        self.lastCheckpointTime = 0
        self.batchCallback = batchCallback

    @classmethod
    def load(cls, path):
        cp = Checkpoint.read(path)
        graph = cp.graph
        ssc = cls(cp.batchDuration, cp.master, graph)
        ssc.checkpointDir = path
        ssc.checkpointDuration = cp.checkpointDuration
        graph.setContext(ssc)
        graph.restoreCheckpointData()
        return ssc, cp.time

    def remember(self, duration):
        self.graph.remember(duration)

    def checkpoint(self, directory, interval):
        self.checkpointDir = directory
        self.checkpointDuration = interval

    # def getInitialCheckpoint(self):
    #    return self.cp

    def registerInputStream(self, ds):
        return self.graph.addInputStream(ds)

    def registerOutputStream(self, ds):
        return self.graph.addOutputStream(ds)

    def networkTextStream(self, hostname, port):
        ds = SocketInputDStream(self, hostname, port)
        self.registerInputStream(ds)
        return ds

    def customStream(self, func):
        ds = NetworkInputDStream(self, func)
        self.registerInputStream(ds)
        return ds

    def fileStream(self, directory, filter=None, newFilesOnly=True, oldThreshold=3600):
        """
        `oldThreshold`: If a file is not modified in last `oldThreshold` seconds, it will be
        classified as an old file, and will be ommitted in future batches. The default value is
        3600, this may be **changed** in the future, we highly recommend user to set `oldThreshold`
        explicitly.
        """
        ds = FileInputDStream(self, directory, filter, newFilesOnly, oldThreshold)
        self.registerInputStream(ds)
        return ds

    def rotatingfiles(self, files):
        ds = RotatingFilesInputDStream(self, files)
        self.registerInputStream(ds)
        return ds

    def textFileStream(self, directory, filter=None, newFilesOnly=True, oldThreshold=3600):
        return self.fileStream(
            directory, filter, newFilesOnly, oldThreshold).map(lambda k_v: k_v[1])

    def makeStream(self, rdd):
        return ConstantInputDStream(self, rdd)

    def queueStream(self, queue, oneAtTime=True, defaultRDD=None):
        ds = QueueInputDStream(self, queue, oneAtTime, defaultRDD)
        self.registerInputStream(ds)
        return ds

    def union(self, streams):
        return UnionDStream(streams)

    def start(self, t=None):
        self.sc.start()
        nis = [ds for ds in self.graph.inputStreams
               if isinstance(ds, NetworkInputDStream)]
        for stream in nis:
            stream.startReceiver()

        self.scheduler = Scheduler(self)
        self.scheduler.start(t or time.time())

    def _runOnce(self):
        return self.scheduler.runOnce()

    def awaitTermination(self, timeout=None):
        if self.scheduler is None:
            raise RuntimeError('StreamimgContext not started')

        try:
            deadline = time.time() + timeout if timeout is not None else None
            while True:
                is_terminated = self._runOnce()
                if is_terminated or (
                        deadline is not None and time.time() > deadline):
                    break
                if self.batchCallback:
                    self.batchCallback()
        except KeyboardInterrupt:
            pass
        finally:
            self.sc.stop()
            logger.info("StreamingContext stopped successfully")

    def stop(self):
        if self.scheduler:
            self.scheduler.stop()

    def doCheckpoint(self, time):
        if self.checkpointDuration and time >= self.lastCheckpointTime + \
                self.checkpointDuration:
            self.lastCheckpointTime = time
            self.graph.updateCheckpointData(time)
            Checkpoint(self, time).write(self.checkpointDir)


class Checkpoint(object):

    def __init__(self, ssc, time):
        self.time = time
        self.master = ssc.sc.master
        self.graph = ssc.graph
        self.checkpointDuration = ssc.checkpointDuration
        self.batchDuration = ssc.batchDuration

    def write(self, path):
        output_file = os.path.join(path, 'metadata')
        with atomic_file(output_file) as f:
            f.write(pickle.dumps(self, -1))

    @classmethod
    def read(cls, path):
        filename = os.path.join(path, 'metadata')
        with open(filename) as f:
            return pickle.loads(f.read())


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
        logger.debug("start to run job %s", job)
        used = job.run()
        delayed = time.time() - job.time
        logger.info("job used %s, delayed %s", used, delayed)


class RecurringTimer(object):

    def __init__(self, period, callback):
        self.period = period
        self.callback = callback
        self.thread = None
        self.stopped = False

    def start(self, start):
        self.nextTime = (int(start / self.period) + 1) * self.period
        self.stopped = False
        self.thread = spawn(self.run)
        logger.debug("RecurringTimer started, nextTime is %d", self.nextTime)

    def stop(self):
        self.stopped = True
        if self.thread:
            self.thread.join()

    def run(self):
        while not self.stopped:
            now = time.time()
            if now >= self.nextTime:
                logger.debug("start call %s with %d (delayed %f)", self.callback,
                             self.nextTime, now - self.nextTime)
                self.callback(self.nextTime)
                self.nextTime += self.period
            else:
                time.sleep(max(min(self.nextTime - now, 1), 0.01))

_STOP, _EVENT = range(2)


class Scheduler(object):

    def __init__(self, ssc):
        self.ssc = ssc
        self.graph = ssc.graph
        self.jobManager = JobManager(1)
        self.timer = RecurringTimer(ssc.batchDuration, self.generateEvent)
        self._queue = deque()

    def start(self, t):
        self.graph.start(t)
        self.timer.start(t)
        logger.info("Scheduler started")

    def stop(self):
        self.timer.stop()
        self.graph.stop()
        self._queue.append((_STOP, None))

    def generateEvent(self, time):
        self._queue.append((_EVENT, time))

    def runOnce(self):
        try:
            t, v = self._queue.popleft()
        except IndexError:
            time.sleep(0.1)
            return False

        if t == _STOP:
            return True

        self.generateRDDs(v)
        return False

    def generateRDDs(self, time):
        for job in self.graph.generateRDDs(time):
            logger.debug("start to run job %s", job)
            self.jobManager.runJob(job)
        self.graph.forgetOldRDDs(time)
        self.ssc.doCheckpoint(time)


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
        self.rememberDuration = None
        self.mustCheckpoint = False
        self.last_checkpoint_time = 0
        self.checkpointData = {}
        self.graph = None

    def __getstate__(self):
        d = dict(self.__dict__)
        d.pop('generatedRDDs')
        d.pop('ssc')
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.generatedRDDs = {}

    def setContext(self, context):
        self.ssc = context
        for dep in self.dependencies:
            dep.setContext(context)

    @property
    def zeroTime(self):
        return self.graph.zeroTime

    @property
    def parentRememberDuration(self):
        return self.rememberDuration

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
        return abs(d - dd) < 1e-3

    def compute(self, time):
        raise NotImplementedError

    def getOrCompute(self, time):
        if time in self.generatedRDDs:
            return self.generatedRDDs[time]
        if self.isTimeValid(time):
            rdd = self.compute(time)
            self.generatedRDDs[time] = rdd
            if (self.ssc.checkpointDuration and
                    time >= self.last_checkpoint_time + self.ssc.checkpointDuration):
                self.last_checkpoint_time = time
                if rdd:
                    rdd.checkpoint(self.ssc.checkpointDir)
            return rdd

    def generateJob(self, time):
        rdd = self.getOrCompute(time)
        if rdd:
            return Job(time, lambda: self.ssc.sc.runJob(rdd, lambda x: {}))

    def forgetOldRDDs(self, time):
        oldest = time - (self.rememberDuration or 0)
        for k in self.generatedRDDs.keys():
            if k < oldest:
                self.generatedRDDs.pop(k)
        for dep in self.dependencies:
            dep.forgetOldRDDs(time)

    def updateCheckpointData(self, time):
        newRdds = [(t, rdd.checkpoint_path) for t, rdd in self.generatedRDDs.items()
                   if rdd and rdd.checkpoint_path]

        if newRdds:
            oldRdds = self.checkpointData
            self.checkpointData = dict(newRdds)
            for t, p in oldRdds.iteritems():
                if t not in self.checkpointData:
                    try:
                        shutil.rmtree(p)
                    except OSError:
                        pass

        for dep in self.dependencies:
            dep.updateCheckpointData(time)

        logger.info(
            "updated checkpoint data for time %s (%d)",
            time,
            len(newRdds))

    def restoreCheckpointData(self):
        for t, path in self.checkpointData.iteritems():
            self.generatedRDDs[t] = CheckpointRDD(self.ssc.sc, path)
        for dep in self.dependencies:
            dep.restoreCheckpointData()
        logger.info("restoreCheckpointData")

    def slice(self, beginTime, endTime):
        rdds = []
        t = endTime  # - self.slideDuration
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
        return self.map(lambda x: (None, x)).reduceByKey(
            func, 1).map(lambda x_y1: x_y1[1])

    def count(self):
        return self.map(lambda x: 1).reduce(lambda x, y: x + y)

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
            return self.map(lambda x: (1, x)).reduceByKeyAndWindow(
                func, invFunc, window, slideDuration, 1).map(lambda x_y: x_y[1])
        return self.window(window, slideDuration).reduce(func)

    def countByWindow(self, windowDuration, slideDuration=None):
        return self.map(lambda x: 1).reduceByWindow(
            lambda x, y: x + y, windowDuration, slideDuration, lambda x, y: x - y)

    def defaultPartitioner(self, part=None):
        if part is None:
            part = self.ssc.sc.defaultParallelism
        return HashPartitioner(part)

    def groupByKey(self, numPart=None):
        createCombiner = lambda x: [x]
        mergeValue = lambda l, x: l.append(x) or l
        mergeCombiner = lambda x, y: x.extend(y) or x
        if not isinstance(numPart, Partitioner):
            numPart = self.defaultPartitioner(numPart)
        return self.combineByKey(
            createCombiner, mergeValue, mergeCombiner, numPart)

    def reduceByKey(self, func, part=None):
        if not isinstance(part, Partitioner):
            part = self.defaultPartitioner(part)
        return self.combineByKey(lambda x: x, func, func, part)

    def combineByKey(self, createCombiner, mergeValue,
                     mergeCombiner, partitioner):
        agg = Aggregator(createCombiner, mergeValue, mergeCombiner)
        return ShuffledDStream(self, agg, partitioner)

    def countByKey(self, numPartitions=None):
        return self.map(lambda k__: (k__[0], 1)).reduceByKey(
            lambda x, y: x + y, numPartitions)

    def groupByKeyAndWindow(
            self, window, slideDuration=None, numPartitions=None):
        return self.window(window, slideDuration).groupByKey(numPartitions)

    def reduceByKeyAndWindow(
            self, func, invFunc, windowDuration, slideDuration=None, partitioner=None):
        if invFunc is None:
            return self.window(windowDuration, slideDuration).reduceByKey(
                func, partitioner)
        if slideDuration is None:
            slideDuration = self.slideDuration
        if not isinstance(partitioner, Partitioner):
            partitioner = self.defaultPartitioner(partitioner)
        return ReducedWindowedDStream(
            self, func, invFunc, windowDuration, slideDuration, partitioner)

    def countByKeyAndWindow(self, windowDuration,
                            slideDuration=None, numPartitions=None):
        return self.map(lambda k__2: (k__2[0], 1)).reduceByKeyAndWindow(
            lambda x, y: x + y, lambda x, y: x - y, windowDuration, slideDuration, numPartitions)

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
        return self.cogroup(other, partitioner).flatMapValues(
            lambda x_y3: itertools.product(x_y3[0], x_y3[1]))


class DerivedDStream(DStream):
    transformer = None

    def __init__(self, parent, func=None):
        DStream.__init__(self, parent.ssc)
        self.parent = parent
        self.func = func
        self.dependencies = [parent]
        self.slideDuration = parent.slideDuration

    def __getstate__(self):
        d = DStream.__getstate__(self)
        del d['func']
        d['_func'] = dump_func(self.func)
        return d

    def __setstate__(self, state):
        self.func = load_func(state.pop('_func'))
        DStream.__setstate__(self, state)

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
            return rdd.mapPartitions(self.func)  # TODO preserve


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
            return Job(time, lambda: self.func(rdd, time))


class StateDStream(DerivedDStream):

    def __init__(self, parent, updateFunc, partitioner,
                 preservePartitioning=True):
        DerivedDStream.__init__(self, parent, updateFunc)
        self.partitioner = partitioner
        self.preservePartitioning = preservePartitioning
        self.mustCheckpoint = True
        self.rememberDuration = self.slideDuration  # FIXME

    def compute(self, t):
        if t <= self.zeroTime:
            # print 'less', t, self.zeroTime
            return
        prevRDD = self.getOrCompute(t - self.slideDuration)
        parentRDD = self.parent.getOrCompute(t)
        updateFuncLocal = self.func
        if prevRDD:
            if parentRDD:
                cogroupedRDD = parentRDD.cogroup(prevRDD)
                return cogroupedRDD.mapValue(
                    lambda vs_rs: (vs_rs[0], vs_rs[1] and vs_rs[1][0] or None)) \
                    .mapPartitions(updateFuncLocal)  # preserve TODO
            else:
                return prevRDD.mapValue(
                    lambda rs: ([], rs)).mapPartitions(updateFuncLocal)
        else:
            if parentRDD:
                groupedRDD = parentRDD.groupByKey(self.partitioner)
                return groupedRDD.mapValue(
                    lambda v: (v, None)).mapPartitions(updateFuncLocal)


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
        assert len(set([p.slideDuration for p in parents])) == 1, \
            "the slideDuration must be same"
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
        prevWindowRDD = (self.getOrCompute(prevWindow.end)
                         or self.ssc.sc.makeRDD([]))
        allRDDs = [prevWindowRDD] + oldRDDs + newRDDs
        cogroupedRDD = CoGroupedRDD(allRDDs, self.partitioner)

        nOld = len(oldRDDs)
        nNew = len(newRDDs)

        def mergeValues(values):
            # print values, nOld, nNew
            assert len(values) == 1 + nOld + nNew
            oldValues = [values[i][0] for i in range(1, nOld + 1) if values[i]]
            newValues = [values[i][0]
                         for i in range(1 + nOld, nOld + 1 + nNew) if values[i]]
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
        self.slideDuration = ssc.batchDuration

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
    if '/.' in path:
        return False
    return True


class ModTimeAndRangeFilter(object):

    def __init__(self, lastModTime, filter, oldThreshold):
        self.lastModTime = lastModTime
        self.latestModTime = 0
        self.filter = filter
        self.oldThreshold = oldThreshold
        self.accessedFiles = {}
        self.oldFiles = set()

    def __call__(self, path):
        if not self.filter(path):
            return

        if path in self.oldFiles:
            return

        if os.path.islink(path):
            self.oldFiles.add(path)
            return

        try:
            mtime = os.path.getmtime(path)
        except Exception as e:
            logger.warning(str(e))
            return
        if mtime < self.lastModTime:
            if mtime < time.time() - self.oldThreshold:
                self.oldFiles.add(path)
            return
        if mtime > self.latestModTime:
            self.latestModTime = mtime

        nsize = os.path.getsize(path)
        osize = self.accessedFiles.get(path, 0)
        if nsize <= osize:
            return
        self.accessedFiles[path] = nsize
        logger.info("got new file %s [%d,%d]", path, osize, nsize)
        return path, osize, nsize

    def rotate(self):
        self.lastModTime = self.latestModTime


class FileInputDStream(InputDStream):

    def __init__(self, ssc, directory, filter=None, newFilesOnly=True, oldThreshold=3600):
        InputDStream.__init__(self, ssc)
        assert os.path.exists(directory), \
            'directory %s must exists' % directory
        assert os.path.isdir(directory), '%s is not directory' % directory
        self.directory = directory
        lastModTime = time.time() - oldThreshold if newFilesOnly else 0
        filter = filter or defaultFilter
        self.filter = ModTimeAndRangeFilter(lastModTime, filter, oldThreshold)
        self.newFilesOnly = newFilesOnly

    def compute(self, validTime):
        files = []
        for root, dirs, names in os.walk(self.directory):
            for name in names:
                r = self.filter(os.path.join(root, name))
                if r:
                    files.append(r)
        if files:
            self.filter.rotate()
            return self.ssc.sc.union([self.ssc.sc.partialTextFile(path, begin, end)
                                      for path, begin, end in files])

    def start(self):
        if self.newFilesOnly:
            # Init the new files old sizes.
            for root, dirs, names in os.walk(self.directory):
                for name in names:
                    self.filter(os.path.join(root, name))


class RotatingFilesInputDStream(InputDStream):
    def __init__(self, ssc, files):
        InputDStream.__init__(self, ssc)
        self.files = files
        self._state = {}

    def start(self):
        self._state = dict(self._get_state())

    def _get_state(self):
        for fn in self.files:
            try:
                realname = os.path.realpath(fn)
                f = open_file(realname)
                if f:
                    yield realname, (f.info.inode, f.info.length, f.info.mtime)
                else:
                    st = os.stat(realname)
                    yield realname, (st.st_ino, st.st_size, st.st_mtime)

            except (OSError, Error):
                pass

    def compute(self, validTime):
        state = {}
        offsets = {}

        for fn, (inode, size, mtime) in self._get_state():
            if fn not in self._state:
                offsets[fn] = (0, size)
            else:
                _inode, _size, _mtime = self._state[fn]
                if inode != _inode or (mtime > _mtime and size < _size):
                    offsets[fn] = (0, size)
                elif mtime >= _mtime and size > _size:
                    offsets[fn] = (_size, size)

            state[fn] = (inode, size, mtime)

        for fn, (_inode, _size, _mtime) in self._state.iteritems():
            if fn not in state:
                try:
                    st = os.stat(fn)
                    inode, size, mtime = st.st_ino, st.st_size, st.st_mtime
                except OSError:
                    continue

                if inode != _inode or (mtime > _mtime and size < _size):
                    offsets[fn] = (0, size)
                elif mtime >= _mtime and size > _size:
                    offsets[fn] = (_size, size)

        self._state = state
        return self.ssc.sc.union([self.ssc.sc.partialTextFile(path, begin, end)
                                  for path, (begin, end) in offsets.iteritems()])



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


class NetworkInputDStream(InputDStream):

    def __init__(self, ssc, func):
        InputDStream.__init__(self, ssc)
        self.func = func
        self._lock = threading.RLock()
        self._messages = []

    def __getstate__(self):
        d = InputDStream.__getstate__(self)
        del d['func']
        del d['_lock']
        d['_func'] = dump_func(self.func)
        return d

    def __setstate__(self, state):
        self.func = load_func(state.pop('_func'))
        self._lock = threading.RLock()
        InputDStream.__setstate__(self, state)

    def startReceiver(self):
        def _run():
            while True:
                generator = self.func()
                try:
                    for message in generator:
                        if not self.ssc.sc.started:
                            return
                        with self._lock:
                            self._messages.append(message)
                except:
                    logger.exception('fail to receive')

        spawn(_run)

    def compute(self, t):
        with self._lock:
            if self._messages:
                rdd = self.ssc.sc.makeRDD(self._messages)
                self._messages = []
                return rdd


class SocketInputDStream(NetworkInputDStream):

    def __init__(self, ssc, hostname, port):
        NetworkInputDStream.__init__(self, ssc, self._receive)
        self.hostname = hostname
        self.port = port

    def __getstate__(self):
        d = InputDStream.__getstate__(self)
        del d['func']
        del d['_lock']
        return d

    def __setstate__(self, state):
        self.func = self._receive
        self._lock = threading.RLock()
        InputDStream.__setstate__(self, state)

    def _receive(self):
        client, f = None, None
        try:
            client = socket.socket()
            client.connect((self.hostname, self.port))
            f = client.makefile()
            for line in f:
                yield line
        except:
            time.sleep(0.5 + random.random())
            raise
        finally:
            if f:
                f.close()
            if client:
                client.close()

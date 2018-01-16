from __future__ import absolute_import
import marshal
import time
import six.moves.cPickle
import struct

from dpark.util import compress, atomic_file, get_logger
from dpark.serialize import marshalable, load_func, dump_func, dumps, loads
from dpark.shuffle import LocalFileShuffle, AutoBatchedSerializer, GroupByAutoBatchedSerializer
from six.moves import range

logger = get_logger(__name__)

class Task:
    def __init__(self):
        self.id = Task.newId()

    nextId = 0
    @classmethod
    def newId(cls):
        cls.nextId += 1
        return cls.nextId

    def run(self, id):
        raise NotImplementedError

    def preferredLocations(self):
        raise NotImplementedError


class DAGTask(Task):
    def __init__(self, stageId):
        Task.__init__(self)
        self.stageId = stageId

    def __repr__(self):
        return '<task %d:%d>'%(self.stageId, self.id)


class ResultTask(DAGTask):
    def __init__(self, stageId, rdd, func, partition, locs, outputId):
        DAGTask.__init__(self, stageId)
        self.rdd = rdd
        self.func = func
        self.partition = partition
        self.split = rdd.splits[partition]
        self.locs = locs
        self.outputId = outputId

    def run(self, attemptId):
        logger.debug("run task %s with %d", self, attemptId)
        return self.func(self.rdd.iterator(self.split))

    def preferredLocations(self):
        return self.locs

    def __repr__(self):
        partition = getattr(self, 'partition', None)
        rdd = getattr(self, 'rdd', None)
        return "<ResultTask(%s) of %s" % (partition, rdd)

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['func']
        del d['rdd']
        del d['split']
        return d, dumps(self.rdd), dump_func(self.func), dumps(self.split)

    def __setstate__(self, state):
        d, rdd, func, split = state
        self.__dict__.update(d)
        self.rdd = loads(rdd)
        self.func = load_func(func)
        self.split = loads(split)


class ShuffleMapTask(DAGTask):
    def __init__(self, stageId, rdd, dep, partition, locs):
        DAGTask.__init__(self, stageId)
        self.rdd = rdd
        self.shuffleId = dep.shuffleId
        self.aggregator = dep.aggregator
        self.partitioner = dep.partitioner
        self.sort_shuffle = dep.sort_shuffle
        self.iter_values = dep.iter_values
        self.partition = partition
        self.split = rdd.splits[partition]
        self.locs = locs

    def __repr__(self):
        shuffleId = getattr(self, 'shuffleId', None)
        partition = getattr(self, 'partition', None)
        rdd = getattr(self, 'rdd', None)
        return '<ShuffleTask(%s, %s) of %s>' % (shuffleId, partition, rdd)

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['rdd']
        del d['split']
        return d, dumps(self.rdd), dumps(self.split)

    def __setstate__(self, state):
        d, rdd, split = state
        self.__dict__.update(d)
        self.rdd = loads(rdd)
        self.split = loads(split)

    def preferredLocations(self):
        return self.locs

    def _prepare_shuffle(self, rdd):
        split = self.split
        numOutputSplits = self.partitioner.numPartitions
        getPartition = self.partitioner.getPartition
        mergeValue = self.aggregator.mergeValue
        createCombiner = self.aggregator.createCombiner

        buckets = [{} for i in range(numOutputSplits)]
        for item in rdd.iterator(split):
            try:
                k, v = item
                bucketId = getPartition(k)
                bucket = buckets[bucketId]
                r = bucket.get(k, None)
                if r is not None:
                    bucket[k] = mergeValue(r, v)
                else:
                    bucket[k] = createCombiner(v)
            except ValueError as e:
                logger.exception('The ValueError exception: %s at %s', str(e), str(rdd.scope.call_site))
                raise

        return enumerate(buckets)

    def run_without_sorted(self, it):
        for i, bucket in it:
            try:
                if marshalable(bucket):
                    flag, d = b'm', marshal.dumps(bucket)
                else:
                    flag, d = b'p', six.moves.cPickle.dumps(bucket, -1)
            except ValueError:
                flag, d = b'p', six.moves.cPickle.dumps(bucket, -1)
            cd = compress(d)
            for tried in range(1, 4):
                try:
                    path = LocalFileShuffle.getOutputFile(self.shuffleId, self.partition, i, len(cd) * tried)
                    with atomic_file(path, bufsize=1024*4096) as f:
                        f.write(flag + struct.pack("I", 5 + len(cd)))
                        f.write(cd)

                    break
                except IOError as e:
                    logger.warning("write %s failed: %s, try again (%d)", path, e, tried)
            else:
                raise e

        return LocalFileShuffle.getServerUri()

    def run_with_sorted(self, it):
        serializer = GroupByAutoBatchedSerializer() if self.iter_values else AutoBatchedSerializer()
        for i, bucket in it:
            for tried in range(1, 4):
                try:
                    path = LocalFileShuffle.getOutputFile(self.shuffleId, self.partition, i)
                    with atomic_file(path, bufsize=1024*4096) as f:
                        items = sorted(bucket.items(), key=lambda x: x[0])
                        serializer.dump_stream(items, f)
                    break
                except IOError as e:
                    logger.warning("write %s failed: %s, try again (%d)", path, e, tried)
            else:
                raise e
        return LocalFileShuffle.getServerUri()

    def run(self, attempId):
        logger.debug("begin suffleMapTask %d of %s, ", self.partition, self.rdd)
        t0 = time.time()
        it = self._prepare_shuffle(self.rdd)
        t1 = time.time()
        if not self.sort_shuffle:
            url = self.run_without_sorted(it)
        else:
            url = self.run_with_sorted(it)
        t2 = time.time()
        logger.debug("end shuffleMapTask %d of %s, calc/dump time: %02f/%02f sec ",
                     self.partition, self.rdd, t1 - t0, t2 - t1)
        return url


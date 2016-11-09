import marshal
import cPickle
import struct

from dpark.util import compress, atomic_file, get_logger
from dpark.serialize import marshalable, load_func, dump_func, dumps, loads
from dpark.shuffle import LocalFileShuffle

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
        for k,v in rdd.iterator(split):
            bucketId = getPartition(k)
            bucket = buckets[bucketId]
            r = bucket.get(k, None)
            if r is not None:
                bucket[k] = mergeValue(r, v)
            else:
                bucket[k] = createCombiner(v)

        return enumerate(buckets)

    def run(self, attempId):
        logger.debug("shuffling %d of %s", self.partition, self.rdd)
        for i, bucket in self._prepare_shuffle(self.rdd):
            try:
                if marshalable(bucket):
                    flag, d = 'm', marshal.dumps(bucket)
                else:
                    flag, d = 'p', cPickle.dumps(bucket, -1)
            except ValueError:
                flag, d = 'p', cPickle.dumps(bucket, -1)
            cd = compress(d)
            for tried in range(1, 4):
                try:
                    path = LocalFileShuffle.getOutputFile(self.shuffleId, self.partition, i, len(cd) * tried)
                    with atomic_file(path, bufsize=1024*4096) as f:
                        f.write(flag + struct.pack("I", 5 + len(cd)))
                        f.write(cd)

                    break
                except IOError, e:
                    logger.warning("write %s failed: %s, try again (%d)", path, e, tried)
            else:
                raise

        return LocalFileShuffle.getServerUri()

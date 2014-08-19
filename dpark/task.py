import os,os.path
import socket
import marshal
import cPickle
import logging
import struct

from dpark.util import compress, decompress
from dpark.serialize import marshalable, load_func, dump_func
from dpark.shuffle import LocalFileShuffle

logger = logging.getLogger(__name__)

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
        return "<ResultTask(%d) of %s" % (self.partition, self.rdd)

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['func']
        return d, dump_func(self.func)

    def __setstate__(self, state):
        self.__dict__, code = state
        self.func = load_func(code)


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
        return '<ShuffleTask(%d, %d) of %s>' % (self.shuffleId, self.partition, self.rdd)

    def preferredLocations(self):
        return self.locs

    def run(self, attempId):
        logger.debug("shuffling %d of %s", self.partition, self.rdd)
        numOutputSplits = self.partitioner.numPartitions
        getPartition = self.partitioner.getPartition
        mergeValue = self.aggregator.mergeValue
        createCombiner = self.aggregator.createCombiner

        buckets = [{} for i in range(numOutputSplits)]
        for k,v in self.rdd.iterator(self.split):
            bucketId = getPartition(k)
            bucket = buckets[bucketId]
            r = bucket.get(k, None)
            if r is not None:
                bucket[k] = mergeValue(r, v)
            else:
                bucket[k] = createCombiner(v)

        for i in range(numOutputSplits):
            try:
                if marshalable(buckets[i]):
                    flag, d = 'm', marshal.dumps(buckets[i])
                else:
                    flag, d = 'p', cPickle.dumps(buckets[i], -1)
            except ValueError:
                flag, d = 'p', cPickle.dumps(buckets[i], -1)
            cd = compress(d)
            for tried in range(1, 4):
                try:
                    path = LocalFileShuffle.getOutputFile(self.shuffleId, self.partition, i, len(cd) * tried)
                    tpath = path + ".%s.%s" % (socket.gethostname(), os.getpid())
                    f = open(tpath, 'wb', 1024*4096)
                    f.write(flag + struct.pack("I", 5 + len(cd)))
                    f.write(cd)
                    f.close()
                    os.rename(tpath, path)
                    break
                except IOError, e:
                    logger.warning("write %s failed: %s, try again (%d)", path, e, tried)
                    try: os.remove(tpath)
                    except OSError: pass
            else:
                raise

        return LocalFileShuffle.getServerUri()

import marshal
import cPickle
import logging
import struct

from utils import load_func, dump_func
from shuffle import LocalFileShuffle

#class TaskContext:
#    def __init__(self, stageId, splitId, attemptId):
#        self.stageId = stageId
#        self.splitId = splitId
#        self.attemptId = attemptId
#
#class TaskResult:
#    def __init__(self, value, accumUpdates):
#        self.value = value
#        self.accumUpdates = accumUpdates

class Task:
    nextId = 0
    def __init__(self):
        self.id = self.newId()

    @classmethod
    def newId(cls):
        cls.nextId += 1
        return cls.nextId

    def run(self, id):
        raise NotImplementedError
    def preferredLocations(self):
        return []
    def generation(self):
        raise NotImplementedError
    
class DAGTask(Task):
    def __init__(self, stageId):
        self.id = self.newId()
        self.stageId = stageId
        #self.gen = env.mapOutputTracker.getGeneration

    def __str__(self):
        return '<task %d:%d>'%(self.stageId, self.id)

#    def generation(self):
#        return self.gen

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
        logging.debug("run task %s with %d", self, attemptId)
        return self.func(self.rdd.iterator(self.split))

    def preferredLocations(self):
        return self.locs

    def __str__(self):
        return "<ResultTask(%d, %d) of %s" % (self.stageId, self.partition, self.rdd)
    
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
        self.stageId = stageId
        self.rdd = rdd
        self.shuffleId = dep.shuffleId
        self.aggregator = dep.aggregator
        self.partitioner = dep.partitioner
        self.partition = partition
        self.split = rdd.splits[partition]
        self.locs = locs

    def __str__(self):
        return '<shuffletask(%d,%d) of %s>' % (self.stageId, self.partition, self.rdd)

    def run(self, attempId):
        logging.debug("shuffling %d of %s", self.partition, self.rdd)
        aggregator= self.aggregator
        partitioner = self.partitioner
        numOutputSplits = partitioner.numPartitions
        buckets = [{} for i in range(numOutputSplits)]
        getPartition = partitioner.getPartition
        mergeValue = aggregator.mergeValue
        createCombiner = aggregator.createCombiner
        for k,v in self.rdd.iterator(self.split):
            bucketId = getPartition(k)
            bucket = buckets[bucketId]
            if k in bucket:
                bucket[k] = mergeValue(bucket[k], v)
            else:
                bucket[k] = createCombiner(v)
        for i in range(numOutputSplits):
            path = LocalFileShuffle.getOutputFile(self.shuffleId, self.partition, i)
            f = open(path, 'w', 1024*4096)
            #for v in buckets[i].iteritems():
            #    v = marshal.dumps(v)
            #    f.write(struct.pack('h', len(v)))
            #    f.write(v)
            #marshal.dump(buckets[i], f)
            cPickle.dump(buckets[i].items(), f, -1)
            f.close()
        return LocalFileShuffle.getServerUri()

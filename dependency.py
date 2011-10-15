
class Dependency:
    def __init__(self, rdd):
        self.rdd = rdd

class NarrowDependency(Dependency):
    isShuffle = False
    def getParents(self, outputPartition):
        raise NotImplementedError

class OneToOneDependency(NarrowDependency):
    def getParents(self, pid):
        return [pid]

class CartesionDependency(NarrowDependency):
    def __init__(self, rdd, first, numSplitsInRdd2):
        NarrowDependency.__init__(self, rdd)
        self.first = first
        self.numSplitsInRdd2 = numSplitsInRdd2

    def getParents(self, pid):
        if self.first:
            return [pid / self.numSplitsInRdd2]
        else:
            return [pid % self.numSplitsInRdd2]

class RangeDependency(NarrowDependency):
    def __init__(self, rdd, inStart, outStart, length):
        Dependency.__init__(self, rdd)
        self.inStart = inStart
        self.outStart = outStart
        self.length = length

    def getParents(self, pid):
        if pid >= self.outStart and pid < self.outStart + self.length:
            return [pid - self.outStart + self.inStart]
        return []


class Aggregator:
    def createCombiner(self, v):
        pass
    def mergeValue(self, c, v):
        pass
    def mergeCombiners(self, c, v):
        pass

class Partitioner:
    @property
    def numPartitions(self):
        pass
    def getPartition(self, key):
        pass

class HashPartitioner(Partitioner):
    def __init__(self, partitions):
        self.partitions = partitions
        
    @property
    def numPartitions(self):
        return self.partitions

    def getPartition(self, key):
        return hash(key) % self.partitions

    def __equal__(self, other):
        return other.numPartitions == self.numPartitions

class ShuffleDependency(Dependency):
    isShuffle = True
    def __init__(self, shuffleId, rdd, aggregator, partitioner):
        Dependency.__init__(self, rdd)
        self.shuffleId = shuffleId
        self.aggregator = aggregator
        self.partitioner = partitioner


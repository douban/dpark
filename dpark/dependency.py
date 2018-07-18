from __future__ import absolute_import
import bisect

from dpark.utils import portable_hash
from dpark.serialize import load_func, dump_func
from dpark.utils.heaponkey import HeapOnKey
from six.moves import range


class Dependency:
    def __init__(self, rdd):
        self.rdd = rdd

    def __getstate__(self):
        raise ValueError("Should not pickle dependency: %r" % self)


class NarrowDependency(Dependency):
    isShuffle = False

    def getParents(self, outputPartition):
        raise NotImplementedError


class OneToOneDependency(NarrowDependency):
    def getParents(self, pid):
        return [pid]


class OneToRangeDependency(NarrowDependency):
    def __init__(self, rdd, splitSize, length):
        Dependency.__init__(self, rdd)
        self.splitSize = splitSize
        self.length = length

    def getParents(self, pid):
        return list(range(pid * self.splitSize,
                          min((pid + 1) * self.splitSize, self.length)))


class CartesianDependency(NarrowDependency):
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
        if self.outStart <= pid < self.outStart + self.length:
            return [pid - self.outStart + self.inStart]
        return []


class ShuffleDependency(Dependency):
    isShuffle = True

    def __init__(self, shuffleId, rdd, aggregator, partitioner, rddconf):
        Dependency.__init__(self, rdd)
        self.shuffleId = shuffleId
        self.aggregator = aggregator
        self.partitioner = partitioner
        self.rddconf = rddconf


class AggregatorBase(object):

    def createCombiner(self, x):
        raise NotImplementedError(self.__class__.__name__)

    def mergeValue(self, s, x):
        raise NotImplementedError(self.__class__.__name__)

    def mergeCombiners(self, x, y):
        raise NotImplementedError(self.__class__.__name__)

    def aggregate_sorted(self, items):
        create = self.createCombiner
        merge = self.mergeValue
        i = None
        for i, (k, v) in enumerate(items):
            if i == 0:
                curr_key = k
                curr_value = create(v)
            elif k != curr_key:
                yield curr_key, curr_value
                curr_key = k
                curr_value = create(v)
            else:
                curr_value = merge(curr_value, v)
        if i is not None:
            yield curr_key, curr_value


class GroupByAggregator(AggregatorBase):

    def createCombiner(self, x):
        return [x]

    def mergeValue(self, c, x):
        c.append(x)
        return c

    def mergeCombiners(self, x, y):
        x.extend(y)
        return x


class Aggregator(object):
    def __init__(self, createCombiner, mergeValue,
                 mergeCombiners):
        self.createCombiner = createCombiner
        self.mergeValue = mergeValue
        self.mergeCombiners = mergeCombiners

    def __getstate__(self):
        return (dump_func(self.createCombiner),
                dump_func(self.mergeValue),
                dump_func(self.mergeCombiners))

    def __setstate__(self, state):
        c1, c2, c3 = state
        self.createCombiner = load_func(c1)
        self.mergeValue = load_func(c2)
        self.mergeCombiners = load_func(c3)


class AddAggregator:
    def createCombiner(self, x):
        return x

    def mergeValue(self, s, x):
        return s + x

    def mergeCombiners(self, x, y):
        return x + y


class MergeAggregator:
    def createCombiner(self, x):
        return [x]

    def mergeValue(self, s, x):
        s.append(x)
        return s

    def mergeCombiners(self, x, y):
        x.extend(y)
        return x


class HeapAggregator:

    def __init__(self, heap_limit, key=None, order_reverse=False):
        self.heap = HeapOnKey(key=key, min_heap=order_reverse)
        self.heap_limit = heap_limit
        assert (heap_limit > 0)

    def __getstate__(self):
        return self.heap, self.heap_limit

    def __setstate__(self, state):
        self.heap, self.heap_limit = state

    def createCombiner(self, x):
        return [x]

    def mergeValue(self, s, x):
        if len(s) >= self.heap_limit:
            self.heap.push_pop(s, x)
        else:
            self.heap.push(s, x)
        return s

    def mergeCombiners(self, x, y):
        for item in y:
            if len(x) < self.heap_limit:
                self.heap.push(x, item)
            else:
                self.heap.push_pop(x, item)
        return x


class UniqAggregator:
    def createCombiner(self, x):
        return set([x])

    def mergeValue(self, s, x):
        s.add(x)
        return s

    def mergeCombiners(self, x, y):
        x |= y
        return x


class Partitioner:
    @property
    def numPartitions(self):
        raise NotImplementedError

    def getPartition(self, key):
        raise NotImplementedError


class HashPartitioner(Partitioner):
    def __init__(self, partitions, thresholds=None):
        self.partitions = max(1, int(partitions))
        self.thresholds = thresholds
        assert self.partitions != 0
        assert self.thresholds is None or len(self.thresholds) == self.partitions - 1

    @property
    def numPartitions(self):
        return self.partitions

    def getPartition(self, key):
        if self.thresholds is None:
            return portable_hash(key) % self.partitions
        else:
            return bisect.bisect(self.thresholds, portable_hash(key))

    def __eq__(self, other):
        if isinstance(other, HashPartitioner):
            return other.numPartitions == self.numPartitions and \
                   other.thresholds == self.thresholds
        return False


class RangePartitioner(Partitioner):
    def __init__(self, keys, reverse=False):
        self.keys = sorted(keys)
        self.reverse = reverse

    @property
    def numPartitions(self):
        return len(self.keys) + 1

    def getPartition(self, key):
        idx = bisect.bisect(self.keys, key)
        return len(self.keys) - idx if self.reverse else idx

    def __eq__(self, other):
        if isinstance(other, RangePartitioner):
            return other.keys == self.keys and self.reverse == other.reverse
        return False

from utils import load_func, dump_func
from dependency import *

class Split:
    def __init__(self, idx):
        self.index = idx

class RDD:
    def __init__(self, sc):
        self.sc = sc
        self.id = sc.newRddId()
        self.partitioner = None
        self.shouldCache = False

    @property
    def splits(self):
        return []

    def compute(self, split):
        pass

    def preferredLocations(self, split):
        return []

    @property
    def dependencies(self):
        return []

    def cache(self):
        self.shouldCache = True
        return self

    def iterator(self, split):
        if self.shouldCache:
            for i in sc.cacheTracker.getOrCompute(self, split):
                yield i
        else:
            for i in self.compute(split):
                yield i

    def map(self, f):
        return MappedRDD(self, f)

    def flatMap(self, f):
        return FlatMappedRDD(self, f)

    def filter(self, f):
        return FilteredRDD(self, f)

    def sample(self, withReplacement, faction, seed):
        return SampleRDD(self, withReplacement, faction, seed)

    def union(self, rdd):
        return UnionRDD(self.sc, [self, rdd])

    def glom(self):
        return GlommedRDD(self)

    def cartesion(self, other):
        return CartesionRDD(self, other)

    def groupBy(self, f, numSplits=None):
        if numSplits is None:
            numSplits = self.sc.defaultNumSplit
        return self.map(lambda x: (f(x), x)).groupByKey(numSplits)

    def pipe(self, command):
        if isinstance(command, str):
            command = [command]
        return PipedRDD(this, command)

    def mapPartitions(self, f):
        return MapPartitionsRDD(self, f)

    def foreach(self, f):
        def mf(it):
            for i in it:
                f(i)
        return self.sc.runJob(self, mf)

    def collect(self):
        return sum(self.sc.runJob(self, lambda x:list(x)), [])

    def reduce(self, f):
        def reducePartition(it):
            if it:
                return [reduce(f, it)]
            else:
                return []
        options = self.sc.runJob(self, reducePartition)
        return reduce(f, sum(options, []))

    def count(self):
        result = sc.runJob(self, lambda x: len(x))
        return result.sum()

    def toList(self):
        return self.collect()

    def take(self, n):
        if n == 0: return []
        r = []
        p = 0
        while len(r) < n and p < self.splits.size:
            left = n - len(r)
            res = self.sc.runJob(self, lambda x: x[:left], [p], True)
            r.extend(res[0])
            if len(r) == n: break
            p += 1
        return r

    def first(self):
        r = self.take(1)
        if r: return r[0]

    def saveAsTextFile(self, path):
        return self.map(lambda x: (None, str(x))).saveAsHadoopFile(path)

    def saveAsObjectFile(self, path):
        return self.glom().map(lambda x: (None, dumps(x))).saveAsSequenceFile(path)


class MappedRDD(RDD):
    def __init__(self, prev, f=lambda x:x):
        RDD.__init__(self, prev.sc)
        self.prev = prev
        self.f = f
        self.dependencies = [OneToOneDependency(prev)]

    @property
    def splits(self):
        return self.prev.splits

    def compute(self, split):
        return map(self.f, self.prev.iterator(split))

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['f']
        return d, dump_func(self.f)

    def __setstate__(self, state):
        self.__dict__, code = state
        self.f = load_func(code)

class FlatMappedRDD(MappedRDD):
    def compute(self, split):
        for i in self.prev.iterator(split):
            for j in self.f(i):
                yield j

class FilteredRDD(MappedRDD):
    def compute(self, split):
        return filter(self.f, self.prev.iterator(split))
           
class GlommedRDD(MappedRDD):
    def compute(self, split):
        return [list(self.prev.iterator(split))]

class MapPartitionsRDD(MappedRDD):
    def compute(self, split):
        return self.f(self.prev.iterator(split))

class PipedRDD(MappedRDD):
    def compute(self, split):
        import subprocess
        p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        def read():
            for i in self.prev.iterator(split):
                p.stdin.write(str(i))
            p.stdin.close()
        Thread(target=read).start()
        for line in p.stdout:
            yield line


class CartesionSplit(Split):
    def __init__(self, idx, s1, s2):
        self.index = idx
        self.s1 = s1
        self.s2 = s2

class CartesionRDD(RDD):
    def __init__(self, rdd1, rdd2):
        RDD.__init__(self, rdd1.sc)
        self.rdd1 = rdd1
        self.rdd2 = rdd2
        self.numSplitsInRdd2 = n = len(rdd2.splits)
        self._splits = [CartesionSplit(s1.index*n+s2.index, s1, s2)
            for s1 in rdd1.splits for s2 in rdd2.splits]
        self.dependencies = [CartesionDependency(rdd1, True, n),
                             CartesionDependency(rdd2, False, n)]

    @property
    def splits(self):
        return self._splits

    def preferredLocations(self, split):
        return self.rdd1.preferredLocations(split.s1) + self.rdd2.preferredLocations(split.s2)

    def compute(self, split):
        for x in self.rdd1.iterator(split.s1):
            for y in self.rdd2.iterator(split.s2):
                yield x,y


class CoGroupSplit(Split):
    def __init__(self, idx, deps):
        self.index = idx
        self.deps = deps

    def __hash__(self):
        return self.index

class CoGroupAggregator:
    def createCombiner(self, v):
        return [v]
    def mergeValue(self, c, v):
        c = c + v
        return c
    def mergeCombiners(self, c, v):
        return c + v

class CoGroupedRDD(RDD):
    def __init__(self, rdds, partitioner):
        RDD.__init__(self, rdds[0].sc)
        self.rdds = rdds
        self.partitioner = partitioner
        raise NotImplementedError
        # TODO

class SampledRDDSplit(Split):
    def __init__(self, prev, seed):
        pass

class SampleRDD(RDD):
    def __init__(self, prev, withReplacement, frac, seed):
        RDD.__init__(self, prev.sc)
        self.prev = prev
        raise NotImplementedError
        # TODO

class UnionSplit:
    def __init__(self, idx, rdd, split):
        self.index = idx
        self.rdd = rdd
        self.split = split

class UnionRDD(RDD):
    def __init__(self, sc, rdds):
        RDD.__init__(self, sc)
        self.rdds = rdds
        self._splits = []
        for rdd in rdds:
            for split in rdd.splits:
                self._splits.append(UnionSplit(len(self._splits), rdd, split))
    
    @property
    def splits(self):
        return self._splits

    def preferredLocations(self, split):
        return split.rdd.preferredLocations(split.split)

    def compute(self, split):
        return split.rdd.iterator(split.split)

class ParallelCollectionSplit:
    def __init__(self, rddId, slice, values):
        self.rddId = rddId
        self.index = self.slice = slice
        self.values = values
        
    def iterator(self):
        return self.values

    def __hash__(self):
        return 41 * (41 + self.rddId) + slice

    def __equal__(self, other):
        return (isinstance(other, ParallelCollectionSplit) and 
            self.rddId == other.rddId and self.slice == other.slice)

class ParallelCollection(RDD):
    def __init__(self, sc, data, numSlices):
        RDD.__init__(self, sc)
        self.data = data
        self.numSlices = numSlices
        slices = self.slice(data, numSlices)
        self._splits = [ParallelCollectionSplit(self.id, i, slices[i]) 
            for i in range(len(slices))]
        self.dependencies = []

    @property
    def splits(self):
        return self._splits

    def compute(self, split):
        return split.values

    def preferredLocations(self, split):
        return []

    @classmethod
    def slice(cls, data, numSlices):
        m = len(data)
        n = m / numSlices
        if m % numSlices != 0:
            n += 1
        data = list(data)
        return [data[i*n : i*n+n] for i in range(numSlices)]

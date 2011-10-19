import os, os.path

from utils import load_func, dump_func
from dependency import *
from env import env

class Split:
    def __init__(self, idx):
        self.index = idx

class RDD:
    def __init__(self, sc):
        self.sc = sc
        self.id = sc.newRddId()
        self.partitioner = None
        self.shouldCache = False
        self._splits = []
        self.dependencies = []

    @property
    def splits(self):
        return self._splits

    def compute(self, split):
        raise NotImplementedError

    def preferredLocations(self, split):
        return []

    def cache(self):
        self.shouldCache = True
        return self

    def iterator(self, split):
        if self.shouldCache:
            for i in env.cacheTracker.getOrCompute(self, split):
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
            numSplits = min(self.sc.defaultMinSplits, len(self.splits))
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

    def __iter__(self):
        for i in self.collect():
            yield i

    def reduce(self, f):
        def reducePartition(it):
            if it:
                return [reduce(f, it)]
            else:
                return []
        options = self.sc.runJob(self, reducePartition)
        return reduce(f, sum(options, []))

    def uniq(self):
        return self.map(lambda x:(x,None)).reduceByKey(lambda x,y:None).map(lambda (x,y):x)

    def count(self):
        def ilen(x):
            s = 0
            for i in x:
                s += 1
            return s
        result = self.sc.runJob(self, lambda x: ilen(x))
        return sum(result)

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
        return OutputTextFileRDD(self, path)

#    def saveAsBinaryFile(self, path):
#        return OutputBinaryFile(self, path)

 #   def saveAsObjectFile(self, path):
 #       return self.glom().map(lambda x:cPickle.dumps(x)).saveAsTextFile(path)

    # Extra functions for (K,V) pairs RDD
    def reduceByKeyToDriver(self, func):
        def mergeMaps(m1, m2):
            for k,v in m2.iteritems():
                m1[k]=func(m1[k], v) if k in m1 else v
            return m1
        return self.map(lambda (x,y):{x:y}).reduce(mergeMaps)

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners, numSplits=None):
        if numSplits is None:
            numSplits = min(self.sc.defaultMinSplits, len(self.splits))
        aggregator = Aggregator(createCombiner, mergeValue, mergeCombiners)
        partitioner = HashPartitioner(numSplits)
        return ShuffledRDD(self, aggregator, partitioner)

    def reduceByKey(self, func, numSplits=None):
        return self.combineByKey(lambda x:x, func, func, numSplits)

    def groupByKey(self, numSplits=None):
        createCombiner = lambda x: [x]
        mergeValue = lambda l, v: l+[v]
        mergeCombiners = lambda l1, l2: l1 + l2
        return self.combineByKey(createCombiner, mergeValue, mergeCombiners, numSplits)

    def join(self, other, numSplits=None):
        vs = self.map(lambda k,v: (k,(1,v)))
        ws = other.map(lambda k,v: (k,(2,v)))
        def dispatch((k,seq)):
            vbuf, wbuf = [], []
            for n,v in seq:
                if n == 1:
                    vbuf.append(v)
                elif n == 2:
                    wbuf.append(v)
                else:
                    yield Exception("invalid n")
            for vv in v:
                for ww in w:
                    yield (k, (vv, ww))
        ws.union(ws).groupByKey(numSplits).flatMap(dispatch)

    def leftOuterJoin(self, other, numSplits=None):
        vs = self.map(lambda k,v: (k,(1,v)))
        ws = other.map(lambda k,v: (k,(2,v)))
        def dispatch((k,seq)):
            vbuf, wbuf = [], []
            for n,v in seq:
                if n == 1:
                    vbuf.append(v)
                elif n == 2:
                    wbuf.append(v)
                else:
                    yield Exception("invalid n")
            if not wbuf:
                wbuf.append(None)
            for vv in v:
                for ww in w:
                    yield (k, (vv, ww))
        ws.union(ws).groupByKey(numSplits).flatMap(dispatch)

    def rightOuterJoin(self, other, numSplits=None):
        vs = self.map(lambda k,v: (k,(1,v)))
        ws = other.map(lambda k,v: (k,(2,v)))
        def dispatch((k,seq)):
            vbuf, wbuf = [], []
            for n,v in seq:
                if n == 1:
                    vbuf.append(v)
                elif n == 2:
                    wbuf.append(v)
                else:
                    yield Exception("invalid n")
            if not vbuf:
                vbuf.append(None)
            for vv in v:
                for ww in w:
                    yield (k, (vv, ww))
        ws.union(ws).groupByKey(numSplits).flatMap(dispatch)

    def collectAsMap(self):
        return dict(self.collect())

    def mapValues(self, f):
        return MappedValuesRDD(self, f)

    def flatMapValues(self, f):
        return FlatMappedValuesRDD(self, f)

    def groupWith(self, other):
        part = self.partitioner or HashPartitioner(self.sc.defaultParallelism)
        return CoGroupedRDD([self, other], part)

    def groupWith2(self, other1, other2):
        part = self.partitioner or HashPartitioner(self.sc.defaultParallelism)
        return CoGroupedRDD([self, other1, other2], part)

    def lookup(self, key):
        if self.partitioner:
            index = self.partitioner.getPartition(key)
            def process(it):
                return [v for k,v in it if k == key]
            return self.sc.runJob(self, process, [index], False)
        else:
            raise Exception("lookup() called on an RDD without a partitioner")
            
#    def saveAsHadoopFile(self, path):
#        pass

#   def saveAsHadoopDataset(self, conf):
#        pass


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
        self.f = load_func(code, globals())

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

class MappedValuesRDD(MappedRDD):

    @property
    def partitioner(self):
        return self.prev.partitioner

    def compute(self, split):
        return map(lambda k,v: (k,self.f(v)), self.prev.iterator(split))

class FlatMappedValuesRDD(MappedValuesRDD):
    def compute(self, split):
        for k,v in self.prev.iterator(split):
            for vv in self.f(v):
                yield k,vv

class ShuffledRDDSplit(Split):
    def __hash__(self):
        return self.index

class ShuffledRDD(RDD):
    def __init__(self, parent, aggregator, part):
        RDD.__init__(self, parent.sc)
        self.parent = parent
        self.aggregator = aggregator
        self.partitioner = part
        self._splits = [ShuffledRDDSplit(i) for i in range(part.numPartitions)]
        self.dependencies = [ShuffleDependency(self.sc.newShuffleId(), parent, aggregator, part)]
    
    def compute(self, split):
        combiners = {}
        mergeCombiners = self.aggregator.mergeCombiners
        def mergePair(k, c):
            o = combiners.get(k, None)
            if o is None:
                combiners[k] = c
            else:
                combiners[k] = mergeCombiners(o, c)
        fetcher = env.shuffleFetcher
        fetcher.fetch(self.dependencies[0].shuffleId, split.index, mergePair)
        return combiners.iteritems()

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

    def compute(self, split):
        return split.values

    @classmethod
    def slice(cls, data, numSlices):
        m = len(data)
        n = m / numSlices
        if m % numSlices != 0:
            n += 1
        data = list(data)
        return [data[i*n : i*n+n] for i in range(numSlices)]


class TextFileRDD(RDD):
    def __init__(self, sc, path, numSplits=None, splitSize=None):
        RDD.__init__(self, sc)
        self.path = path
        if not os.path.exists(path):
            raise IOError("not exists")
        size = os.path.getsize(path)
        if splitSize is None:
            if numSplits is None:
                splitSize = 64*1024*1024
            else:
                splitSize = size / numSplits
        
        n = size / splitSize
        if size % splitSize > 0:
            n += 1
        self.splitSize = splitSize
        self._splits = [Split(i) for i in range(n)]

    def compute(self, split):
        f = open(self.path)
        start = split.index * self.splitSize
        end = start + self.splitSize
        if start > 0:
            f.seek(start-1)
            byte = f.read(1)
            skip = byte != '\n'
        else:
            f.seek(start)
            skip = False
        for line in f:
            if start >= end: break
            start += len(line)
            if skip:
                skip = False
            else:
                if line[-1] == '\n': line = line[:-1]
                yield line
        f.close()

class OutputTextFileRDD(RDD):
    def __init__(self, rdd, path):
        RDD.__init__(self, rdd.sc)
        self.rdd = rdd
        self.path = os.path.abspath(path)
        if os.path.exists(path):
            if not os.path.isdir(path):
                raise Exception("output must be dir")
        else:
            os.makedirs(path)
        self.dependencies = [OneToOneDependency(rdd)]

    @property
    def splits(self):
        return self.rdd.splits

    def compute(self, split):
        path = os.path.join(self.path, str(split.index))
        f = open(path,'w')
        for line in self.rdd.iterator(split):
            f.write(line)
            if not line.endswith('\n'):
                f.write('\n')
        f.close()
        yield path

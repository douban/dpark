import os, os.path
import threading
from itertools import *

from utils import load_func, dump_func
from dependency import *
from env import env

def ilen(x):
    try:
        return len(x)
    except TypeError:
        return sum(1 for i in x)

class Split:
    def __init__(self, idx):
        self.index = idx

class RDD:
    def __init__(self, ctx):
        self.ctx = ctx
        self.id = ctx.newRddId()
        self._splits = []
        self.dependencies = []
        self.aggregator = None
        self._partitioner = None
        self.shouldCache = False

    def __len__(self):
        return len(self.splits)
    
    @property
    def splits(self):
        return self._splits

    def compute(self, split):
        raise NotImplementedError

    @property
    def partitioner(self):
        return self._partitioner

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
        return UnionRDD(self.ctx, [self, rdd])

    def glom(self):
        return GlommedRDD(self)

    def cartesion(self, other):
        return CartesionRDD(self, other)

    def groupBy(self, f, numSplits=None):
        if numSplits is None:
            numSplits = min(self.ctx.defaultMinSplits, len(self.splits))
        return self.map(lambda x: (f(x), x)).groupByKey(numSplits)

    def pipe(self, command):
        if isinstance(command, str):
            command = command.split(' ')
        return PipedRDD(self, command)

    def mapPartitions(self, f):
        return MapPartitionsRDD(self, f)

    def foreach(self, f):
        def mf(it):
            for i in it:
                f(i)
        return self.ctx.runJob(self, mf)

    def collect(self):
        return sum(self.ctx.runJob(self, lambda x:list(x)), [])

    def __iter__(self):
        for i in self.collect():
            yield i

    def reduce(self, f):
        def reducePartition(it):
            if it:
                return [reduce(f, it)]
            else:
                return []
        options = self.ctx.runJob(self, reducePartition)
        return reduce(f, sum(options, []))

    def uniq(self):
        g = self.map(lambda x:(x,None)).reduceByKey(lambda x,y:None)
        return g.map(lambda (x,y):x)

    def count(self):
        result = self.ctx.runJob(self, lambda x: ilen(x))
        return sum(result)

    def toList(self):
        return self.collect()

    def take(self, n):
        if n == 0: return []
        r = []
        p = 0
        while len(r) < n and p < len(self.splits):
            res = self.ctx.runJob(self, lambda x: islice(x, n - len(r)), [p], True)
            if res[0]:
                r.extend(res[0])
            else:
                break 
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
            numSplits = min(self.ctx.defaultMinSplits, len(self.splits))
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
        vs = self.map(lambda (k,v): (k,(1,v)))
        ws = other.map(lambda (k,v): (k,(2,v)))
        def dispatch((k,seq)):
            vbuf, wbuf = [], []
            for n,v in seq:
                if n == 1:
                    vbuf.append(v)
                elif n == 2:
                    wbuf.append(v)
                else:
                    yield Exception("invalid n")
            for vv in vbuf:
                for ww in wbuf:
                    yield (k, (vv, ww))
        return vs.union(ws).groupByKey(numSplits).flatMap(dispatch)

    def leftOuterJoin(self, other, numSplits=None):
        vs = self.map(lambda (k,v): (k,(1,v)))
        ws = other.map(lambda (k,v): (k,(2,v)))
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
            for vv in vbuf:
                for ww in wbuf:
                    yield (k, (vv, ww))
        return vs.union(ws).groupByKey(numSplits).flatMap(dispatch)

    def rightOuterJoin(self, other, numSplits=None):
        vs = self.map(lambda (k,v): (k,(1,v)))
        ws = other.map(lambda (k,v): (k,(2,v)))
        def dispatch((k,seq)):
            vbuf, wbuf = [], []
            for n,v in seq:
                if n == 1:
                    vbuf.append(v)
                elif n == 2:
                    wbuf.append(v)
            if not vbuf:
                vbuf.append(None)
            for vv in vbuf:
                for ww in wbuf:
                    yield (k, (vv, ww))
        return vs.union(ws).groupByKey(numSplits).flatMap(dispatch)

    def collectAsMap(self):
        return dict(self.collect())

    def mapValue(self, f):
        return MappedValuesRDD(self, f)

    def flatMapValue(self, f):
        return FlatMappedValuesRDD(self, f)

    def groupWith(self, *others):
        part = self.partitioner or HashPartitioner(self.ctx.defaultParallelism)
        return CoGroupedRDD([self]+list(others), part)

    def lookup(self, key):
        if self.partitioner:
            index = self.partitioner.getPartition(key)
            def process(it):
                for k,v in it:
                    if k == key:
                        return v
            return self.ctx.runJob(self, process, [index], False)[0]
        else:
            raise Exception("lookup() called on an RDD without a partitioner")
            

class MappedRDD(RDD):
    def __init__(self, prev, func=lambda x:x):
        RDD.__init__(self, prev.ctx)
        self.prev = prev
        self.func = func
        self.dependencies = [OneToOneDependency(prev)]

    def __str__(self):
        return '<%s %s>' % (self.__class__.__name__, self.prev)
    
    @property
    def splits(self):
        return self.prev.splits

    def compute(self, split):
        for v in self.prev.iterator(split):
            yield self.func(v)

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['func']
        return d, dump_func(self.func)

    def __setstate__(self, state):
        self.__dict__, code = state
        self.func = load_func(code, globals())

class FlatMappedRDD(MappedRDD):
    def compute(self, split):
        for i in self.prev.iterator(split):
            for j in self.func(i):
                yield j

class FilteredRDD(MappedRDD):
    def compute(self, split):
        for i in self.prev.iterator(split):
            if self.func(i):
                yield i
           
class GlommedRDD(RDD):
    def __init__(self, prev):
        RDD.__init__(self, prev.ctx)
        self.prev = prev
        self.splits = self.prev.splits
        self.dependencies = [OneToOneDependency(prev)]

    def compute(self, split):
        yield self.prev.iterator(split)

class MapPartitionsRDD(MappedRDD):
    def compute(self, split):
        return self.func(self.prev.iterator(split))

class PipedRDD(RDD):
    def __init__(self, prev, command):
        RDD.__init__(self, prev.ctx)
        self.prev = prev
        self.command = command
        self.dependencies = [OneToOneDependency(prev)]

    def __str__(self):
        return '<PipedRDD %s %s>' % (' '.join(self.command), self.prev)

    @property
    def splits(self):
        return self.prev.splits

    def compute(self, split):
        import subprocess
        p = subprocess.Popen(self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        def read():
            for i in self.prev.iterator(split):
                p.stdin.write("%s\n" % (i,))
            p.stdin.close()
        threading.Thread(target=read).start()
        for line in p.stdout:
            yield line[:-1] # drop \n

class MappedValuesRDD(MappedRDD):
    @property
    def partitioner(self):
        return self.prev.partitioner

    def compute(self, split):
        for k, v in self.prev.iterator(split):
            yield k,self.func(v)

class FlatMappedValuesRDD(MappedValuesRDD):
    def compute(self, split):
        for k,v in self.prev.iterator(split):
            for vv in self.func(v):
                yield k,vv

class ShuffledRDDSplit(Split):
    def __hash__(self):
        return self.index

class ShuffledRDD(RDD):
    def __init__(self, parent, aggregator, part):
        RDD.__init__(self, parent.ctx)
        self.parent = parent
        self.aggregator = aggregator
        self._partitioner = part
        self._splits = [ShuffledRDDSplit(i) for i in range(part.numPartitions)]
        self.dependencies = [ShuffleDependency(self.ctx.newShuffleId(),
                parent, aggregator, part)]
    
    def __str__(self):
        return '<ShuffledRDD %s>' % self.parent

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
        RDD.__init__(self, rdd1.ctx)
        self.rdd1 = rdd1
        self.rdd2 = rdd2
        self.numSplitsInRdd2 = n = len(rdd2.splits)
        self.splits = [CartesionSplit(s1.index*n+s2.index, s1, s2)
            for s1 in rdd1.splits for s2 in rdd2.splits]
        self.dependencies = [CartesionDependency(rdd1, True, n),
                             CartesionDependency(rdd2, False, n)]

    def __str__(self):
        return '<cartesion %s and %s>' % (self.rdd1, self.rdd2)

    def preferredLocations(self, split):
        return self.rdd1.preferredLocations(split.s1) + self.rdd2.preferredLocations(split.s2)

    def compute(self, split):
        for x in self.rdd1.iterator(split.s1):
            for y in self.rdd2.iterator(split.s2):
                yield x,y


class CoGroupSplitDep: pass
class NarrowCoGroupSplitDep(CoGroupSplitDep):
    def __init__(self, rdd, split):
        self.rdd = rdd
        self.split = split
class ShuffleCoGroupSplitDep(CoGroupSplitDep):
    def __init__(self, shuffleId):
        self.shuffleId = shuffleId

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
        return c + [v]
    def mergeCombiners(self, c, v):
        return c + v

class CoGroupedRDD(RDD):
    def __init__(self, rdds, partitioner):
        RDD.__init__(self, rdds[0].ctx)
        self.rdds = rdds
        self.aggregator = CoGroupAggregator()
        self.partitioner = partitioner
        self.dependencies = dep = [rdd.partitioner == partitioner
                and OneToOneDependency(rdd)
                or ShuffleDependency(self.ctx.newShuffleId(), 
                    rdd, self.aggregator, partitioner)
                for i,rdd in enumerate(rdds)]
        self.splits = [CoGroupSplit(j, 
                          [isinstance(dep[i],ShuffleDependency)
                            and ShuffleCoGroupSplitDep(dep[i].shuffleId)
                            or NarrowCoGroupSplitDep(r, r.splits[j]) 
                            for i,r in enumerate(rdds)])
                        for j in range(partitioner.numPartitions)]
        
    def compute(self, split):
        m = {}
        def getSeq(k):
            return m.setdefault(k, [[] for i in self.rdds])
        for i,dep in enumerate(split.deps):
            if isinstance(dep, NarrowCoGroupSplitDep):
                if dep.rdd.aggregator:
                    for k,vs in dep.rdd.iterator(dep.split):
                        getSeq(k)[i].extend(vs)
                else:
                    for k,v in dep.rdd.iterator(dep.split):
                        getSeq(k)[i].append(v)
            elif isinstance(dep, ShuffleCoGroupSplitDep):
                def mergePair(k, vs):
                    getSeq(k)[i].extend(vs)
                env.shuffleFetcher.fetch(dep.shuffleId, split.index, mergePair)
        return m.iteritems()
        
class SampledRDDSplit(Split):
    def __init__(self, prev, seed):
        pass

class SampleRDD(RDD):
    def __init__(self, prev, withReplacement, frac, seed):
        RDD.__init__(self, prev.ctx)
        self.prev = prev
        raise NotImplementedError
        # TODO

class UnionSplit:
    def __init__(self, idx, rdd, split):
        self.index = idx
        self.rdd = rdd
        self.split = split

class UnionRDD(RDD):
    def __init__(self, ctx, rdds):
        RDD.__init__(self, ctx)
        self.rdds = rdds
        self.splits = [UnionSplit(0, rdd, split) 
                for rdd in rdds for split in rdd.splits]
        for i,split in enumerate(self.splits):
            split.index = i
        self.dependencies = []
        pos = 0
        for rdd in rdds:
            self.dependencies.append(RangeDependency(rdd, 0, pos, len(rdd)))
            pos += len(rdd)

    def __str__(self):
        return '<union of %s>' % (','.join(str(rdd) for rdd in self.rdds))

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
    def __init__(self, ctx, data, numSlices):
        RDD.__init__(self, ctx)
        self.size = len(data)
        slices = self.slice(data, numSlices)
        self._splits = [ParallelCollectionSplit(self.id, i, slices[i]) 
                for i in range(len(slices))]
        self.dependencies = []

    def __str__(self):
        return '<ParallelCollection %d>' % self.size

    def compute(self, split):
        return split.values

    @classmethod
    def slice(cls, data, numSlices):
        if numSlices <= 0:
            raise ValueError("invalid numSlices %d" % numSlices)
        m = len(data)
        if not m:
            return [[]]
        n = m / numSlices
        if m % numSlices != 0:
            n += 1
        if isinstance(data, xrange):
            first = data[0]
            last = data[n-1]
            step = (last - first) / (m-1)
            nstep = step * n
            return [xrange(first+i*nstep, first+(i+1)*nstep, nstep)
                for i in range(numSlices)]
        if not isinstance(data, list):
            data = list(data)
        return [data[i*n : i*n+n] for i in range(numSlices)]


class TextFileRDD(RDD):
    def __init__(self, ctx, path, numSplits=None, splitSize=None):
        RDD.__init__(self, ctx)
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

    def __str__(self):
        return '<TextFileRDD %s>' % self.path

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
        RDD.__init__(self, rdd.ctx)
        self.rdd = rdd
        self.path = os.path.abspath(path)
        if os.path.exists(path):
            if not os.path.isdir(path):
                raise Exception("output must be dir")
        else:
            os.makedirs(path)
        self._splits = self.rdd.splits
        self.dependencies = [OneToOneDependency(rdd)]

    def __str__(self):
        return '<OutputTextFileRDD %s>' % self.path

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

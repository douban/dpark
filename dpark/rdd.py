import sys
import os, os.path
import time
import threading
import socket
import csv
import cStringIO
import itertools
import operator
import math
import random
import bz2
import logging
from copy import copy
import shutil

from serialize import load_func, dump_func
from dependency import *
from env import env

logger = logging.getLogger("rdd")

def ilen(x):
    try:
        return len(x)
    except TypeError:
        return sum(1 for i in x)

class Split:
    def __init__(self, idx):
        self.index = idx

def cached(func):
    def getstate(self):
        d = self._pickle_cache.get(self.id)
        if d is None:
            d = func(self)
            self._pickle_cache[self.id] = d
        return d
    return getstate

class RDD:
    _pickle_cache = {}
    def __init__(self, ctx):
        self.ctx = ctx
        self.id = RDD.newId()
        self.err = ctx.options.err
        self._splits = []
        self.dependencies = []
        self.aggregator = None
        self._partitioner = None
        self.shouldCache = False

    nextId = 0
    @classmethod
    def newId(cls):
        cls.nextId += 1
        return cls.nextId

    @cached
    def __getstate__(self):
        d = dict(self.__dict__)
        d.pop('dependencies', None)
        d.pop('_splits', None)
        d.pop('ctx', None)
        return d

    def __len__(self):
        return len(self.splits)
   
    def __getslice__(self, i,j):
        return SliceRDD(self, i, j)

    def mergeSplit(self, splitSize=2, numSplits=None):
        return MergedRDD(self, splitSize, numSplits)

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
#        self.shouldCache = True
        return self

    def iterator(self, split):
        if self.shouldCache:
            return env.cacheTracker.getOrCompute(self, split)
        else:
            return self.compute(split)

    def map(self, f):
        return MappedRDD(self, f)

    def flatMap(self, f):
        return FlatMappedRDD(self, f)

    def filter(self, f):
        return FilteredRDD(self, f)

    def sample(self, faction, withReplacement=False, seed=12345):
        return SampleRDD(self, faction, withReplacement, seed)

    def union(self, rdd):
        return UnionRDD(self.ctx, [self, rdd])

    def sort(self, key=lambda x:x, numSplits=None):
        if numSplits is None:
            numSplits = min(self.ctx.defaultMinSplits, len(self))
        n = numSplits * 10 / len(self)
        samples = self.glom().flatMap(lambda x:itertools.islice(x, n)).map(key).collect()
        keys = sorted(samples)[5::10][:numSplits-1]
        aggr = MergeAggregator()
        parter = RangePartitioner(keys)
        parted = ShuffledRDD(self.map(lambda x:(key(x),x)), aggr, parter).flatMap(lambda (x,y):y)
        return parted.glom().flatMap(lambda x:sorted(x, key=key))

    def glom(self):
        return GlommedRDD(self)

    def cartesian(self, other):
        return CartesianRDD(self, other)
    
    def zipWith(self, other):
        return ZippedRDD(self.ctx, [self, other])

    def groupBy(self, f, numSplits=None):
        if numSplits is None:
            numSplits = min(self.ctx.defaultMinSplits, len(self))
        return self.map(lambda x: (f(x), x)).groupByKey(numSplits)

    def pipe(self, command, quiet=False):
        if isinstance(command, str):
            command = command.split(' ')
        return PipedRDD(self, command, quiet)

    def fromCsv(self, dialect='excel'):
        return CSVReaderRDD(self, dialect)

    def mapPartitions(self, f):
        return MapPartitionsRDD(self, f)
    mapPartiton = mapPartitions

    def foreach(self, f):
        def mf(it):
            for i in it:
                f(i)
        list(self.ctx.runJob(self, mf))

    def collect(self):
        return sum(self.ctx.runJob(self, lambda x:list(x)), [])

    def __iter__(self):
        return self.collect()

    def reduce(self, f):
        def reducePartition(it):
            if self.err < 1e-8:
                try:
                    return [reduce(f, it)]
                except TypeError:
                    return []
            
            s = None
            total, err = 0, 0
            for v in it:
                try:
                    total += 1
                    if s is None:
                        s = v
                    else:
                        s = f(s, v)
                except Exception, e:
                    logging.warning("skip bad record %s: %s", v, e)
                    err += 1
                    if total > 100 and err > total * self.err * 10:
                        raise Exception("too many error occured: %s" % (float(err)/total))

            if err > total * self.err:
                raise Exception("too many error occured: %s" % (float(err)/total))
            
            return [s] if s is not None else []

        s = sum(self.ctx.runJob(self, reducePartition), [])
        if s:
            return reduce(f, s)

    def uniq(self, numSplits=None):
        g = self.map(lambda x:(x,None)).reduceByKey(lambda x,y:None, numSplits)
        return g.map(lambda (x,y):x)

    def fold(self, zero, f):
        '''Aggregate the elements of each partition, and then the
        results for all the partitions, using a given associative
        function and a neutral "zero value". The function op(t1, t2)
        is allowed to modify t1 and return it as its result value to
        avoid object allocation; however, it should not modify t2.'''
        return reduce(f,
                      self.ctx.runJob(self, lambda x: reduce(f, x, copy(zero))),
                      zero)

    def aggregate(self, zero, seqOp, combOp):
        '''Aggregate the elements of each partition, and then the
        results for all the partitions, using given combine functions
        and a neutral "zero value". This function can return a
        different result type, U, than the type of this RDD, T. Thus,
        we need one operation for merging a T into an U (seqOp(U, T))
        and one operation for merging two U's (combOp(U, U)). Both of
        these functions are allowed to modify and return their first
        argument instead of creating a new U to avoid memory
        allocation.'''
        return reduce(combOp,
                      self.ctx.runJob(self, lambda x: reduce(seqOp, x, copy(zero))),
                      zero)

    def count(self):
        return sum(self.ctx.runJob(self, lambda x: ilen(x)))

    def toList(self):
        return self.collect()

    def take(self, n):
        if n == 0: return []
        r = []
        p = 0
        while len(r) < n and p < len(self):
            res = list(self.ctx.runJob(self, lambda x: list(itertools.islice(x, n - len(r))), [p], True))[0]
            if res:
                r.extend(res)
            p += 1
        return r

    def first(self):
        r = self.take(1)
        if r: return r[0]

    def saveAsTextFile(self, path, ext='', overwrite=False):
        return OutputTextFileRDD(self, path, ext, overwrite).collect()

    def saveAsTextFileByKey(self, path, ext='', overwrite=False):
        return MultiOutputTextFileRDD(self, path, ext, overwrite).collect()

    def saveAsCSVFile(self, path, overwrite=False):
        return OutputCSVFileRDD(self, path, overwrite).collect()

    # Extra functions for (K,V) pairs RDD
    def reduceByKeyToDriver(self, func):
        def mergeMaps(m1, m2):
            for k,v in m2.iteritems():
                m1[k]=func(m1[k], v) if k in m1 else v
            return m1
        return self.map(lambda (x,y):{x:y}).reduce(mergeMaps)

    def combineByKey(self, aggregator, splits=None):
        if splits is None:
            splits = min(self.ctx.defaultMinSplits, len(self))
        if type(splits) is int:
            splits = HashPartitioner(splits)
        return ShuffledRDD(self, aggregator, splits)

    def reduceByKey(self, func, numSplits=None):
        aggregator = Aggregator(lambda x:x, func, func)
        return self.combineByKey(aggregator, numSplits)

    def groupByKey(self, numSplits=None):
        createCombiner = lambda x: [x]
        mergeValue = lambda x,y:x.append(y) or x
        mergeCombiners = lambda x,y: x.extend(y) or x
        aggregator = Aggregator(createCombiner, mergeValue, mergeCombiners)
        return self.combineByKey(aggregator, numSplits)

    def partitionByKey(self, numSplits=None):
        return self.groupByKey(numSplits).flatMapValue(lambda x: x)

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
            for vv in vbuf:
                for ww in wbuf:
                    yield (k, (vv, ww))
        return vs.union(ws).groupByKey(numSplits).flatMap(dispatch)

    def outerJoin(self, other, numSplits=None):
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
            if not wbuf:
                wbuf.append(None)
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
        d = {}
        for v in self.ctx.runJob(self, lambda x:list(x)):
            d.update(dict(v))
        return d

    def mapValue(self, f):
        return MappedValuesRDD(self, f)

    def flatMapValue(self, f):
        return FlatMappedValuesRDD(self, f)

    def groupWith(self, *others, **kw):
        numSplits = kw.get('numSplits') or self.ctx.defaultParallelism
        part = self.partitioner or HashPartitioner(numSplits)
        return CoGroupedRDD([self]+list(others), part)

    def lookup(self, key):
        if self.partitioner:
            index = self.partitioner.getPartition(key)
            def process(it):
                for k,v in it:
                    if k == key:
                        return v
            return list(self.ctx.runJob(self, process, [index], False))[0]
        else:
            raise Exception("lookup() called on an RDD without a partitioner")


class MappedRDD(RDD):
    def __init__(self, prev, func=lambda x:x):
        RDD.__init__(self, prev.ctx)
        self.prev = prev
        self.func = func
        self.dependencies = [OneToOneDependency(prev)]
    
    def __len__(self):
        return len(self.prev)

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.prev)
    
    @property
    def splits(self):
        return self.prev.splits

    def preferredLocations(self, split): 
        return self.prev.preferredLocations(split)

    def compute(self, split):
        if self.err < 1e-8:
            return itertools.imap(self.func, self.prev.iterator(split))
        return self._compute_with_error(split)

    def _compute_with_error(self, split):
        total, err = 0, 0
        for v in self.prev.iterator(split):
            try:
                total += 1
                yield self.func(v)
            except Exception, e:
                logger.warning("ignored record %r: %s", v, e)
                err += 1
                if total > 100 and err > total * self.err * 10:
                    raise Exception("too many error occured: %s" % (float(err)/total))

        if err > total * self.err:
            raise Exception("too many error occured: %s" % (float(err)/total))

    @cached
    def __getstate__(self):
        d = RDD.__getstate__(self)
        del d['func']
        return d, dump_func(self.func)

    def __setstate__(self, state):
        self.__dict__, code = state
        try:
            self.func = load_func(code)
        except Exception:
            print 'load failed', self.__class__, code
            raise

class FlatMappedRDD(MappedRDD):
    def compute(self, split):
        if self.err < 1e-8:
            return itertools.chain.from_iterable(itertools.imap(self.func,
                self.prev.iterator(split)))
        return self._compute_with_error(split)
    
    def _compute_with_error(self, split):
        total, err = 0, 0
        for v in self.prev.iterator(split):
            try:
                total += 1
                for k in self.func(v):
                    yield k
            except Exception, e:
                logger.warning("ignored record %r: %s", v, e)
                err += 1
                if total > 100 and err > total * self.err * 10:
                    raise Exception("too many error occured: %s" % (float(err)/total))

        if err > total * self.err:
            raise Exception("too many error occured: %s" % (float(err)/total))


class FilteredRDD(MappedRDD):
    def compute(self, split):
        if self.err < 1e-8:
            return itertools.ifilter(self.func, self.prev.iterator(split))
        return self._compute_with_error(split)

    def _compute_with_error(self, split):
        total, err = 0, 0
        for v in self.prev.iterator(split):
            try:
                total += 1
                if self.func(v):
                    yield v
            except Exception, e:
                logger.warning("ignored record %r: %s", v, e)
                err += 1
                if total > 100 and err > total * self.err * 10:
                    raise Exception("too many error occured: %s" % (float(err)/total))

        if err > total * self.err:
            raise Exception("too many error occured: %s" % (float(err)/total))
           
class GlommedRDD(RDD):
    def __init__(self, prev):
        RDD.__init__(self, prev.ctx)
        self.prev = prev
        self.dependencies = [OneToOneDependency(prev)]

    def __len__(self):
        return len(self.prev)

    @property
    def splits(self):
        return self.prev.splits

    def preferredLocations(self, split): 
        return self.prev.preferredLocations(split)

    def compute(self, split):
        yield self.prev.iterator(split)

class MapPartitionsRDD(MappedRDD):
    def compute(self, split):
        return self.func(self.prev.iterator(split))

class PipedRDD(RDD):
    def __init__(self, prev, command, quiet=False):
        RDD.__init__(self, prev.ctx)
        self.prev = prev
        self.command = command
        self.quiet = quiet
        self.dependencies = [OneToOneDependency(prev)]

    def __len__(self):
        return len(self.prev)

    def __repr__(self):
        return '<PipedRDD %s %s>' % (' '.join(self.command), self.prev)

    @property
    def splits(self):
        return self.prev.splits

    def preferredLocations(self, split): 
        return self.prev.preferredLocations(split)

    def compute(self, split):
        import subprocess
        p = subprocess.Popen(self.command, stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, 
                stderr=self.quiet and subprocess.PIPE or sys.stderr)
        def read(stdin):
            it = self.prev.iterator(split)
            if isinstance(it, list):
                it = iter(it)
            try:
                first = it.next()
            except StopIteration:
                stdin.close()
                return
            if isinstance(first, str) and first.endswith('\n'):
                stdin.write(first)
                stdin.writelines(it)
            else:
                stdin.write("%s\n"%first)
                stdin.writelines(itertools.imap(lambda x:"%s\n"%x, it))
            stdin.close()
        threading.Thread(target=read, args=[p.stdin]).start()
        return p.stdout

class MappedValuesRDD(MappedRDD):
    @property
    def partitioner(self):
        return self.prev.partitioner

    def compute(self, split):
        func = self.func
        if self.err < 1e-8:
            return itertools.imap(lambda (k,v):(k,func(v)),
                self.prev.iterator(split))
        return self._compute_with_error(split)

    def _compute_with_error(self, split):
        func = self.func
        total, err = 0, 0
        for k,v in self.prev.iterator(split):
            try:
                total += 1
                yield (k,func(v))
            except Exception, e:
                logger.warning("ignored record %r: %s", v, e)
                err += 1
                if total > 100 and err > total * self.err * 10:
                    raise Exception("too many error occured: %s" % (float(err)/total))

        if err > total * self.err:
            raise Exception("too many error occured: %s" % (float(err)/total))

class FlatMappedValuesRDD(MappedValuesRDD):
    def compute(self, split):
        total, err = 0, 0
        for k,v in self.prev.iterator(split):
            try:
                total += 1
                for vv in self.func(v):
                    yield k,vv
            except Exception, e:
                logger.warning("ignored record %r: %s", v, e)
                err += 1
                if total > 100 and err > total * self.err * 10:
                    raise Exception("too many error occured: %s" % (float(err)/total))

        if err > total * self.err:
            raise Exception("too many error occured: %s" % (float(err)/total))

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
        self.shuffleId = self.ctx.newShuffleId()
        self.dependencies = [ShuffleDependency(self.shuffleId,
                parent, aggregator, part)]
        self.name = '<ShuffledRDD %s>' % self.parent

    def __len__(self):
        return self._partitioner.numPartitions

    def __repr__(self):
        return self.name

    @cached
    def __getstate__(self):
        d = RDD.__getstate__(self)
        d.pop('parent', None)
        return d

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
        fetcher.fetch(self.shuffleId, split.index, mergePair)
        return combiners.iteritems()

class CartesianSplit(Split):
    def __init__(self, idx, s1, s2):
        self.index = idx
        self.s1 = s1
        self.s2 = s2

class CartesianRDD(RDD):
    def __init__(self, rdd1, rdd2):
        RDD.__init__(self, rdd1.ctx)
        self.rdd1 = rdd1
        self.rdd2 = rdd2
        self.numSplitsInRdd2 = n = len(rdd2)
        self._splits = [CartesianSplit(s1.index*n+s2.index, s1, s2)
            for s1 in rdd1.splits for s2 in rdd2.splits]
        self.dependencies = [CartesianDependency(rdd1, True, n),
                             CartesianDependency(rdd2, False, n)]

    def __len__(self):
        return len(self.rdd1) * len(self.rdd2)

    def __repr__(self):
        return '<cartesian %s and %s>' % (self.rdd1, self.rdd2)

    def preferredLocations(self, split):
        return self.rdd1.preferredLocations(split.s1) + self.rdd2.preferredLocations(split.s2)

    def compute(self, split):
        b = None
        for i in self.rdd1.iterator(split.s1):
            if b is None:
                b = []
                for j in self.rdd2.iterator(split.s2):
                    yield (i, j)
                    b.append(j)
            else:
                for j in b:
                    yield (i,j)

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
        self.len = len(rdds)
        self.aggregator = CoGroupAggregator()
        self.partitioner = partitioner
        self.dependencies = dep = [rdd.partitioner == partitioner
                and OneToOneDependency(rdd)
                or ShuffleDependency(self.ctx.newShuffleId(), 
                    rdd, self.aggregator, partitioner)
                for i,rdd in enumerate(rdds)]
        self._splits = [CoGroupSplit(j, 
                          [isinstance(dep[i],ShuffleDependency)
                            and ShuffleCoGroupSplitDep(dep[i].shuffleId)
                            or NarrowCoGroupSplitDep(r, r.splits[j]) 
                            for i,r in enumerate(rdds)])
                        for j in range(partitioner.numPartitions)]
        self.name = '<CoGrouped of %s>' % (','.join(str(rdd) for rdd in rdds))

    def __len__(self):
        return self.partitioner.numPartitions

    def __repr__(self):
        return self.name

    def preferredLocations(self, split): 
        return sum([dep.rdd.preferredLocations(dep.split) for dep in split.deps 
                if isinstance(dep, NarrowCoGroupSplitDep)], [])

    def preferredLocations(self, split): 
        return sum([dep.rdd.preferredLocations(dep.split) for dep in split.deps 
                if isinstance(dep, NarrowCoGroupSplitDep)], [])

    def compute(self, split):
        m = {}
        def getSeq(k):
            return m.setdefault(k, tuple([[] for i in range(self.len)]))
        for i,dep in enumerate(split.deps):
            if isinstance(dep, NarrowCoGroupSplitDep):
                for k,v in dep.rdd.iterator(dep.split):
                    getSeq(k)[i].append(v)
            elif isinstance(dep, ShuffleCoGroupSplitDep):
                def mergePair(k, vs):
                    getSeq(k)[i].extend(vs)
                env.shuffleFetcher.fetch(dep.shuffleId, split.index, mergePair)
        return m.iteritems()
        

class SampleRDD(RDD):
    def __init__(self, prev, frac, withReplacement, seed):
        RDD.__init__(self, prev.ctx)
        self.prev = prev
        self.frac = frac
        self.withReplacement = withReplacement
        self.seed = seed
        self.dependencies = [OneToOneDependency(prev)]

    def __len__(self):
        return len(self.prev)

    @property
    def splits(self):
        return self.prev.splits

    def __repr__(self):
        return '<SampleRDD(%s) of %s>' % (self.frac, self.prev)

    def preferredLocations(self, split):
        return self.prev.preferredLocations(split)

    def compute(self, split):
        rd = random.Random(self.seed + split.index)
        if self.withReplacement:
            olddata = list(self.prev.iterator(split))
            sampleSize = int(math.ceil(len(olddata) * self.frac))
            for i in xrange(sampleSize):
                yield rd.choice(olddata)
        else:
            for i in self.prev.iterator(split):
                if rd.random() <= self.frac:
                    yield i


class UnionSplit:
    def __init__(self, idx, rdd, split):
        self.index = idx
        self.rdd = rdd
        self.split = split

class UnionRDD(RDD):
    def __init__(self, ctx, rdds):
        RDD.__init__(self, ctx)
        self._splits = [UnionSplit(0, rdd, split) 
                for rdd in rdds for split in rdd.splits]
        for i,split in enumerate(self._splits):
            split.index = i
        self.dependencies = []
        pos = 0
        for rdd in rdds:
            self.dependencies.append(RangeDependency(rdd, 0, pos, len(rdd)))
            pos += len(rdd)
        self.name = '<union %d %s>' % (len(rdds), ','.join(str(rdd) for rdd in rdds[:2]))

    def __repr__(self):
        return self.name

    def preferredLocations(self, split):
        return split.rdd.preferredLocations(split.split)

    def compute(self, split):
        return split.rdd.iterator(split.split)

class SliceRDD(RDD):
    def __init__(self, rdd, i, j):
        RDD.__init__(self, rdd.ctx)
        self.rdd = rdd
        if j > len(rdd):
            j = len(rdd)
        self.i = i
        self.j = j
        self._splits = rdd.splits[i:j]
        self.dependencies = [RangeDependency(rdd, i, 0, j-i)]

    def __len__(self):
        return self.j - self.i

    def __repr__(self):
        return '<SliceRDD [%d:%d] of %s>' % (self.i, self.j, self.rdd)

    def preferredLocations(self, split):
        return self.rdd.preferredLocations(split)
    
    def compute(self, split):
        return self.rdd.iterator(split)


class MultiSplit(Split):
    def __init__(self, index, splits):
        self.index = index
        self.splits = splits

class MergedRDD(RDD):
    def __init__(self, rdd, splitSize=None, numSplits=None):
        RDD.__init__(self, rdd.ctx)
        if splitSize is None:
            splitSize = (len(rdd) + numSplits - 1) / numSplits
        numSplits = (len(rdd) + splitSize - 1) / splitSize
        self.rdd = rdd
        self.splitSize = splitSize
        self.numSplits = numSplits

        splits = rdd.splits
        self._splits = [MultiSplit(i, splits[i*splitSize:(i+1)*splitSize])
               for i in range(numSplits)]
        self.dependencies = [OneToRangeDependency(rdd, splitSize, len(rdd))]

    def __len__(self):
        return self.numSplits 

    def __repr__(self):
        return '<MergedRDD %s:1 of %s>' % (self.splitSize, self.rdd)

    def preferredLocations(self, split):
        return sum([self.rdd.preferredLocations(sp) for sp in split.splits], [])

    def compute(self, split):
        return itertools.chain.from_iterable(self.rdd.iterator(sp) for sp in split.splits)


class ZippedRDD(RDD):
    def __init__(self, ctx, rdds):
        assert len(set([len(rdd) for rdd in rdds])) == 1, 'rdds must have the same length'
        RDD.__init__(self, ctx)
        self.rdds = rdds
        self._splits = [MultiSplit(i, splits) 
                for i, splits in enumerate(zip(*[rdd.splits for rdd in rdds]))]
        self.dependencies = [OneToOneDependency(rdd) for rdd in rdds]

    def __len__(self):
        return len(self.rdds[0])

    def __repr__(self):
        return '<Zipped %s>' % (','.join(str(rdd) for rdd in self.rdds))

    def preferredLocations(self, split):
        return sum([rdd.preferredLocations(sp) 
            for rdd,sp in zip(self.rdds, split.splits)], [])

    def compute(self, split):
        return itertools.izip(*[rdd.iterator(sp) 
            for rdd, sp in zip(self.rdds, split.splits)])


class CSVReaderRDD(RDD):
    def __init__(self, rdd, dialect='excel'):
        RDD.__init__(self, rdd.ctx)
        self.rdd = rdd
        self.dialect = dialect
        self.dependencies = [OneToOneDependency(rdd)]
    
    def __len__(self):
        return len(self.rdd)

    def __repr__(self):
        return '<CSVReaderRDD %s of %s>' % (self.dialect, self.rdd)

    @property
    def splits(self):
        return self.rdd.splits

    def __repr__(self):
        return '<CSVReaderRDD %s of %s>' % (self.dialect, self.rdd)

    def preferredLocations(self, split):
        return self.rdd.preferredLocations(split)
    
    def compute(self, split):
        return csv.reader(self.rdd.iterator(split), self.dialect)


class ParallelCollectionSplit:
    def __init__(self, index, values):
        self.index = index
        self.values = values

class ParallelCollection(RDD):
    def __init__(self, ctx, data, numSlices):
        RDD.__init__(self, ctx)
        self.size = len(data)
        slices = self.slice(data, max(1, min(self.size, numSlices)))
        self._splits = [ParallelCollectionSplit(i, slices[i]) 
                for i in range(len(slices))]
        self.dependencies = []

    def __repr__(self):
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
            last = data[m-1]
            step = (last - first) / (m-1)
            nstep = step * n
            slices = [xrange(first+i*nstep, first+(i+1)*nstep, step)
                for i in range(numSlices-1)]
            slices.append(xrange(first+(numSlices-1)*nstep, 
                min(last+step, first+numSlices*nstep), step))
            return slices
        if not isinstance(data, list):
            data = list(data)
        return [data[i*n : i*n+n] for i in range(numSlices)]


class TextFileRDD(RDD):

    DEFAULT_SPLIT_SIZE = 64*1024*1024

    def __init__(self, ctx, path, numSplits=None, splitSize=None):
        RDD.__init__(self, ctx)
        self.path = path
        size = os.path.getsize(path)
        if splitSize is None:
            if numSplits is None:
                splitSize = self.DEFAULT_SPLIT_SIZE
            else:
                splitSize = size / numSplits
        n = size / splitSize
        if size % splitSize > 0:
            n += 1
        self.splitSize = splitSize
        self.len = n

    def __len__(self):
        return self.len

    @property
    def splits(self):
        return [Split(i) for i in range(self.len)]

    def __repr__(self):
        return '<TextFileRDD %s>' % self.path

    def compute(self, split):
        if len(self) == 1 and split.index == 0:
            return open(self.path, 'r', 4096 * 1024)
        return self.read(split)

    def read(self, split):
        start = split.index * self.splitSize
        end = start + self.splitSize

        f = open(self.path, 'r', 4096 * 1024)
        if start > 0:
            f.seek(start-1)
            byte = f.read(1)
            while byte != '\n':
                byte = f.read(1)
                start += 1

        for line in f:
            if start >= end: break
            start += len(line)
            yield line
        f.close()


class BZip2FileRDD(TextFileRDD):
    
    DEFAULT_SPLIT_SIZE = 10*1024*1024
    MAX_BLOCK_SIZE = 9000

    def __init__(self, ctx, path, numSplits=None, splitSize=None):
        TextFileRDD.__init__(self, ctx, path, numSplits, splitSize)
        self.magic = open(path).read(10)

    def __repr__(self):
        return '<BZip2FileRDD %s>' % self.path

    def compute(self, split):
        f = open(self.path, 'r', 4096 * 1024)
        f.seek(split.index * self.splitSize)
        d = f.read(self.splitSize)
        fp = d.find(self.magic)
        if fp > 0:
            d = d[fp:] # drop end of last block

        # real all the block    
        nd = f.read(self.MAX_BLOCK_SIZE)
        np = nd.find(self.magic)
        while nd and np < 0:
            t = f.read(len(nd))
            if not t: break
            nd += t
            np = nd.find(self.magic)
        d += nd[:np] if np >= 0 else nd
        f.close()

        last_line = None if split.index > 0 else ''
        while d:
            try:
                io = cStringIO.StringIO(bz2.decompress(d))
            except IOError, e:
                #bad position, skip it
                pass
            else:
                if last_line is None:
                    io.readline() # skip the first line
                    last_line = ''
                else:
                    last_line += io.readline()
                    if last_line.endswith('\n'):
                        yield last_line
                        last_line = ''
                
                for line in io:
                    if line.endswith('\n'): # drop last line
                        yield line
                    else:
                        last_line = line

            np = d.find(self.magic, len(self.magic))
            if np <= 0:
                break
            d = d[np:]


class MFSTextFileRDD(RDD):
    def __init__(self, ctx, path, master, numSplits=None, splitSize=None):
        RDD.__init__(self, ctx)
        self.path = path
        self.file = moosefs.mfsopen(path, master)
        size = self.file.length
        if splitSize is None:
            if numSplits is None:
                splitSize = 64*1024*1024
            else:
                splitSize = size / numSplits
        n = size / splitSize
        if size % splitSize > 0:
            n += 1
        self.splitSize = splitSize
        self.len = n

    def __len__(self):
        return self.len
    
    @property
    def splits(self):
        return [Split(i) for i in range(self.len)]

    def preferredLocations(self, split):
        return self.file.locs(split.index)

    def __repr__(self):
        return '<TextFileRDD %s>' % self.path

    def compute(self, split):
        start = split.index * self.splitSize
        end = start + self.splitSize
        MAX_RECORD_LENGTH = 10240

        f = moosefs.ReadableFile(self.file)
        if start > 0:
            f.seek(start-1, end + MAX_RECORD_LENGTH)
            byte = f.read(1)
            while byte and byte != '\n':
                byte = f.read(1)
                start += 1
        else:
            f.seek(0, end + MAX_RECORD_LENGTH)

        for line in f:
            if not line: break
            start += len(line)
            yield line
            if start >= end: break
        f.close()


class OutputTextFileRDD(RDD):
    def __init__(self, rdd, path, ext='', overwrite=False):
        if os.path.exists(path):
            if not os.path.isdir(path):
                raise Exception("output must be dir")
            if overwrite:
                shutil.rmtree(path)
                os.makedirs(path)
        else:
            os.makedirs(path)

        RDD.__init__(self, rdd.ctx)
        self.rdd = rdd
        self.path = os.path.abspath(path)
        if ext and not ext.startswith('.'):
            ext = '.' + ext
        self.ext = ext
        self.overwrite = overwrite
        self.dependencies = [OneToOneDependency(rdd)]

    def __len__(self):
        return len(self.rdd)

    def __repr__(self):
        return '<OutputTextFileRDD %s>' % self.path

    @property
    def splits(self):
        return self.rdd.splits

    def preferredLocations(self, split):
        return self.rdd.preferredLocations(split)
    
    def compute(self, split):
        path = os.path.join(self.path, 
            "%04d%s" % (split.index, self.ext))
        if os.path.exists(path) and not self.overwrite:
            return
        tpath = os.path.join(self.path, 
            ".%04d%s.%s.%d.tmp" % (split.index, self.ext, 
            socket.gethostname(), os.getpid()))
        try:
            f = open(tpath,'w', 4096 * 1024 * 16)
        except IOError:
            time.sleep(1) # there are dir cache in mfs for 1 sec
            f = open(tpath,'w', 4096 * 1024 * 16)
        
        have_data = self.writedata(f, self.rdd.iterator(split))
        f.close()
        if have_data and not os.path.exists(path):
            os.rename(tpath, path)
            yield path
        else:
            os.remove(tpath)

    def writedata(self, f, lines):
        empty = True
        for line in lines:
            f.write(line)
            if not line.endswith('\n'):
                f.write('\n')
            empty = False
        return not empty

class MultiOutputTextFileRDD(OutputTextFileRDD):
    def __repr__(self):
        return '<MultiOutputTextFileRDD %s>' % self.path
   
    def compute(self, split):
        files, paths = {}, {}
        def get_file(key):
            f = files.get(key)
            if f is None:
                dpath = os.path.join(self.path, str(key))
                if not os.path.exists(dpath):
                    try: os.mkdir(dpath)
                    except: pass
                tpath = os.path.join(dpath, 
                    ".%04d%s.%s.%d.tmp" % (split.index, self.ext, 
                    socket.gethostname(), os.getpid()))
                try:
                    f = open(tpath,'w', 4096 * 1024 * 16)
                except IOError:
                    time.sleep(1) # there are dir cache in mfs for 1 sec
                    f = open(tpath,'w', 4096 * 1024 * 16)
                files[key] = f
                paths[key] = tpath
            return f
        
        for k, v in self.rdd.iterator(split):
            f = get_file(k)
            f.write(v)
            if not v.endswith('\n'):
                f.write('\n')

        for k in files:
            files[k].close()
            path = os.path.join(self.path, str(k), "%04d%s" % (split.index, self.ext))
            if not os.path.exists(path):
                os.rename(paths[k], path)
            else:
                os.remove(paths[k])
            yield path


class OutputCSVFileRDD(OutputTextFileRDD):
    def __init__(self, rdd, path, overwrite):
        OutputTextFileRDD.__init__(self, rdd, path, '.csv', overwrite)

    def __repr__(self):
        return '<OutputCSVFileRDD %s>' % self.path

    def writedata(self, f, rows):
        writer = csv.writer(f)
        empty = True
        for row in rows:
            if not isinstance(row, (tuple, list)):
                row = (row,)
            writer.writerow(row)
            empty = False
        return not empty 

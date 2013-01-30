import sys
import os, os.path
import time
import socket
import csv
from cStringIO import StringIO
import itertools
import operator
import math
import cPickle
import random
import bz2
import gzip
import zlib
import logging
from copy import copy
import shutil
import heapq
import struct

from serialize import load_func, dump_func
from dependency import *
from util import ilen, spawn
import shuffle
from env import env
import moosefs

logger = logging.getLogger("rdd")

class Split:
    def __init__(self, idx):
        self.index = idx

def cached(func):
    def getstate(self):
        d = getattr(self, '_pickle_cache', None)
        if d is None:
            d = func(self)
            self._pickle_cache = d
        return d
    return getstate

class RDD(object):
    def __init__(self, ctx):
        self.ctx = ctx
        self.id = RDD.newId()
        self._splits = []
        self.dependencies = []
        self.aggregator = None
        self._partitioner = None
        self.shouldCache = False
        self.snapshot_path = None
        ctx.init()
        self.err = ctx.options.err
        self.mem = ctx.options.mem

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

    def mergeSplit(self, splitSize=None, numSplits=None):
        return MergedRDD(self, splitSize, numSplits)

    @property
    def splits(self):
        return self._splits

    def compute(self, split):
        raise NotImplementedError

    @property
    def partitioner(self):
        return self._partitioner

    def _preferredLocations(self, split):
        return []

    def cache(self):
        #self.shouldCache = True
        return self

    def preferredLocations(self, split):
        if self.shouldCache:
            locs = env.cacheTracker.getCachedLocs(self.id)
            if locs:
                return locs[split.index]
        return self._preferredLocations(split)

    def snapshot(self, path=None):
        if path is None:
            path = self.ctx.options.snapshot_dir
        if path:
            ident = '%d_%x' % (self.id, hash(str(self)))
            path = os.path.join(path, ident)
            if not os.path.exists(path):
                try: os.makedirs(path)
                except OSError: pass
            self.snapshot_path = path
        return self

    def iterator(self, split):
        if self.snapshot_path:
            p = os.path.join(self.snapshot_path, str(split.index))
            if os.path.exists(p):
                v = cPickle.loads(open(p).read())
            else:
                v = list(self.compute(split))
                with open(p, 'w') as f:
                    f.write(cPickle.dumps(v))
            return v

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

    def sort(self, key=lambda x:x, reverse=False, numSplits=None):
        if not len(self):
            return self
        if numSplits is None:
            numSplits = min(self.ctx.defaultMinSplits, len(self))
        n = numSplits * 10 / len(self)
        samples = self.glom().flatMap(lambda x:itertools.islice(x, n)).map(key).collect()
        keys = sorted(samples, reverse=reverse)[5::10][:numSplits-1]
        parter = RangePartitioner(keys, reverse=reverse)
        aggr = MergeAggregator()
        parted = ShuffledRDD(self.map(lambda x:(key(x),x)), aggr, parter).flatMap(lambda (x,y):y)
        return parted.glom().flatMap(lambda x:sorted(x, key=key, reverse=reverse))

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
    mapPartition = mapPartitions

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
        
        return reduce(f, itertools.chain.from_iterable(self.ctx.runJob(self, reducePartition)))

    def uniq(self, numSplits=None, taskMemory=None):
        g = self.map(lambda x:(x,None)).reduceByKey(lambda x,y:None, numSplits, taskMemory)
        return g.map(lambda (x,y):x)

    def top(self, n=10, key=None, reverse=False):
        if reverse: 
            def topk(it):
                return heapq.nsmallest(n, it, key)
        else:
            def topk(it):
                return heapq.nlargest(n, it, key)
        return heapq.nlargest(n, sum(self.ctx.runJob(self, topk), []), key)

    def hot(self, n=10, numSplits=None, taskMemory=None):
        st = self.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y, numSplits, taskMemory)
        return st.top(n, key=lambda x:x[1])

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

    def saveAsTextFile(self, path, ext='', overwrite=True, compress=False):
        return OutputTextFileRDD(self, path, ext, overwrite, compress=compress).collect()

    def saveAsTextFileByKey(self, path, ext='', overwrite=True, compress=False):
        return MultiOutputTextFileRDD(self, path, ext, overwrite, compress=compress).collect()

    def saveAsCSVFile(self, path, overwrite=True):
        return OutputCSVFileRDD(self, path, overwrite).collect()

    def saveAsBinaryFile(self, path, fmt, overwrite=True):
        return OutputBinaryFileRDD(self, path, fmt, overwrite).collect()

    def saveAsTableFile(self, path, overwrite=True):
        return OutputTableFileRDD(self, path, overwrite).collect()

    # Extra functions for (K,V) pairs RDD
    def reduceByKeyToDriver(self, func):
        def mergeMaps(m1, m2):
            for k,v in m2.iteritems():
                m1[k]=func(m1[k], v) if k in m1 else v
            return m1
        return self.map(lambda (x,y):{x:y}).reduce(mergeMaps)

    def combineByKey(self, aggregator, splits=None, taskMemory=None):
        if splits is None:
            splits = min(self.ctx.defaultMinSplits, len(self))
        if type(splits) is int:
            splits = HashPartitioner(splits)
        return ShuffledRDD(self, aggregator, splits, taskMemory)

    def reduceByKey(self, func, numSplits=None, taskMemory=None):
        aggregator = Aggregator(lambda x:x, func, func)
        return self.combineByKey(aggregator, numSplits, taskMemory)

    def groupByKey(self, numSplits=None, taskMemory=None):
        createCombiner = lambda x: [x]
        mergeValue = lambda x,y:x.append(y) or x
        mergeCombiners = lambda x,y: x.extend(y) or x
        aggregator = Aggregator(createCombiner, mergeValue, mergeCombiners)
        return self.combineByKey(aggregator, numSplits, taskMemory)

    def partitionByKey(self, numSplits=None, taskMemory=None):
        return self.groupByKey(numSplits, taskMemory).flatMapValue(lambda x: x)

    def innerJoin(self, other):
        o_b = self.ctx.broadcast(other.collectAsMap())
        r = self.filter(lambda (k,v):k in o_b.value).map(lambda (k,v):(k,(v,o_b.value[k])))
        r.mem += (o_b.bytes * 10) >> 20 # memory used by broadcast obj
        return r

    def join(self, other, numSplits=None, taskMemory=None):
        return self._join(other, (), numSplits, taskMemory)

    def leftOuterJoin(self, other, numSplits=None, taskMemory=None):
        return self._join(other, (1,), numSplits, taskMemory)

    def rightOuterJoin(self, other, numSplits=None, taskMemory=None):
        return self._join(other, (2,), numSplits, taskMemory)
    
    def outerJoin(self, other, numSplits=None, taskMemory=None):
        return self._join(other, (1,2), numSplits, taskMemory)

    def _join(self, other, keeps, numSplits=None, taskMemory=None):
        vs = self.map(lambda (k,v): (k,(1,v)))
        ws = other.map(lambda (k,v): (k,(2,v)))
        def dispatch((k,seq)):
            vbuf, wbuf = [], []
            for n,v in seq:
                if n == 1:
                    vbuf.append(v)
                elif n == 2:
                    wbuf.append(v)
            if not vbuf and 2 in keeps:
                vbuf.append(None)
            if not wbuf and 1 in keeps:
                wbuf.append(None)
            for vv in vbuf:
                for ww in wbuf:
                    yield (k, (vv, ww))
        return vs.union(ws).groupByKey(numSplits, taskMemory).flatMap(dispatch)
    
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
        taskMemory = kw.get('taskMemory')
        part = self.partitioner or HashPartitioner(numSplits)
        return CoGroupedRDD([self]+list(others), part, taskMemory)

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

    def asTable(self, fields):
        from table import TableRDD
        return TableRDD(self, fields)


class DerivedRDD(RDD):
    def __init__(self, rdd):
        RDD.__init__(self, rdd.ctx)
        self.prev = rdd
        self.mem = max(self.mem, rdd.mem)
        self.dependencies = [OneToOneDependency(rdd)]

    def __len__(self):
        return len(self.prev)

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.prev)
    
    @property
    def splits(self):
        return self.prev.splits

    def _preferredLocations(self, split):
        return self.prev.preferredLocations(split)


class MappedRDD(DerivedRDD):
    def __init__(self, prev, func=lambda x:x):
        DerivedRDD.__init__(self, prev)
        self.func = func
    
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
                    raise Exception("too many error occured: %s, %s" % ((float(err)/total), e))

        if err > total * self.err:
            raise Exception("too many error occured: %s, %s" % ((float(err)/total), e))


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
           
class GlommedRDD(DerivedRDD):
    def compute(self, split):
        yield self.prev.iterator(split)

class MapPartitionsRDD(MappedRDD):
    def compute(self, split):
        return self.func(self.prev.iterator(split))

class PipedRDD(DerivedRDD):
    def __init__(self, prev, command, quiet=False, shell=False):
        DerivedRDD.__init__(self, prev)
        self.command = command
        self.quiet = quiet
        self.shell = shell

    def __repr__(self):
        return '<PipedRDD %s %s>' % (' '.join(self.command), self.prev)

    def compute(self, split):
        import subprocess
        devnull = open(os.devnull, 'w')
        p = subprocess.Popen(self.command, stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, 
                stderr=self.quiet and devnull or sys.stderr,
                shell=self.shell)
        def read(stdin):
            try:
                it = self.prev.iterator(split)
                # fetch the first item
                for first in it:
                    break
                else:
                    return
                if isinstance(first, str) and first.endswith('\n'):
                    stdin.write(first)
                    stdin.writelines(it)
                else:
                    stdin.write("%s\n"%first)
                    stdin.writelines(itertools.imap(lambda x:"%s\n"%x, it))
            finally:
                stdin.close()
                devnull.close()
        spawn(read, p.stdin)
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
    def __init__(self, parent, aggregator, part, taskMemory=None):
        RDD.__init__(self, parent.ctx)
        self.parent = parent
        self.numParts = len(parent)
        self.aggregator = aggregator
        self._partitioner = part
        if taskMemory:
            self.mem = taskMemory
        self._splits = [ShuffledRDDSplit(i) for i in range(part.numPartitions)]
        self.shuffleId = self.ctx.newShuffleId()
        self.dependencies = [ShuffleDependency(self.shuffleId,
                parent, aggregator, part)]
        self.name = '<ShuffledRDD %s>' % self.parent

    def __len__(self):
        return self.partitioner.numPartitions

    def __repr__(self):
        return self.name

    @cached
    def __getstate__(self):
        d = RDD.__getstate__(self)
        d.pop('parent', None)
        return d

    def compute(self, split):
        merger = shuffle.Merger(self.numParts, self.aggregator.mergeCombiners) 
        fetcher = env.shuffleFetcher
        fetcher.fetch(self.shuffleId, split.index, merger.merge)
        return merger


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
        self.mem = max(rdd1.mem, rdd2.mem) * 1.5
        self.numSplitsInRdd2 = n = len(rdd2)
        self._splits = [CartesianSplit(s1.index*n+s2.index, s1, s2)
            for s1 in rdd1.splits for s2 in rdd2.splits]
        self.dependencies = [CartesianDependency(rdd1, True, n),
                             CartesianDependency(rdd2, False, n)]

    def __len__(self):
        return len(self.rdd1) * len(self.rdd2)

    def __repr__(self):
        return '<cartesian %s and %s>' % (self.rdd1, self.rdd2)

    def _preferredLocations(self, split):
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
    def __init__(self, rdds, partitioner, taskMemory=None):
        RDD.__init__(self, rdds[0].ctx)
        self.len = len(rdds)
        if taskMemory:
            self.mem = taskMemory
        self.aggregator = CoGroupAggregator()
        self._partitioner = partitioner
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
        self.name = ('<CoGrouped of %s>' % (','.join(str(rdd) for rdd in rdds)))[:80]

    def __len__(self):
        return self.partitioner.numPartitions

    def __repr__(self):
        return self.name

    def _preferredLocations(self, split): 
        return sum([dep.rdd.preferredLocations(dep.split) for dep in split.deps 
                if isinstance(dep, NarrowCoGroupSplitDep)], [])

    def compute(self, split):
        m = shuffle.CoGroupMerger(self.len)
        for i,dep in enumerate(split.deps):
            if isinstance(dep, NarrowCoGroupSplitDep):
                m.append(i, dep.rdd.iterator(dep.split))
            elif isinstance(dep, ShuffleCoGroupSplitDep):
                def merge(items):
                    m.extend(i, items)
                env.shuffleFetcher.fetch(dep.shuffleId, split.index, merge)
        return m
        

class SampleRDD(DerivedRDD):
    def __init__(self, prev, frac, withReplacement, seed):
        DerivedRDD.__init__(self, prev)
        self.frac = frac
        self.withReplacement = withReplacement
        self.seed = seed

    def __repr__(self):
        return '<SampleRDD(%s) of %s>' % (self.frac, self.prev)

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
        self.mem = max(r.mem for r in rdds) if rdds else self.mem
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

    def _preferredLocations(self, split):
        return split.rdd.preferredLocations(split.split)

    def compute(self, split):
        return split.rdd.iterator(split.split)

class SliceRDD(RDD):
    def __init__(self, rdd, i, j):
        RDD.__init__(self, rdd.ctx)
        self.rdd = rdd
        self.mem = rdd.mem
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

    def _preferredLocations(self, split):
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
        self.mem = rdd.mem
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

    def _preferredLocations(self, split):
        return sum([self.rdd.preferredLocations(sp) for sp in split.splits], [])

    def compute(self, split):
        return itertools.chain.from_iterable(self.rdd.iterator(sp) for sp in split.splits)


class ZippedRDD(RDD):
    def __init__(self, ctx, rdds):
        assert len(set([len(rdd) for rdd in rdds])) == 1, 'rdds must have the same length'
        RDD.__init__(self, ctx)
        self.rdds = rdds
        self.mem = max(r.mem for r in rdds)
        self._splits = [MultiSplit(i, splits) 
                for i, splits in enumerate(zip(*[rdd.splits for rdd in rdds]))]
        self.dependencies = [OneToOneDependency(rdd) for rdd in rdds]

    def __len__(self):
        return len(self.rdds[0])

    def __repr__(self):
        return '<Zipped %s>' % (','.join(str(rdd) for rdd in self.rdds))

    def _preferredLocations(self, split):
        return sum([rdd.preferredLocations(sp) 
            for rdd,sp in zip(self.rdds, split.splits)], [])

    def compute(self, split):
        return itertools.izip(*[rdd.iterator(sp) 
            for rdd, sp in zip(self.rdds, split.splits)])


class CSVReaderRDD(DerivedRDD):
    def __init__(self, prev, dialect='excel'):
        DerivedRDD.__init__(self, prev)
        self.dialect = dialect
    
    def __repr__(self):
        return '<CSVReaderRDD %s of %s>' % (self.dialect, self.prev)

    def compute(self, split):
        return csv.reader(self.prev.iterator(split), self.dialect)


class ParallelCollectionSplit:
    def __init__(self, index, values):
        self.index = index
        self.values = values

class ParallelCollection(RDD):
    def __init__(self, ctx, data, numSlices, taskMemory=None):
        RDD.__init__(self, ctx)
        self.size = len(data)
        if taskMemory:
            self.mem = taskMemory
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


def open_mfs_file(path):
    rpath = os.path.realpath(path)
    if rpath.startswith('/mfs/'):
        return moosefs.mfsopen(rpath[4:], 'mfsmaster')
    if rpath.startswith('/home2/'):
        return moosefs.mfsopen(rpath[6:], 'mfsmaster2')

class TextFileRDD(RDD):

    DEFAULT_SPLIT_SIZE = 64*1024*1024

    def __init__(self, ctx, path, numSplits=None, splitSize=None):
        RDD.__init__(self, ctx)
        self.path = path
        self.fileinfo = open_mfs_file(path)
        self.size = size = self.fileinfo.length if self.fileinfo else os.path.getsize(path)
        
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
        self._splits = [Split(i) for i in range(self.len)]

    def __len__(self):
        return self.len

    def __repr__(self):
        return '<%s %s>' % (self.__class__, self.path)

    def _preferredLocations(self, split):
        if not self.fileinfo:
            return []

        if self.splitSize != moosefs.CHUNKSIZE:
            start = self.splitSize * split.index / moosefs.CHUNKSIZE
            end = (self.splitSize * (split.index + 1) + moosefs.CHUNKSIZE - 1) / moosefs.CHUNKSIZE
            return sum((self.fileinfo.locs(i) for i in range(start, end)), [])
        else:
            return self.fileinfo.locs(split.index)

    def open_file(self):
        if self.fileinfo:
            return moosefs.ReadableFile(self.fileinfo)
        else:
            return open(self.path, 'r', 4096 * 1024)

    def compute(self, split):
        f = self.open_file() 
        if len(self) == 1 and split.index == 0:
            return f
        
        start = split.index * self.splitSize
        end = start + self.splitSize
        if start > 0:
            f.seek(start-1)
            byte = f.read(1)
            while byte != '\n':
                byte = f.read(1)
                start += 1

        if self.fileinfo:
            # cut by end
            if end < self.size:
                f.seek(end-1)
                while f.read(1) not in ('', '\n'):
                    end += 1
                f.length = end 
            f.seek(start)
            return f

        return self.read(f, start, end)

    def read(self, f, start, end):
        for line in f:
            start += len(line)
            yield line
            if start >= end: break
        f.close()

class GZipFileRDD(TextFileRDD):
    "the gziped file must be seekable, compressed by pigz -i"    
    BLOCK_SIZE = 64 << 10
    DEFAULT_SPLIT_SIZE = 32 << 20

    def __init__(self, ctx, path, splitSize=None):
        TextFileRDD.__init__(self, ctx, path, None, splitSize)

    def find_block(self, f, pos):
        f.seek(pos)
        block = f.read(32*1024)
        if len(block) < 4:
            f.seek(0, 2)
            return f.tell() # EOF
        ENDING = '\x00\x00\xff\xff'
        while True:
            p = block.find(ENDING)
            while p < 0:
                pos += max(len(block) - 3, 0)
                block = block[-3:] + f.read(32<<10)
                if len(block) < 4:
                    return pos + 3 # EOF
                p = block.find(ENDING)
            pos += p + 4
            block = block[p+4:]
            if len(block) < 4096:
                block += f.read(4096)
                if not block:
                    return pos # EOF
            try:
                if zlib.decompressobj(-zlib.MAX_WBITS).decompress(block):
                    return pos # FOUND 
            except Exception, e:
                pass

    def compute(self, split):
        f = self.open_file()
        last_line = ''
        if split.index == 0:
            zf = gzip.GzipFile(fileobj=f)
            zf._read_gzip_header()
            start = f.tell()
        else:
            start = self.find_block(f, split.index * self.splitSize)
            if start >= split.index * self.splitSize + self.splitSize:
                return
            for i in xrange(1, 100):
                if start - i * self.BLOCK_SIZE <= 4:
                    break
                last_block = self.find_block(f, start - i * self.BLOCK_SIZE)
                if last_block < start:
                    f.seek(last_block)
                    d = f.read(start - last_block)
                    dz = zlib.decompressobj(-zlib.MAX_WBITS)
                    last_line = dz.decompress(d).split('\n')[-1]
                    if last_line.endswith('\n'):
                        last_line = ''
                    break

        end = self.find_block(f, split.index * self.splitSize + self.splitSize)
        # TODO: speed up
        f.seek(start)
        if self.fileinfo:
            f.length = end
        dz = zlib.decompressobj(-zlib.MAX_WBITS)
        skip_first = False
        while start < end:
            d = f.read(min(64<<10, end-start))
            start += len(d)
            if not d: break

            try:
                io = StringIO(dz.decompress(d))
            except Exception, e:
                if self.err < 1e-6:
                    raise
                old = start
                start = self.find_block(f, start)
                f.seek(start)
                logger.error("drop corrupted block (%d bytes) in %s",
                        start - old + len(d), self.path)
                skip_first = True
                continue

            last_line += io.readline()
            if skip_first:
                skip_first = False
            elif last_line.endswith('\n'):
                yield last_line
            last_line = ''

            ll = list(io)
            if not ll: continue

            last_line = ll.pop()
            for line in ll:
                yield line
            if last_line.endswith('\n'):
                yield last_line
                last_line = ''

        f.close()


class TableFileRDD(TextFileRDD):

    DEFAULT_SPLIT_SIZE = 32 << 20

    def __init__(self, ctx, path, splitSize=None):
        TextFileRDD.__init__(self, ctx, path, None, splitSize)
    
    def find_magic(self, f, pos, magic):
        f.seek(pos)
        block = f.read(32*1024)
        if len(block) < len(magic):
            return -1
        p = block.find(magic)
        while p < 0:
            pos += len(block) - len(magic) + 1 
            block = block[1 - len(magic):] + f.read(32<<10)
            if len(block) == len(magic) - 1:
                return -1
            p = block.find(magic)
        return pos + p

    def compute(self, split):
        import msgpack
        f = self.open_file()
        magic = f.read(8)
        start = split.index * self.splitSize
        end = (split.index + 1) * self.splitSize
        start = self.find_magic(f, start, magic)
        if start < 0:
            return
        f.seek(start)
        hdr_size = 12
        while start < end:
            m = f.read(len(magic))
            if m != magic:
                break
            compressed, count, size = struct.unpack("III", f.read(hdr_size))
            d = f.read(size)
            assert len(d) == size, 'unexpected end'
            if compressed:
                d = zlib.decompress(d)
            for r in msgpack.Unpacker(StringIO(d)):
                yield r
            start += len(magic) + hdr_size + size
        f.close()


class BZip2FileRDD(TextFileRDD):
    
    DEFAULT_SPLIT_SIZE = 32*1024*1024
    BLOCK_SIZE = 9000

    def __init__(self, ctx, path, numSplits=None, splitSize=None):
        TextFileRDD.__init__(self, ctx, path, numSplits, splitSize)

    def compute(self, split):
        f = self.open_file()
        magic = f.read(10)
        f.seek(split.index * self.splitSize)
        d = f.read(self.splitSize)
        fp = d.find(magic)
        if fp > 0:
            d = d[fp:] # drop end of last block

        # real all the block    
        nd = f.read(self.BLOCK_SIZE)
        np = nd.find(magic)
        while nd and np < 0:
            t = f.read(len(nd))
            if not t: break
            nd += t
            np = nd.find(magic)
        d += nd[:np] if np >= 0 else nd
        f.close()

        last_line = None if split.index > 0 else ''
        while d:
            try:
                io = StringIO(bz2.decompress(d))
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

            np = d.find(magic, len(magic))
            if np <= 0:
                break
            d = d[np:]


class BinaryFileRDD(TextFileRDD):
    def __init__(self, ctx, path, fmt=None, length=None, numSplits=None, splitSize=None):
        TextFileRDD.__init__(self, ctx, path, numSplits, splitSize)
        self.fmt = fmt
        if fmt:
            length = struct.calcsize(fmt)
        self.length = length
        assert length, "fmt or length must been provided"

        self.splitSize = max(self.splitSize / length, 1) * length
        n = self.size / self.splitSize
        if self.size % self.splitSize > 0:
            n += 1
        self.len = n

    def __repr__(self):
        return '<BinaryFileRDD(%s) %s>' % (self.fmt, self.path)

    def compute(self, split):
        start = split.index * self.splitSize
        end = min(start + self.splitSize, self.size)

        f = self.open_file()
        f.seek(start)
        rlen = self.length
        fmt = self.fmt
        for i in xrange((end - start) / rlen):
            d = f.read(rlen)
            if len(d) < rlen: break
            if fmt:
                d = struct.unpack(fmt, d)
            yield d


class OutputTextFileRDD(DerivedRDD):
    def __init__(self, rdd, path, ext='', overwrite=False, compress=False):
        if os.path.exists(path):
            if not os.path.isdir(path):
                raise Exception("output must be dir")
            if overwrite:
                for n in os.listdir(path):
                    p = os.path.join(path, n)
                    if os.path.isdir(p):
                        shutil.rmtree(p)
                    else:
                        os.remove(p)
        else:
            os.makedirs(path)

        DerivedRDD.__init__(self, rdd)
        self.path = os.path.abspath(path)
        if ext and not ext.startswith('.'):
            ext = '.' + ext
        if compress and not ext.endswith('gz'):
            ext += '.gz'
        self.ext = ext
        self.overwrite = overwrite
        self.compress = compress

    def __repr__(self):
        return '<%s %s>' % (self.__class__, self.path)

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
        if self.compress:
            have_data = self.write_compress_data(f, self.prev.iterator(split))
        else:    
            have_data = self.writedata(f, self.prev.iterator(split))
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

    def write_compress_data(self, f, lines):
        empty = True
        f = gzip.GzipFile(filename='', mode='w', fileobj=f)
        size = 0
        for line in lines:
            f.write(line)
            if not line.endswith('\n'):
                f.write('\n')
            size += len(line) + 1
            if size >= 256 << 10:
                f.flush()
                f.compress = zlib.compressobj(9, zlib.DEFLATED,
                    -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)
                size = 0
            empty = False
        if not empty:
            f.flush()
        return not empty

class MultiOutputTextFileRDD(OutputTextFileRDD):
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
                if self.compress:
                    f = gzip.GzipFile(filename='', mode='w', fileobj=f)
                files[key] = f
                paths[key] = tpath
            return f
       
        sizes = {}
        for k, v in self.prev.iterator(split):
            f = get_file(k)
            f.write(v)
            if not v.endswith('\n'):
                f.write('\n')
            if self.compress:
                size = sizes.get(k, 0) + len(v)
                if size > 256 << 10: # 128k
                    f.flush()
                    f.compress = zlib.compressobj(9, zlib.DEFLATED,
                        -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)
                    size = 0
                sizes[k] = size

        for k in files:
            if self.compress:
                files[k].flush()
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

    def writedata(self, f, rows):
        writer = csv.writer(f)
        empty = True
        for row in rows:
            if not isinstance(row, (tuple, list)):
                row = (row,)
            writer.writerow(row)
            empty = False
        return not empty 

        
class OutputBinaryFileRDD(OutputTextFileRDD):
    def __init__(self, rdd, path, fmt, overwrite):
        OutputTextFileRDD.__init__(self, rdd, path, '.bin', overwrite)
        self.fmt = fmt

    def writedata(self, f, rows):
        empty = True
        for row in rows:
            if isinstance(row, (tuple, list)):
                f.write(struct.pack(self.fmt, *row))
            else:
                f.write(struct.pack(self.fmt, row))
            empty = False
        return not empty 

class OutputTableFileRDD(OutputTextFileRDD):
    MAGIC = '\x00\xDE\x00\xAD\xFF\xBE\xFF\xEF'
    BLOCK_SIZE = 256 << 10 # 256K

    def __init__(self, rdd, path, overwrite=True, compress=True):
        OutputTextFileRDD.__init__(self, rdd, path, ext='.tab', overwrite=overwrite, compress=False)
        self.compress = compress

    def writedata(self, f, rows):
        import msgpack
        def flush(buf):
            d = buf.getvalue()
            if self.compress:
                d = zlib.compress(d, 1)
            f.write(self.MAGIC)
            f.write(struct.pack("III", self.compress, count, len(d)))
            f.write(d)
        
        count, buf = 0, StringIO()
        for row in rows:
            msgpack.pack(row, buf)
            count += 1
            if buf.tell() > self.BLOCK_SIZE:
                flush(buf)
                count, buf = 0, StringIO()

        if count > 0:
            flush(buf)

        return f.tell() > 0

    write_compress_data = writedata

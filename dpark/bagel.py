import sys
import time
import operator

from dpark.util import get_logger

logger = get_logger(__name__)

class Vertex:
    def __init__(self, id, value, outEdges, active):
        self.id = id
        self.value = value
        self.outEdges = outEdges
        self.active = active
    def __repr__(self):
        return "<Vertex(%s, %s, %s)>" % (self.id, self.value, self.active)

class Edge:
    def __init__(self, target_id, value=0):
        self.target_id = target_id
        self.value = value
    def __repr__(self):
        return '<Edge(%s, %s)>' % (self.target_id, self.value)

class Message:
    def __init__(self, target_id, value):
        self.target_id = target_id
        self.value = value
    def __repr__(self):
        return "<Message(%s, %s)>" % (self.target_id, self.value)


class Combiner(object):
    def createCombiner(self, msg): raise NotImplementedError
    def mergeValue(self, combiner, msg): raise NotImplementedError
    def mergeCombiners(self, a, b): raise NotImplementedError

class Aggregator(object):
    def createAggregator(self, vert): raise NotImplementedError
    def mergeAggregator(self, a, b): raise NotImplementedError

class BasicCombiner(Combiner):
    def __init__(self, op):
        self.op = op
    def createCombiner(self, msg):
        return msg
    def mergeValue(self, combiner, msg):
        return self.op(combiner, msg)
    def mergeCombiners(self, a, b):
        return self.op(a, b)

DefaultValueCombiner = BasicCombiner(operator.add)

class DefaultListCombiner(Combiner):
    def createCombiner(self, msg):
        return [msg]
    def mergeValue(self, combiner, msg):
        return combiner+[msg]
    def mergeCombiners(self, a, b):
        return a + b

class Bagel(object):
    @classmethod
    def run(cls, ctx, verts, msgs, compute,
            combiner=DefaultValueCombiner, aggregator=None,
            maxSuperstep=sys.maxint, numSplits=None, checkpointDir=None):

        superstep = 0
        checkpointDir = checkpointDir or ctx.options.checkpoint_dir

        while superstep < maxSuperstep:
            logger.info("Starting superstep %d", superstep)
            start = time.time()
            aggregated = cls.agg(verts, aggregator) if aggregator else None
            combinedMsgs = msgs.combineByKey(combiner, numSplits)
            grouped = verts.groupWith(combinedMsgs, numSplits=numSplits)
            verts, msgs, numMsgs, numActiveVerts = cls.comp(ctx, grouped,
                lambda v, ms: compute(v, ms, aggregated, superstep), checkpointDir)
            logger.info("superstep %d took %.1f s %d messages, %d active nodes",
                    superstep, time.time()-start, numMsgs, numActiveVerts)

            superstep += 1
            if numMsgs == 0 and numActiveVerts == 0:
                break
        return verts

    @classmethod
    def agg(cls, verts, aggregator):
        r = verts.map(lambda (id, vert): aggregator.createAggregator(vert))
        return r.reduce(aggregator.mergeAggregators)

    @classmethod
    def comp(cls, ctx, grouped, compute, checkpointDir=None):
        numMsgs = ctx.accumulator(0)
        numActiveVerts = ctx.accumulator(0)
        def proc((vs, cs)):
            if not vs:
                return []
            newVert, newMsgs = compute(vs[0], cs)
            numMsgs.add(len(newMsgs))
            if newVert.active:
                numActiveVerts.add(1)
            return [(newVert, newMsgs)]
        processed = grouped.flatMapValue(proc)
        verts = processed.mapValue(lambda (vert, msgs): vert)
        msgs = processed.flatMap(lambda (id, (vert, msgs)): msgs)
        if checkpointDir:
            verts = verts.checkpoint(checkpointDir)
        #else:
        #    processed = processed.cache()
        # force evaluation of processed RDD for accurate performance measurements
        n = verts.count()
        return verts, msgs, numMsgs.value, numActiveVerts.value

    @classmethod
    def addAggregatorArg(cls, compute):
        def _(vert, messages, aggregator, superstep):
            return compute(vert, messages)
        return _

#!/usr/bin/env python
import sys, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dpark import Bagel, DparkContext

class SPVertex:
    def __init__(self, id, value, outEdges, active):
        self.id = id
        self.value = value
        self.outEdges = outEdges
        self.active = active
    def __repr__(self):
        return "<V(%s, %s, %s)>" % (self.id, self.value, self.active)

class SPEdge:
    def __init__(self, target_id, value):
        self.target_id = target_id
        self.value = value
    def __repr__(self):
        return '<Edge(%s, %s)>' % (self.target_id, self.value)

class SPMessage:
    def __init__(self, target_id, value):
        self.target_id = target_id
        self.value = value
    def __repr__(self):
        return "<Message(%s, %s)>" % (self.target_id, 
            self.value)

class MinCombiner:
    def createCombiner(self, msg): return msg.value
    def mergeValue(self, comb, msg): return min(comb, msg.value)
    def mergeCombiners(self, a, b): return min(a,b)

def to_vertex((id, lines)):
    outEdges = [SPEdge(tid, int(v)) 
        for _, tid, v in lines]
    return (id, SPVertex(id, sys.maxint, outEdges, True))

def compute(self, vs, agg, superstep):
    newValue = min(self.value, vs[0]) if vs else self.value
    if newValue != self.value:
        outbox = [SPMessage(edge.target_id, newValue + edge.value)
                for edge in self.outEdges]
    else:
        outbox = []
    return SPVertex(self.id, newValue, self.outEdges, False), outbox

if __name__ == '__main__':
    ctx = DparkContext()
    lines = ctx.textFile('graph.txt').map(lambda line:line.split(' '))
    vertices = lines.filter(lambda x:len(x)==3).groupBy(
        lambda line:line[0]).map(to_vertex)
    messages = lines.filter(lambda x:len(x)==2).map(
        lambda (vid, v): (vid, SPMessage(vid, int(v)))
    )
    print 'read', vertices.count(), 'vertices and ', messages.count(), 'messages.'

    result = Bagel.run(ctx, vertices, messages, compute, MinCombiner())
    startVertex = 0
    print 'Shortest path from %s to all vertices:' % startVertex
    for v in result.collect():
        if v.value == sys.maxint:
            v.value = 'inf'
        print v.id, v.value

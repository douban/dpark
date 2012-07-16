#!/usr/bin/env python
import sys, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dpark import Bagel, DparkContext
from dpark.bagel import Vertex, Edge, Message

class MinCombiner:
    def createCombiner(self, msg): return msg.value
    def mergeValue(self, comb, msg): return min(comb, msg.value)
    def mergeCombiners(self, a, b): return min(a,b)

def to_vertex((id, lines)):
    outEdges = [Edge(tid, int(v)) 
        for _, tid, v in lines]
    return (id, Vertex(id, sys.maxint, outEdges, True))

def compute(self, vs, agg, superstep):
    newValue = min(self.value, vs[0]) if vs else self.value
    if newValue != self.value:
        outbox = [Message(edge.target_id, newValue + edge.value)
                for edge in self.outEdges]
    else:
        outbox = []
    return Vertex(self.id, newValue, self.outEdges, False), outbox

if __name__ == '__main__':
    ctx = DparkContext()
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'graph.txt')
    lines = ctx.textFile(path).map(lambda line:line.split(' '))
    vertices = lines.groupBy(lambda line:line[0]).map(to_vertex)
    startVertex = str(0)
    messages = ctx.makeRDD([(startVertex, Message(startVertex, 0))])
    
    print 'read', vertices.count(), 'vertices and ', messages.count(), 'messages.'

    result = Bagel.run(ctx, vertices, messages, compute, MinCombiner())
    
    print 'Shortest path from %s to all vertices:' % startVertex
    for v in result.collect():
        if v.value == sys.maxint:
            v.value = 'inf'
        print v.id, v.value

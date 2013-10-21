#!/usr/bin/env python
import sys, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dpark import DparkContext
from dpark.bagel import Vertex, Edge, Bagel

def parse_vertex(line, numV):
    fields = line.split(' ')
    title, refs = fields[0], fields[1:]
    outEdges = [Edge(ref) for ref in refs]
    return (title, Vertex(title, 1.0/numV, outEdges, True))

def gen_compute(num, epsilon):
    def compute(self, messageSum, agg, superstep):
        if messageSum and messageSum[0]:
            newValue = 0.15 / num + 0.85 * messageSum[0]
        else:
            newValue = self.value
        terminate = (superstep >= 10 and abs(newValue-self.value) < epsilon) or superstep > 30
        outbox = [(edge.target_id, newValue / len(self.outEdges))
                for edge in self.outEdges] if not terminate else []
        return Vertex(self.id, newValue, self.outEdges, not terminate), outbox
    return compute

if __name__ == '__main__':
    inputFile = 'wikipedia.txt'
    threshold = 0.01

    dpark = DparkContext()
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), inputFile)
    input = dpark.textFile(path)
    numVertex = input.count()
    vertices = input.map(lambda line: parse_vertex(line, numVertex)).cache()
    epsilon = 0.01 / numVertex
    messages = dpark.parallelize([])
    result = Bagel.run(dpark, vertices, messages,
        gen_compute(numVertex, epsilon))

    for id, v in result.filter(lambda (id, v): v.value>threshold).collect():
        print id, v

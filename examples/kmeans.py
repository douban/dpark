#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import random
from dpark import DparkContext
dpark = DparkContext()
from vector import Vector

def parseVector(line):
    return Vector(map(float, line.strip().split(' ')))

def closestCenter(p, centers):
    bestDist = p.squaredDist(centers[0])
    bestIndex = 0
    for i in range(1, len(centers)):
        d = p.squaredDist(centers[i])
        if d < bestDist:
            bestDist = d
            bestIndex = i
    return bestIndex


if __name__ == '__main__':
    D = 4
    K = 3
    IT = 10
    MIN_DIST = 0.01
    centers = [Vector([random.random() for j in range(D)]) for i in range(K)]
    points = dpark.textFile('kmeans_data.txt').map(parseVector).cache()

    for it in range(IT):
        print 'iteration', it
        mappedPoints = points.map(lambda p:(closestCenter(p, centers), (p, 1)))
        ncenters = mappedPoints.reduceByKey(
                lambda (s1,c1),(s2,c2): (s1+s2,c1+c2)
            ).map(
                lambda (id, (sum, count)): (id, sum/count)
            ).collectAsMap()
        
        updated = False
        for i in ncenters:
            if centers[i].dist(ncenters[i]) > MIN_DIST:
                centers[i] = ncenters[i]
                updated = True
        if not updated:
            break
        print centers
    
    print 'final', centers

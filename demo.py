import math
import random
import os
from pprint import pprint
from context import SparkContext
import logging
logging.basicConfig(level=logging.ERROR,
    format="%(process)d:%(threadName)s:%(levelname)s %(message)s")

spark = SparkContext("process")

# text search
f = spark.textFile("./", ext='py').map(lambda x:x.strip())
log = f.filter(lambda line: 'logging' in line).cache()
print 'logging', log.count()
print 'error', log.filter(lambda line: 'error' in line).count()
for line in log.filter(lambda line: 'error' in line):
    print line

# word count
counts = f.flatMap(lambda x:x.split()).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).cache()
pprint(counts.filter(lambda (_,v): v>50).collectAsMap())
pprint(sorted(counts.filter(lambda (_,v): v>20).map(lambda (x,y):(y,x)).groupByKey().collect()))
pprint(counts.map(lambda v: "%s:%s"%v ).saveAsTextFile("wc/").collect())

# Pi
import random
def rand(i):
    x = random.random()
    y = random.random()
    return (x*x + y*y) < 1.0 and 1 or 0

N = 100000
count = spark.parallelize(range(N), 4).map(rand).reduce(lambda x,y:x+y)
print 'pi is ', 4.0 * count / N
# Logistic Regression
def parsePoint(line):
    ns = map(int, line.split())
    return (ns[:-1], ns[-1])

def incm(w):
    def inc((x, y)):
        wx = sum(w[i]*x[i] for i in xrange(len(x)))
        if wx * y < -500:
            yy = -y
        else:
            yy = (1/(1+math.exp(-y*wx))-y)
        return [yy * xx for xx in x]
    return inc
add = lambda x,y: [x[i]+y[i] for i in range(len(x))]

points = spark.textFile("point.txt").map(parsePoint).cache()
print points.collect()
w = [1.0,-160.0]
for i in range(10):
    gradient = points.map(incm(w)).reduce(add)
    print w, gradient
    w = [a - b  for a, b in zip(w, gradient)]
print "Final separating plane: ", w

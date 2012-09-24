import math
import random
import os, sys
from pprint import pprint
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dpark

# range
nums = dpark.parallelize(range(100), 4)
print nums.count()
print nums.reduce(lambda x,y:x+y)

# text search
f = dpark.textFile("./", ext='py').map(lambda x:x.strip())
log = f.filter(lambda line: 'logging' in line).cache()
print 'logging', log.count()
print 'error', log.filter(lambda line: 'error' in line).count()
for line in log.filter(lambda line: 'error' in line).collect():
    print line

# word count
counts = f.flatMap(lambda x:x.split()).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).cache()
pprint(counts.filter(lambda (_,v): v>50).collectAsMap())
pprint(sorted(counts.filter(lambda (_,v): v>20).map(lambda (x,y):(y,x)).groupByKey().collect()))
pprint(counts.map(lambda v: "%s:%s"%v ).saveAsTextFile("wc/"))

# Pi
import random
def rand(i):
    x = random.random()
    y = random.random()
    return (x*x + y*y) < 1.0 and 1 or 0

N = 100000
count = dpark.parallelize(range(N), 4).map(rand).reduce(lambda x,y:x+y)
print 'pi is ', 4.0 * count / N

# Logistic Regression
def parsePoint(line):
    ns = map(int, line.split())
    return (ns[:-1], ns[-1])

def dot(a, b):
    return sum(i*j for i,j in zip(a,b))

def incm(w):
    def inc((x, y)):
        wx = dot(w,x)
        if wx < -500:
            wx = -500
        yy = y-1/(1+math.exp(-wx))
        return [yy * xx for xx in x]
    return inc
add = lambda x,y: [x[i]+y[i] for i in range(len(x))]

points = dpark.textFile("point.txt").map(parsePoint).cache()
print points.collect()
w = [1,-160]
for i in range(10):
    gradient = points.map(incm(w)).reduce(add)
    print w, gradient
    if sum(abs(g) for g in gradient) < 0.001:
        break
    w = [a + b/(i+1)  for a, b in zip(w, gradient)]
print "Final separating plane: ", w
print 1/(1+math.exp(-dot(w, [150, 1])))
print 1/(1+math.exp(-dot(w, [180, 1])))

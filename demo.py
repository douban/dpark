import math
from pprint import pprint
from context import SparkContext
import logging
logging.basicConfig(level=logging.INFO)

spark = SparkContext("local")
spark.init()

# text search
#f = spark.textFile("./", ext='py')
#log = f.filter(lambda line: 'logging' in line)
#print 'logging', log.count()
#print 'error', log.filter(lambda line: 'error' in line).count()
#for line in log.filter(lambda line: 'error' in line).collect():
#    print line
#
## word count
#counts = f.flatMap(lambda x:x.split()).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
#pprint(counts.filter(lambda (_,v): v>5).collectAsMap())
#pprint(counts.filter(lambda (_,v): v>10).map(lambda (x,y):(y,x)).reduceByKey(lambda x,y:x+' ' + y).collectAsMap())
#pprint(counts.map(lambda v: "%s:%s"%v ).saveAsTextFile("wc/").collect())
#
## Pi
#import random
#def rand(i):
#    x = random.random()
#    y = random.random()
#    return (x*x + y*y) < 1.0 and 1 or 0
#N = 100000
#count = spark.parallelize(range(N), 4).map(rand).reduce(lambda x,y:x+y)
#print 'pi is ', 4.0 * count / N

# Logistic Regression
def parsePoint(line):
    ns = map(int, line.split())
    return (ns[:-1], ns[-1])
def incm(w):
    def inc((x, y)):
        wx = sum(w[i]*x[i] for i in xrange(len(x)))
        yy = (1/(1+math.exp(-y*wx))-1) * y
        return [yy * xx for xx in x]
    return inc
def add(x,y):
    return [x[i]+y[i] for i in range(len(x))]

print parsePoint("1 3")
points = spark.textFile("point.txt").map(parsePoint).cache()
print points.collect()
w = [0]
N = 100
for i in range(40):
    gradient = points.map(incm(w)).reduce(add)
    print w, gradient
    w  = add(w, gradient)
print "Final separating plane: ", w

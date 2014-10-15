'''
    Notice:
    1. The function for jit should locate in mfs
    2. For the usage of jit types and signatures, please refer Numba documentation <http://numba.github.com/numba-doc/0.14/index.html>
'''
from dpark import _ctx as dpark, jit
import numpy

@jit('f8(f8[:])')
def add1(x):
    sum = 0.0
    for i in xrange(x.shape[0]):
        sum += i*x[i]
    return sum

@jit
def add2(x):
    sum = 0.0
    for i in xrange(x.shape[0]):
        sum += i*x[i]
    return sum

def add3(x):
    sum = 0.0
    for i in xrange(x.shape[0]):
        sum += i*x[i]
    return sum

rdd = dpark.makeRDD(range(0, 10)).map(lambda x: numpy.arange(x*1e7, (x+1)*1e7))

print rdd.map(add1).collect()
print rdd.map(add2).collect()
print rdd.map(add3).collect()

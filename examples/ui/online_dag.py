import time
from dpark import DparkContext


def m(x):
    if x[0] == 0:
        time.sleep(100)
    return x


def r(x, y):
    return x + y


dc = DparkContext("mesos")

rdd = dc.makeRDD([(i, i) for i in range(2)], 2)
rdd.collect()
rdd.reduceByKey(r).map(m).reduceByKey(r).collect()

# -*- coding: utf-8 -*-

from dpark import DparkContext


def m(x):
    return x


def r(x, y):
    return x + y


def src():
    return dc.makeRDD([(1,1)],2)


dc = DparkContext("mesos")

rdd1 = src()
rdd2 = src().reduceByKey(r)

to_union_1_a = [src() for _ in range(2)]
to_union_1_b = [src()]
to_union_2_a = [dc.union(to_union_1_a + to_union_1_b) for _ in range(2)]
to_union_2_b = [rdd2, rdd1]
to_union_3_a = [dc.union(to_union_2_a + to_union_2_b).map(m).reduceByKey(r)]
to_union_3_b = [rdd2]
rdd3 = dc.union(to_union_3_a + to_union_3_b)
rdd4 = rdd2.join(rdd2)

rdd1.collect()
rdd2.collect()
rdd3.collect()
rdd4.collect()

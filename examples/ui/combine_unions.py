# -*- coding: utf-8 -*-

from dpark import DparkContext


dc = DparkContext()


def get_rdd():
    return dc.makeRDD([(1, 1)])


rdd1 = get_rdd()
rdd2 = dc.union([get_rdd() for i in range(2)])
rdd3 = get_rdd().groupByKey()
dc.union([rdd1, rdd2, rdd3]).collect()

# -*- coding: utf-8 -*-

from dpark import DparkContext


def m(x):
    return x


rdd = DparkContext().makeRDD([(1,1)]).map(m).groupByKey()
rdd.map(m).collect()
rdd.map(m).collect()

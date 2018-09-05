# -*- coding: utf-8 -*-

from dpark import DparkContext
from dpark.utils.frame import Scope
from pprint import pprint

def test_scope():
    gid = Scope.gid
    dc = DparkContext()
    rdd = dc.makeRDD([1, 2, 3]).map(int).map(int).map(int)

    for i, r in enumerate([rdd.prev.prev, rdd.prev, rdd]):
        assert r.scope.id == gid + i + 1
        assert r.scope.call_site.startswith("map:{}".format(i))

    rdd = dc.makeRDD([1, 2, 3]) \
        .map(int) \
        .map(int) \
        .map(int)

    for i, r in enumerate([rdd.prev.prev, rdd.prev, rdd]):
        assert r.scope.id == gid + 4 + i + 1
        assert r.scope.call_site.startswith("map:0")


def test_call_graph():
    dc = DparkContext()
    Scope.gid = 0
    rdd = dc.makeRDD([(1, 1), (1, 2)]).map(lambda x: x)
    rdd = rdd.join(rdd)
    g = dc.scheduler.get_call_graph(rdd)
    pprint(g)
    assert g == ([0, 1, 2, 3], {(0, 1): 1, (1, 2): 2, (2, 3): 1})

    fg = dc.scheduler.fmt_call_graph(g)
    pprint(fg)

    Scope.gid = 0
    r1 = dc.union([dc.makeRDD([(1, 2)]) for _ in range(2)])
    r2 = dc.union([dc.makeRDD([(3, 4)]) for _ in range(2)])
    rdd = r1.union(r2)
    g = dc.scheduler.get_call_graph(rdd)
    pprint(g)
    fg = dc.scheduler.fmt_call_graph(g)
    pprint(fg)
    assert g == ([0, 1, 2, 3, 4, 5], {(0, 1): 2, (1, 4): 1, (2, 3): 2, (3, 4): 1, (4, 5): 1})



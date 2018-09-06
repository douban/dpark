# -*- coding: utf-8 -*-

from dpark import DparkContext
from dpark.utils.frame import Scope
from pprint import pprint


def test_scope():

    Scope.gid = 0
    dc = DparkContext()
    rdd = dc.makeRDD([1, 2, 3]).map(int).map(int).map(int)

    for i, r in enumerate([rdd.prev.prev, rdd.prev, rdd]):
        assert r.scope.id == i + 1
        assert r.scope.api_callsite.startswith("map:{}".format(i))

    Scope.gid = 0
    rdd = dc.makeRDD([1, 2, 3]) \
        .map(int) \
        .map(int) \
        .map(int)

    for i, r in enumerate([rdd.prev.prev, rdd.prev, rdd]):
        assert r.scope.id == i + 1
        assert r.scope.api_callsite.startswith("map:0")

    def get_rdd(n):
        return dc.makeRDD([n, n]).map(int).map(int).map(int)

    rdds = [get_rdd(1), get_rdd(2)]
    assert rdds[0].scope.id + 4 == rdds[1].scope.id

    rdds = [get_rdd(i) for i in range(2)]
    assert rdds[0].scope.id == rdds[1].scope.id


def test_call_graph():
    dc = DparkContext()
    Scope.gid = 0
    Scope.api_callsites = {}
    rdd = dc.makeRDD([(1, 1), (1, 2)]).map(lambda x: x)
    rdd = rdd.join(rdd)
    g = dc.scheduler.get_call_graph(rdd)
    pprint(g)
    assert g == ([0, 1, 2, 3], {(0, 1): 1, (1, 2): 2, (2, 3): 1})

    fg = dc.scheduler.fmt_call_graph(g)
    pprint(fg)

    Scope.gid = 0
    Scope.api_callsites = {}
    r1 = dc.union([dc.makeRDD([(1, 2)]) for _ in range(2)])
    r2 = dc.union([dc.makeRDD([(3, 4)]) for _ in range(2)])
    rdd = r1.union(r2)
    g = dc.scheduler.get_call_graph(rdd)
    pprint(g)
    fg = dc.scheduler.fmt_call_graph(g)
    pprint(fg)
    assert g == ([0, 1, 2, 3, 4, 5], {(0, 1): 2, (1, 4): 1, (2, 3): 2, (3, 4): 1, (4, 5): 1})


def test_lineage():
    dc = DparkContext()
    rdd1 = dc.union([dc.makeRDD([(1, 2)]) for _ in range(2)])
    assert len(rdd1.lineages) == 1

    rdd2 = dc.union([dc.makeRDD([(1, 2)]) for _ in range(2)])
    rdd3 = rdd1.union(rdd2)
    assert len(rdd3.lineages) == 2

    rdd4 = dc.union([dc.union([dc.makeRDD([(1, 2)]) for _ in range(2)]) for _ in range(2)])
    assert len(rdd4.lineages) == 1
    assert len(list(rdd4.lineages.values())[0].lineages) == 1

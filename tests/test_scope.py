# -*- coding: utf-8 -*-

from dpark import DparkContext
from dpark.utils.frame import Scope


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

# -*- coding: utf-8 -*-

from dpark import DparkContext
from dpark.utils.frame import Scope
from pprint import pprint


def test_scope():

    Scope.reset()
    dc = DparkContext()

    rdd = dc.makeRDD([1, 2, 3]).map(int).map(int).map(int)
    dc.scheduler.current_scope = Scope.get("")

    for i, r in enumerate([rdd.prev.prev, rdd.prev, rdd]):
        assert r.scope.id == i + 1
        assert r.scope.api_callsite.startswith("map:{}".format(i))

    Scope.reset()
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


def test_call_graph_join():
    dc = DparkContext()
    Scope.reset()
    rdd = dc.makeRDD([(1, 1), (1, 2)]).map(lambda x: x)
    rdd = rdd.join(rdd)
    dc.scheduler.current_scope = Scope.get("")
    g = dc.scheduler.get_call_graph(rdd)
    pprint(g)
    assert g == ([0, 1, 2, 3], {(0, 1): 1, (1, 2): 2, (2, 3): 1})

    fg = dc.scheduler.fmt_call_graph(g)
    # pprint(fg)


def test_call_graph_union():
    dc = DparkContext()
    Scope.reset()
    r1 = dc.union([dc.makeRDD([(1, 2)]) for _ in range(2)])
    r2 = dc.union([dc.makeRDD([(3, 4)]) for _ in range(2)])
    rdd = r1.union(r2)
    dc.scheduler.current_scope = Scope.get("")
    g = dc.scheduler.get_call_graph(rdd)
    # pprint(g)
    fg = dc.scheduler.fmt_call_graph(g)
    # pprint(fg)
    assert g == ([0, 1, 2, 3, 4, 5], {(0, 1): 2, (1, 4): 1, (2, 3): 2, (3, 4): 1, (4, 5): 1})


def test_lineage():
    Scope.reset()

    dc = DparkContext()
    rdd1 = dc.union([dc.makeRDD([(1, 2)]) for _ in range(5)])
    assert len(rdd1.dep_lineage_counts) == 1

    rdd2 = dc.union([dc.makeRDD([(1, 2)]) for _ in range(3)])
    rdd3 = rdd1.union(rdd2)
    assert len(rdd3.dep_lineage_counts) == 2

    rdd4 = dc.union([dc.union([dc.makeRDD([(1, 2)]) for _ in range(2)]) for _ in range(4)])
    assert len(rdd4.dep_lineage_counts) == 1
    assert len(list(rdd4.dependencies)[0].rdd.dep_lineage_counts) == 1
    rdd5 = rdd3.groupWith(rdd4)

    print("rdd1", rdd1.id, rdd1.dep_lineage_counts)
    stage = dc.scheduler.newStage(rdd1, None)
    pprint(stage.pipelines)
    pprint(stage.pipeline_edges)
    assert list(stage.pipelines.keys()) == [rdd1.id]
    assert stage.pipeline_edges == {}

    stage = dc.scheduler.newStage(rdd3, None)
    pprint(stage.pipelines)
    pprint(stage.pipeline_edges)
    assert sorted(list(stage.pipelines.keys())) == [rdd1.id, rdd2.id, rdd3.id]
    assert stage.pipeline_edges == {((-1, rdd1.id), (-1, rdd3.id)):1,
                                    ((-1, rdd2.id), (-1, rdd3.id)):1}

    stage = dc.scheduler.newStage(rdd4, None)
    pprint(stage.pipelines)
    pprint(stage.pipeline_edges)
    assert list(stage.pipelines.keys()) == [rdd4.id]
    assert stage.pipeline_edges == {}

    print("rdd5", rdd5.id, rdd3.id, rdd4.id)
    stage = dc.scheduler.newStage(rdd5, None)
    pprint(stage.pipelines)
    pprint(stage.pipeline_edges)
    assert sorted(list(stage.pipelines.keys())) == [rdd5.id]
    assert sorted(stage.pipeline_edges) == sorted([((s.id, s.rdd.id), (-1, rdd5.id)) for s in stage.parents])

    print('-' * 100)
    pprint(stage.get_pipeline_graph())

    for s in stage.parents:
        if s.rdd.id == rdd4.id:
            assert list(s.pipelines.keys()) == [rdd4.id]
            assert s.pipeline_edges == {}
        elif s.rdd.id == rdd3.id:
            assert sorted(list(s.pipelines.keys())) == [rdd1.id, rdd2.id, rdd3.id]
            assert s.pipeline_edges == {((-1, rdd1.id), (-1, rdd3.id)): 1,
                                        ((-1, rdd2.id), (-1, rdd3.id)): 1}
        else:
            assert False

        pprint(s.get_pipeline_graph())

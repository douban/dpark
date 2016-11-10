# -*- coding: utf-8 -*-
from dpark.rdd import ShuffleDependency
from dpark.util import get_logger

logger = get_logger(__name__)


class RDDNode(object):
    def __init__(self, rdd):
        self.id = rdd.id
        self.name = rdd.__class__.__name__
        self.scope = rdd.scope

    def __str__(self):
        return self.name + '_' + str(self.id)


class StageInfo(object):
    graph_idx = 0
    idToRDDNode = {}
    idToStageInfo = {}

    def __init__(self):
        self._stage_id = None
        self._nodes = set()
        self._internal_edges = set()
        self._depend_stages = set()
        self._input_edges = set()
        self.is_final = False

    def create_stage_info(self, stage):
        if not stage or stage.id in StageInfo.idToStageInfo:
            return
        self._stage_id = stage.id
        StageInfo.idToStageInfo[self._stage_id] = self
        nodes = set()

        def _(r, dep, filter_set):
            if isinstance(dep, ShuffleDependency):
                self._input_edges.add((dep.rdd.id, r.id))
                return False
            if len(dep.rdd.dependencies) != 0 or \
               dep.rdd.scope.func_name not in filter_set:
                # 无下级依赖的rdd如果属于同一个 scope.func_name ，
                # 则这些rdd只展示一次
                self._internal_edges.add((dep.rdd.id, r.id))
                filter_set.add(dep.rdd.scope.func_name)
                return True
            return False

        StageInfo._walk_dependencies(stage.rdd, _, nodes)
        self._nodes = nodes
        self._depend_stages = set(map(lambda st: st.id, stage.parents))
        if len(stage.parents) != 0:
            def create_stage(st):
                stage_info = StageInfo()
                stage_info.create_stage_info(st)
            map(lambda st: create_stage(st), stage.parents)

    @staticmethod
    def _walk_dependencies(rdd, func, nodes):
        # 遍历rdd，构造stageinfo
        visited = set()
        to_visit = [rdd]
        while to_visit:
            r = to_visit.pop(0)
            if r.id not in StageInfo.idToRDDNode:
                StageInfo.idToRDDNode[r.id] = RDDNode(r)
            nodes.add(r.id)
            if r.id in visited:
                continue
            visited.add(r.id)
            visited_set = set()  # 用于记录所有被访问过的rdd的 scope.func_name
            for dep in r.dependencies:
                if func(r, dep, visited_set):
                    to_visit.append(dep.rdd)

    def get_stage_tuples(self):
        tuple_list = []
        for stage_id in self._depend_stages:
            tuple_list += StageInfo.idToStageInfo[stage_id].get_stage_tuples()
        tuple_list.append(((self._stage_id, 'false'), self.get_stage_dot(),
                          self._input_edges))
        return tuple_list

    def get_stage_dot(self):
        ret_str = 'digraph Graph{\n'
        ret_str += 'subgraph cluster_stage_%d{\n' % self._stage_id
        ret_str += 'label="Stage %d"\n' % self._stage_id
        for node_id in self._nodes:
            inner_node = StageInfo.idToRDDNode[node_id]
            ret_str += 'subgraph cluster_node_%d{\n' % node_id
            ret_str += 'label=%s\n' % inner_node.scope.func_name
            ret_str += '%d [label="%s"]\n' % (node_id,
                                              (str(inner_node) + '\n' +
                                               inner_node.scope.call_site))
            ret_str += '}\n'
        ret_str += '}\n'
        for edge in self._internal_edges:
            ret_str += '%d->%d\n' % (edge[0], edge[1])
        ret_str += '}\n'
        return ret_str

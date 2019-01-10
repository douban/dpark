from __future__ import absolute_import
import marshal
import multiprocessing
import os
import socket
import sys
import time
import six
from six.moves import map, range, urllib, queue, cPickle
import weakref
import threading
import json
from collections import Counter

import zmq
from addict import Dict
from pymesos import MesosSchedulerDriver, encode_data, decode_data

import dpark.conf as conf
from dpark.accumulator import Accumulator
from dpark.dependency import ShuffleDependency
from dpark.env import env
from dpark.taskset import TaskSet, TaskCounter
from dpark.mutable_dict import MutableDict
from dpark.task import ResultTask, ShuffleMapTask, TTID, TaskState, TaskEndReason
from dpark.hostatus import TaskHostManager
from dpark.utils import (
    compress, decompress, spawn, getuser,
    sec2nanosec)
from dpark.utils.log import get_logger
from dpark.utils.frame import Scope
from dpark.utils import dag


logger = get_logger(__name__)

EXECUTOR_CPUS = 0.01
EXECUTOR_MEMORY = 128  # cache
POLL_TIMEOUT = 0.1
RESUBMIT_TIMEOUT = 60
MAX_IDLE_TIME = 60 * 30


class Stage(object):

    def __init__(self, rdd, shuffleDep, parents, pipelines, pipeline_edges, rdd_pipelines):
        """

        :param rdd: output rdd of this stage
        :param shuffleDep: for mapOutputStage, determine how computing result will be aggregated, partitioned
        """
        self.id = self.new_id()
        self.num_try = 0
        self.rdd = rdd
        self.shuffleDep = shuffleDep
        self.is_final = (shuffleDep is None)
        self.parents = parents
        self.numPartitions = len(rdd)
        self.num_finished = 0  # for final stage
        self.outputLocs = [[] for _ in range(self.numPartitions)]
        self.task_stats = [[] for _ in range(self.numPartitions)]
        self.taskcounters = []  # a TaskCounter object for each run/retry
        self.submit_time = 0
        self.finish_time = 0
        self.pipelines = pipelines  # each pipeline is a list of rdds
        self.pipeline_edges = pipeline_edges  # ((src_stage_id, src_pipeline_id), (dst_stage_id, dst_pipeline_id)): N
        self.rdd_pipelines = rdd_pipelines  # rdd_id: pipeline_id

    def __str__(self):
        return '<Stage(%d) for %s>' % (self.id, self.rdd)

    def __getstate__(self):
        raise Exception('should not pickle stage')

    def __len__(self):
        return self.numPartitions

    nextId = 0

    @classmethod
    def new_id(cls):
        cls.nextId += 1
        return cls.nextId

    @property
    def try_id(self):
        return TTID.make_taskset_id(self.id, self.num_try + 1)  # incr num_try After create TaskSet

    @property
    def isAvailable(self):
        if not self.parents and self.shuffleDep is None:
            return True
        return all(self.outputLocs)

    @property
    def num_task_finished(self):
        if self.is_final:
            return self.num_finished
        else:
            return len([i for i in self.outputLocs if i])

    @property
    def num_task_running(self):
        if self.taskcounters:
            return self.taskcounters[-1].running
        else:
            return 0

    def addOutputLoc(self, partition, host):
        self.outputLocs[partition].append(host)

    #    def removeOutput(self, partition, host):
    #        prev = self.outputLocs[partition]
    #        self.outputLocs[partition] = [h for h in prev if h != host]

    def removeHost(self, host):
        becameUnavailable = False
        for ls in self.outputLocs:
            if host in ls:
                ls.remove(host)
                becameUnavailable = True
        if becameUnavailable:
            msg = ("%s is now unavailable on host %s, "
                   "postpone resubmit until %d secs later "
                   "to wait for futher fetch failure")
            logger.info(msg, self, host, RESUBMIT_TIMEOUT)

    def finish(self):
        if not self.finish_time:
            self.finish_time = time.time()

    def _summary_stats(self):
        stats = [x[-1] for x in self.task_stats if x]

        d = {}

        def _summary(lst):
            lst.sort()
            r = {'max': max(lst),
                 'min': min(lst),
                 'sum': sum(lst)
                 }
            return r

        if stats:
            for attr in dir(stats[0]):
                if not attr.startswith('_'):
                    d[attr] = _summary(list([getattr(s, attr) for s in stats]))
        return d

    def get_node_id(self, stage_id, pipeline_id):
        if stage_id == -1:
            stage_id = self.id
        return "PIPELINE_{}.{}".format(stage_id, pipeline_id)

    def _fmt_node(self, stage_id, pipeline_id):
        if stage_id == -1:
            stage_id = self.id
        rdds = [{"rdd_name": rdd.ui_label,
                 "rdd_id": rdd.id,
                 "api_callsite_id": rdd.scope.api_callsite_id,
                 "params": rdd.params}
                for rdd in self.pipelines[pipeline_id]]
        n = {
            dag.KW_TYPE: "stage",
            dag.KW_ID: self.get_node_id(stage_id, pipeline_id),
            dag.KW_LABEL: str(stage_id),
            "rdds": rdds
        }
        return n

    def _fmt_edge(self, e):
        e, nrdd = e
        src, dst = [self.get_node_id(*n) for n in e]
        info = {}
        if nrdd > 1:
            info['#rdd'] = nrdd

        return {
            # dag.KW_ID: "{}_{}".format(src, dst),
            dag.KW_SRC: src,
            dag.KW_DST: dst,
            "info": info
        }

    def get_pipeline_graph(self):
        nodes = [self._fmt_node(self.id, pipeline_id) for pipeline_id in self.pipelines.keys()]
        edges = [self._fmt_edge(e) for e in six.iteritems(self.pipeline_edges)]
        g = {dag.KW_NODES: nodes, dag.KW_EDGES: edges}
        return g

    def fmt_stats(self):
        n = self.numPartitions
        stats = self._summary_stats()
        msg = "[metric = min/avg/max]: "
        for k, d in six.iteritems(stats):
            sm = d['sum']
            unit = k.split('_')[0]
            if unit == 'num':
                fmt = " = %d/%d/%d"
            else:
                fmt = " = %.2f/%.2f/%.2f"

            if sm > 0:
                msg += k
                vs = d['min'], sm / n, d['max']
                vs = tuple(map(int, vs))
                unit_s = " "
                if unit == "bytes":
                    vs = tuple([v >> 20 for v in vs])
                    fmt = " = %.2f/%.2f/%.2f"
                    unit_s = " MB "
                msg += (fmt % vs)
                msg += unit_s
        return msg

    def _summary_counters(self):

        def _sum(attr):
            return sum([getattr(counter, attr) for counter in self.taskcounters])

        counters = {
            "task": {
                "all": len(self),
                "running": self.num_task_running,
                "finished": self.num_task_finished,
            },
            "fail": dict([(attr[5:], _sum(attr)) for attr in TaskCounter(0).get_fail_types()])
        }
        return counters

    def get_prof(self):
        stats = self._summary_stats()
        counters = self._summary_counters()
        graph = self.get_pipeline_graph()

        info = {
            'id': self.id,
            'parents': [p.id for p in self.parents],
            'output_rdd': self.rdd.__class__.__name__,
            'output_pipeline': self.get_node_id(self.id, self.rdd.id),
            'api_callsite': self.rdd.scope.api_callsite,
            'start_time': self.submit_time,
            'finish_time': self.finish_time,
            'num_partition': self.numPartitions,
            'mem': self.rdd.mem,
        }
        res = {
            "info": info,
            "stats": stats,
            "counters": counters,
            'graph': graph
        }
        return res

    def get_tree_stages(self):
        stages = []
        to_visit = [self]
        seen = set()
        while to_visit:
            s = to_visit.pop()
            stages.append(s)
            for ss in s.parents:
                if ss.id not in seen:
                    to_visit.append(ss)
                    seen.add(ss.id)

        return stages


class Scheduler:

    def start(self):
        pass

    def runJob(self, rdd, func, partitions, allowLocal):
        pass

    def clear(self):
        pass

    def stop(self):
        pass

    def defaultParallelism(self):
        return 2


class CompletionEvent:

    def __init__(self, task, reason, result, accumUpdates, stats):
        self.task = task
        self.reason = reason
        self.result = result
        self.accumUpdates = accumUpdates
        self.stats = stats


def walk_dependencies(rdd, edge_func=lambda r, d: True, node_func=lambda r: True):
    visited = set()
    to_visit = [rdd]
    while to_visit:
        r = to_visit.pop(0)
        if r.id in visited:
            continue
        visited.add(r.id)
        if node_func(r):
            for dep in r.dependencies:
                if edge_func(r, dep):
                    to_visit.append(dep.rdd)


class DAGScheduler(Scheduler):

    def __init__(self):
        self.id = self.new_id()
        self.completionEvents = queue.Queue()
        self.idToStage = weakref.WeakValueDictionary()
        self.shuffleToMapStage = {}
        self.cacheLocs = {}
        self.idToRunJob = {}
        self.runJobTimes = 0
        self.frameworkId = None
        self.loghub_dir = None
        self.jobstats = []
        self.is_dstream = False
        self.current_scope = None

        self.final_lock = threading.RLock()
        self.final_stage = None
        self.final_rdd = None

    nextId = 0

    @classmethod
    def new_id(cls):
        cls.nextId += 1
        return cls.nextId

    def clear(self):
        self.idToStage.clear()
        self.shuffleToMapStage.clear()
        self.cacheLocs.clear()
        self.cacheTracker.clear()

    @property
    def cacheTracker(self):
        return env.cacheTracker

    @property
    def mapOutputTracker(self):
        return env.mapOutputTracker

    def submitTasks(self, tasks):
        raise NotImplementedError

    def taskEnded(self, task, reason, result, accumUpdates, stats=None):
        self.completionEvents.put(
            CompletionEvent(
                task,
                reason,
                result,
                accumUpdates,
                stats))

    def abort(self):
        self.completionEvents.put(None)

    def getCacheLocs(self, rdd):
        return self.cacheLocs.get(rdd.id, [[] for _ in range(len(rdd))])

    def updateCacheLocs(self):
        self.cacheLocs = self.cacheTracker.getLocationsSnapshot()

    def newStage(self, output_rdd, shuffleDep):
        """ A stage may contain multi data pipeline, which form a tree with one final output pipline as root.
            Zip, CartesianRDD, and Union may commine diff data sources, so lead to a split of the tree.
            The leaves of the tree may be one of:
                1. a pipeline start from a source RDD (TextFileRDD, Collection)
                2. a root pipeline of a parent stage .
            Unioned rdds with same lineage  keep only one by add it to dep_rdds and assign a pipeline_id.

            ---

            Be careful:
                - On one hand, logic for ui should not risk mixing newStage, the latter is much more important.
                - On the other hand, input pipeline need to link to parent stages.
        """
        parent_stages = set()

        pipelines = {output_rdd.id: [output_rdd]}
        pipeline_edges = Counter()

        rdd_pipelines = {output_rdd.id: output_rdd.id}

        to_visit = [output_rdd]
        visited = set()
        dep_filter = set()

        while to_visit:
            r = to_visit.pop(0)
            if r.id in visited:
                continue
            visited.add(r.id)
            my_pipeline_id = rdd_pipelines.get(r.id)
            if my_pipeline_id is not None:  # not all rdd have my_pipeline_id
                my_pipeline = pipelines.get(my_pipeline_id)
                if my_pipeline is None:
                    logger.warning("miss pipeline: {} ".format(r.scope.key))

            if r.shouldCache:
                self.cacheTracker.registerRDD(r.id, len(r))

            dep_rdds = []
            dep_stages = []
            for dep in r.dependencies:
                if isinstance(dep, ShuffleDependency):
                    stage = self.getShuffleMapStage(dep)
                    parent_stages.add(stage)
                    dep_stages.append(stage)
                    if my_pipeline_id is not None:
                        pipeline_edges[(stage.id, stage.rdd.id), (-1, my_pipeline_id)] += 1  # -1 : current_stage
                    else:
                        logger.warning("miss pipeline: {} {}".format(r.scope.key, dep.rdd.scope.key))
                else:
                    to_visit.append(dep.rdd)
                    if r.id not in dep_filter and dep.rdd.id in r.dep_lineage_counts:
                        dep_rdds.append(dep.rdd)
                    else:
                        dep_filter.add(dep.rdd.id)

            if my_pipeline is None:
                continue

            ns, nr = len(dep_stages), len(dep_rdds)

            if ns + nr <= 1:
                if nr == 1:
                    dep_rdd = dep_rdds[0]
                    my_pipeline.append(dep_rdd)
                    rdd_pipelines[dep_rdd.id] = my_pipeline_id
            else:
                for dep_rdd in dep_rdds:
                    did = dep_rdd.id
                    nrdd = r.dep_lineage_counts[did]
                    pipelines[did] = [dep_rdd]  # create a new pipeline/branch
                    rdd_pipelines[did] = did
                    pipeline_edges[((-1, did), (-1, my_pipeline_id))] = nrdd  # -1 : current_stage

        stage = Stage(output_rdd, shuffleDep, list(parent_stages), pipelines, dict(pipeline_edges), rdd_pipelines)
        self.idToStage[stage.id] = stage
        logger.debug('new stage: %s', stage)
        return stage

    def getShuffleMapStage(self, dep):
        stage = self.shuffleToMapStage.get(dep.shuffleId, None)
        if stage is None:
            stage = self.newStage(dep.rdd, dep)
            self.shuffleToMapStage[dep.shuffleId] = stage
        return stage

    def getMissingParentStages(self, stage):
        missing = set()

        def _(r, dep):
            if r.shouldCache and all(self.getCacheLocs(r)):
                return False

            if isinstance(dep, ShuffleDependency):
                stage = self.getShuffleMapStage(dep)
                if not stage.isAvailable:
                    missing.add(stage)
                return False

            return True

        walk_dependencies(stage.rdd, _)
        return list(missing)

    def get_call_graph(self, final_rdd):
        edges = Counter()  # <parent, child > : count
        visited = set()
        to_visit = [final_rdd]

        while to_visit:
            r = to_visit.pop(0)
            if r.id in visited:
                continue
            visited.add(r.id)
            for dep in r.dependencies:
                to_visit.append(dep.rdd)
                if dep.rdd.scope.api_callsite_id != r.scope.api_callsite_id:
                    edges[(dep.rdd.scope.api_callsite_id, r.scope.api_callsite_id)] += 1
        nodes = set()
        run_scope = self.current_scope
        edges[(final_rdd.scope.api_callsite_id, run_scope.api_callsite_id)] = 1
        for s, d in edges.keys():
            nodes.add(s)
            nodes.add(d)
        return sorted(list(nodes)), dict(edges)

    @classmethod
    def fmt_call_graph(cls, g0):
        nodes0, edges0 = g0
        nodes = []
        edges = [{dag.KW_ID: "{}_{}".format(parent, child), dag.KW_SRC: parent, dag.KW_DST: child, "count": count}
                 for ((parent, child), count) in edges0.items()]

        for n in nodes0:
            scope = Scope.scopes_by_api_callsite_id[n][0]
            nodes.append({dag.KW_ID: n,
                          dag.KW_LABEL: scope.api,
                          dag.KW_DETAIL: [scope.api_callsite, scope.stack_above_api]})

        return {dag.KW_NODES: nodes, dag.KW_EDGES: edges}

    def get_profs(self):
        res = [marshal.loads(j) for j in self.jobstats]
        running = self.get_running_prof()
        if running:
            res.append(marshal.loads(marshal.dumps(running)))
        return res

    def get_running_prof(self):
        if self.final_stage:
            with self.final_lock:
                return self._get_stats(self.final_rdd, self.final_stage)

    def runJob(self, finalRdd, func, partitions, allowLocal):
        self.runJobTimes += 1
        self.current_scope = Scope.get("Job %d:{api}" % (self.runJobTimes, ))
        outputParts = list(partitions)
        numOutputParts = len(partitions)
        finalStage = self.newStage(finalRdd, None)
        with self.final_lock:
            self.final_rdd = finalRdd
            self.final_stage = finalStage
        try:
            from dpark.web.ui.views.rddopgraph import StageInfo
            stage_info = StageInfo()
            stage_info.create_stage_info(finalStage)

            def create_stage_info_recur(cur_stage, is_final=False):
                if not cur_stage or cur_stage.id in self.idToRunJob:
                    return
                for par_stage in cur_stage.parents:
                    create_stage_info_recur(par_stage)
                if cur_stage.id not in self.idToRunJob:
                    self.idToRunJob[cur_stage.id] = StageInfo.idToStageInfo[cur_stage.id]
                    self.idToRunJob[cur_stage.id].is_final = is_final

            create_stage_info_recur(finalStage, is_final=True)
        except ImportError:
            pass
        results = [None] * numOutputParts
        finished = [None] * numOutputParts
        last_finished = 0
        finalStage.num_finished = 0

        waiting = set()
        running = set()
        failed = set()
        pendingTasks = {}  # stage -> set([task_id..])
        lastFetchFailureTime = 0

        self.updateCacheLocs()

        logger.debug('Final stage: %s, %d', finalStage, numOutputParts)
        logger.debug('Parents of final stage: %s', finalStage.parents)
        logger.debug(
            'Missing parents: %s',
            self.getMissingParentStages(finalStage))

        def onStageFinished(stage):
            def _(r, dep):
                return r._do_checkpoint()

            MutableDict.merge()
            walk_dependencies(stage.rdd, _)
            logger.info("stage %d finish %s", stage.id, stage.fmt_stats())

        if (allowLocal and
                (
                        not finalStage.parents or
                        not self.getMissingParentStages(finalStage)
                ) and numOutputParts == 1):
            split = finalRdd.splits[outputParts[0]]
            yield func(finalRdd.iterator(split))
            onStageFinished(finalStage)
            return

        def submitStage(stage):
            if not stage.submit_time:
                stage.submit_time = time.time()
            logger.debug('submit stage %s', stage)
            if stage not in waiting and stage not in running:
                missing = self.getMissingParentStages(stage)
                if not missing:
                    submitMissingTasks(stage)
                    running.add(stage)
                else:
                    for parent in missing:
                        submitStage(parent)
                    waiting.add(stage)

        def submitMissingTasks(stage):
            myPending = pendingTasks.setdefault(stage, set())
            tasks = []
            have_prefer = True
            if stage == finalStage:
                for i in range(numOutputParts):
                    if not finished[i]:
                        part = outputParts[i]
                        if have_prefer:
                            locs = self.getPreferredLocs(finalRdd, part)
                            if not locs:
                                have_prefer = False
                        else:
                            locs = []
                        tasks.append(ResultTask(finalStage.id, finalStage.try_id, part, finalRdd,
                                                func, locs, i))
            else:
                for part in range(stage.numPartitions):
                    if not stage.outputLocs[part]:
                        if have_prefer:
                            locs = self.getPreferredLocs(stage.rdd, part)
                            if not locs:
                                have_prefer = False
                        else:
                            locs = []
                        tasks.append(ShuffleMapTask(stage.id, stage.try_id, part, stage.rdd,
                                                    stage.shuffleDep, locs))
            logger.debug('add to pending %s tasks', len(tasks))
            myPending |= set(t.id for t in tasks)
            self.submitTasks(tasks)

        submitStage(finalStage)

        while finalStage.num_finished != numOutputParts:
            try:
                evt = self.completionEvents.get(False)
            except queue.Empty:
                if (failed and
                        time.time() > lastFetchFailureTime + RESUBMIT_TIMEOUT):
                    self.updateCacheLocs()
                    for stage in failed:
                        logger.info('Resubmitting failed stages: %s', stage)
                        submitStage(stage)
                    failed.clear()
                else:
                    time.sleep(0.1)
                continue

            if evt is None:  # aborted
                for taskset in self.active_tasksets.values():
                    self.tasksetFinished(taskset)

                if not self.is_dstream:
                    self._keep_stats(finalRdd, finalStage)

                raise RuntimeError('TaskSet aborted!')

            task, reason = evt.task, evt.reason
            stage = self.idToStage[task.stage_id]
            if stage not in pendingTasks:  # stage from other taskset
                continue
            logger.debug('remove from pending %s from %s', task, stage)
            pendingTasks[stage].remove(task.id)
            if reason == TaskEndReason.success:
                Accumulator.merge(evt.accumUpdates)
                stage.task_stats[task.partition].append(evt.stats)
                if isinstance(task, ResultTask):
                    finished[task.outputId] = True
                    finalStage.num_finished += 1
                    results[task.outputId] = evt.result

                    while last_finished < numOutputParts and finished[last_finished]:
                        yield results[last_finished]
                        results[last_finished] = None
                        last_finished += 1

                    stage.finish()

                elif isinstance(task, ShuffleMapTask):
                    stage = self.idToStage[task.stage_id]
                    stage.addOutputLoc(task.partition, evt.result)
                    if all(stage.outputLocs):
                        stage.finish()
                        logger.debug(
                            '%s finished; looking for newly runnable stages',
                            stage
                        )
                        if pendingTasks[stage]:
                            logger.warn('dirty stage %d with %d tasks'
                                        '(select at most 10 tasks:%s) not clean',
                                        stage.id, len(pendingTasks[stage]),
                                        str(list(pendingTasks[stage])[:10]))
                            del pendingTasks[stage]
                        onStageFinished(stage)
                        running.remove(stage)
                        if stage.shuffleDep is not None:
                            self.mapOutputTracker.registerMapOutputs(
                                stage.shuffleDep.shuffleId,
                                [l[-1] for l in stage.outputLocs])
                        self.updateCacheLocs()
                        newlyRunnable = set(
                            stage for stage in waiting
                            if not self.getMissingParentStages(stage)
                        )
                        waiting -= newlyRunnable
                        running |= newlyRunnable
                        logger.debug(
                            'newly runnable: %s, %s', waiting, newlyRunnable)
                        for stage in newlyRunnable:
                            submitMissingTasks(stage)
            elif reason == TaskEndReason.fetch_failed:
                exception = evt.result
                if stage in running:
                    waiting.add(stage)
                    running.remove(stage)
                mapStage = self.shuffleToMapStage[exception.shuffleId]
                mapStage.removeHost(exception.serverUri)
                failed.add(mapStage)
                lastFetchFailureTime = time.time()
            else:
                logger.error(
                    'task %s failed: %s %s %s',
                    task,
                    reason,
                    type(reason),
                    reason.message)
                raise Exception(reason.message)

        onStageFinished(finalStage)

        if not self.is_dstream:
            self._keep_stats(finalRdd, finalStage)
        assert all(finished)
        with self.final_lock:
            self.final_stage = None
            self.final_rdd = None
        return

    def getPreferredLocs(self, rdd, partition):
        return rdd.preferredLocations(rdd.splits[partition])

    def _keep_stats(self, final_rdd, final_stage):
        try:
            stats = self._get_stats(final_rdd, final_stage)
            self.jobstats.append(marshal.dumps(stats))
            if self.loghub_dir:
                self._dump_stats(stats)
        except Exception as e:
            logger.exception("Fail to dump job stats: %s.", e)

    def _dump_stats(self, stats):
        name = "_".join(map(str, ['sched', self.id, "job", self.runJobTimes])) + ".json"
        path = os.path.join(self.loghub_dir, name)
        logger.info("writing profile to %s", path)
        with open(path, 'w') as f:
            json.dump(stats, f, indent=4)

    def _get_stats(self, final_rdd, final_stage):
        call_graph = self.fmt_call_graph(self.get_call_graph(final_rdd))
        cmd = '[dpark] ' + \
              os.path.abspath(sys.argv[0]) + ' ' + ' '.join(sys.argv[1:])

        stages = sorted([s.get_prof() for s in final_stage.get_tree_stages()],
                        key=lambda x: x['info']['start_time'])

        sink_scope = self.current_scope
        sink_id = "SINK_{}_{}".format(self.id, self.runJobTimes)
        sink_node = {
            dag.KW_TYPE: "sink",
            dag.KW_ID: sink_id,
            dag.KW_LABEL: sink_scope.name,
            "call_id": sink_scope.api_callsite_id
        }

        sink_edge = {
            "source": final_stage.get_node_id(final_stage.id, final_stage.rdd_pipelines[final_rdd.id]),
            "target": sink_id,
        }
        run = {'framework': self.frameworkId,
               'scheduler': self.id,
               "run": self.runJobTimes,
               'sink': {
                   "call_site": sink_scope.api_callsite,
                   "node": sink_node,
                   "edges": sink_edge,
               },
               'stages': stages,
               "call_graph": call_graph,
               }

        ret = {
            'script': {
                'cmd': cmd,
                'env': {'PWD': os.getcwd()}
            },
            'run': run
        }
        return ret


def run_task(task, aid):
    logger.debug('Running task %r', task)
    try:
        Accumulator.clear()
        result = task.run(aid)
        accumUpdates = Accumulator.values()
        MutableDict.flush()
        return task.id, result, accumUpdates
    except Exception as e:
        logger.error('error in task %s', task)
        import traceback
        traceback.print_exc()
        e.task_id = task.id
        raise e


class LocalScheduler(DAGScheduler):
    attemptId = 0

    def nextAttempId(self):
        self.attemptId += 1
        return self.attemptId

    def submitTasks(self, tasks):
        logger.debug('submit tasks %s in LocalScheduler', tasks)
        for task in tasks:
            task_copy = cPickle.loads(cPickle.dumps(task, -1))
            try:
                _, result, update = run_task(task_copy, self.nextAttempId())
                self.taskEnded(task, TaskEndReason.success, result, update)
            except Exception:
                self.taskEnded(task, TaskEndReason.other_failure, None, None)


def run_task_in_process(task, tid, environ):
    try:
        return TaskEndReason.success, run_task(task, tid)
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        return TaskEndReason.other_failure, e


class MultiProcessScheduler(LocalScheduler):

    def __init__(self, threads):
        LocalScheduler.__init__(self)
        self.threads = threads
        self.tasks = {}
        self.pool = None

    def submitTasks(self, tasks):
        if not tasks:
            return

        logger.info('Got a taskset with %d tasks: %s', len(tasks), tasks[0].rdd)

        total, self.finished, start = len(tasks), 0, time.time()

        def initializer():
            # when called on subprocess of multiprocessing's Pool,
            # default sighandler of SIGTERM will be called to quit gracefully,
            # and we ignore other signals to prevent dead lock.

            import signal
            from .context import _signals
            for sig in _signals:
                if sig == signal.SIGTERM:
                    signal.signal(sig, signal.SIG_DFL)
                else:
                    signal.signal(sig, signal.SIG_IGN)

        def callback(args):
            state, data = args
            logger.debug('task end: %s', state)

            if state == TaskEndReason.other_failure:
                logger.warning('task failed: %s', data)
                self.taskEnded(data.task_id, TaskEndReason.other_failure, result=None, accumUpdates=None)
                return

            tid, result, update = data
            task = self.tasks.pop(tid)
            self.finished += 1
            logger.info('Task %s finished (%d/%d)        \x1b[1A',
                        tid, self.finished, total)
            if self.finished == total:
                logger.info(
                    'TaskSet finished in %.1f seconds' + ' ' * 20,
                    time.time() - start)
            self.taskEnded(task, TaskEndReason.success, result, update)

        for task in tasks:
            logger.debug('put task async: %s', task)
            self.tasks[task.id] = task
            if not self.pool:
                # daemonic processes are not allowed to have children
                from dpark.broadcast import start_download_manager
                start_download_manager()
                self.pool = multiprocessing.Pool(
                    self.threads or 2,
                    initializer=initializer
                )

            self.pool.apply_async(run_task_in_process,
                                  [task, self.nextAttempId(), env.environ],
                                  callback=callback)

    def stop(self):
        if self.pool:
            self.pool.terminate()
            self.pool.join()
        logger.debug('process pool stopped')


def safe(f):
    def _(self, *a, **kw):
        with self.lock:
            r = f(self, *a, **kw)
        return r

    return _


class LogReceiver(object):

    def __init__(self, output):
        self.output = output
        self._started = False
        self.addr = None

    def start(self):
        ctx = zmq.Context()
        sock = ctx.socket(zmq.PULL)
        port = sock.bind_to_random_port('tcp://0.0.0.0')
        self._started = True

        def collect_log():
            while self._started:
                if sock.poll(1000, zmq.POLLIN):
                    line = sock.recv()
                    self.output.write(line)

            sock.close()
            ctx.destroy()

        spawn(collect_log)

        host = socket.gethostname()
        self.addr = 'tcp://%s:%d' % (host, port)
        logger.debug('log collecter start at %s', self.addr)

    def stop(self):
        self._started = False


class MesosScheduler(DAGScheduler):

    def __init__(self, master, options, webui_url=None):
        DAGScheduler.__init__(self)
        self.master = master
        self.cpus = options.cpus
        self.mem = options.mem
        self.task_per_node = options.parallel or 8
        self.group = options.group
        self.logLevel = options.logLevel
        self.options = options
        self.role = options.role
        self.color = options.color
        self.webui_url = webui_url
        self.started = False
        self.last_finish_time = 0
        self.last_task_launch_time = None
        self.is_suppressed = False
        self.isRegistered = False
        self.executor = None
        self.driver = None
        self.out_logger = LogReceiver(sys.stdout)
        self.err_logger = LogReceiver(sys.stderr)
        self.lock = threading.RLock()
        self.task_host_manager = TaskHostManager()
        self.init_tasksets()

    def init_tasksets(self):
        self.active_tasksets = {}
        self.ttid_to_agent_id = {}
        self.agent_id_to_ttids = {}

    def clear(self):
        DAGScheduler.clear(self)
        self.init_tasksets()

    def processHeartBeat(self):
        # no need in dpark now, just for compatibility with pymesos
        pass

    def start(self):
        self.out_logger.start()
        self.err_logger.start()

    def start_driver(self):
        name = '[dpark] ' + \
               os.path.abspath(sys.argv[0]) + ' ' + ' '.join(sys.argv[1:])
        if len(name) > 256:
            name = name[:256] + '...'
        framework = Dict()
        framework.user = getuser()
        if framework.user == 'root':
            raise Exception('dpark is not allowed to run as \'root\'')
        framework.name = name
        if self.role:
            framework.role = self.role
        framework.hostname = socket.gethostname()
        if self.webui_url:
            framework.webui_url = self.webui_url

        self.driver = MesosSchedulerDriver(
            self, framework, self.master, use_addict=True
        )
        self.driver.start()
        logger.debug('Mesos Scheudler driver started')

        self.started = True
        self.last_finish_time = time.time()

        def check():
            while self.started:
                with self.lock:
                    now = time.time()
                    if (not self.active_tasksets and
                            now - self.last_finish_time > MAX_IDLE_TIME):
                        logger.info('stop mesos scheduler after %d seconds idle',
                                    now - self.last_finish_time)
                        self.stop()
                        break

                    for taskset in self.active_tasksets.values():
                        if taskset.check_task_timeout():
                            self.requestMoreResources()
                time.sleep(1)

        spawn(check)

    @safe
    def registered(self, driver, frameworkId, masterInfo):
        self.isRegistered = True
        self.frameworkId = frameworkId.value
        logger.debug('connect to master %s:%s, registered as %s',
                     masterInfo.hostname, masterInfo.port, frameworkId.value)
        self.executor = self.getExecutorInfo(str(frameworkId.value))
        from dpark.utils.log import add_loghub
        _, self.loghub_dir = add_loghub(self.frameworkId)

    @safe
    def reregistered(self, driver, masterInfo):
        logger.warning('re-connect to mesos master %s:%s',
                       masterInfo.hostname, masterInfo.port)

    def disconnected(self, driver):
        logger.debug('framework is disconnected')

    def _get_container_image(self):
        return self.options.image

    @safe
    def getExecutorInfo(self, framework_id):
        info = Dict()
        info.framework_id.value = framework_id
        info.command.value = '%s %s' % (
            sys.executable,
            os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),
                    'executor.py'))
        )
        info.executor_id.value = env.get('DPARK_ID', 'default')
        info.command.environment.variables = variables = []

        v = Dict()
        variables.append(v)
        v.name = 'UID'
        v.value = str(os.getuid())

        v = Dict()
        variables.append(v)
        v.name = 'GID'
        v.value = str(os.getgid())

        container_image = self._get_container_image()
        if container_image:
            info.container.type = 'DOCKER'
            info.container.docker.image = container_image
            info.container.docker.parameters = parameters = []
            p = Dict()
            p.key = 'memory-swap'
            p.value = '-1'
            parameters.append(p)

            info.container.volumes = volumes = []
            for path in ['/etc/passwd', '/etc/group']:
                v = Dict()
                volumes.append(v)
                v.host_path = v.container_path = path
                v.mode = 'RO'

            for path in conf.MOOSEFS_MOUNT_POINTS:
                v = Dict()
                volumes.append(v)
                v.host_path = v.container_path = path
                v.mode = 'RW'

            for path in conf.DPARK_WORK_DIR.split(','):
                v = Dict()
                volumes.append(v)
                v.host_path = v.container_path = path
                v.mode = 'RW'

            def _mount_volume(_volumes, _host_path, _container_path, _mode):
                _v = Dict()
                _volumes.append(_v)
                _v.container_path = _container_path
                _v.mode = _mode
                if _host_path:
                    _v.host_path = _host_path

            if self.options.volumes:
                for volume in self.options.volumes.split(','):
                    fields = volume.split(':')
                    if len(fields) == 3:
                        host_path, container_path, mode = fields
                        mode = mode.upper()
                        assert mode in ('RO', 'RW')
                    elif len(fields) == 2:
                        host_path, container_path = fields
                        mode = 'RW'
                    elif len(fields) == 1:
                        container_path, = fields
                        host_path = ''
                        mode = 'RW'
                    else:
                        raise Exception('cannot parse volume %s', volume)
                    _mount_volume(volumes, host_path,
                                  container_path, mode)

        info.resources = resources = []

        mem = Dict()
        resources.append(mem)
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = EXECUTOR_MEMORY

        cpus = Dict()
        resources.append(cpus)
        cpus.name = 'cpus'
        cpus.type = 'SCALAR'
        cpus.scalar.value = EXECUTOR_CPUS

        Script = os.path.realpath(sys.argv[0])
        info.name = Script

        info.data = encode_data(marshal.dumps(
            (
                Script, os.getcwd(), sys.path, dict(os.environ),
                self.task_per_node, self.out_logger.addr, self.err_logger.addr,
                self.logLevel, self.color, env.environ
            )
        ))
        assert len(info.data) < (50 << 20), \
            'Info data too large: %s' % (len(info.data),)
        return info

    @safe
    def submitTasks(self, tasks):
        if not tasks:
            return

        rdd = tasks[0].rdd
        assert all(t.rdd is rdd for t in tasks)

        taskset = TaskSet(self, tasks, rdd.cpus or self.cpus, rdd.mem or self.mem,
                          rdd.gpus, self.task_host_manager)
        self.active_tasksets[taskset.id] = taskset
        stage_scope = ''
        try:
            from dpark.web.ui.views.rddopgraph import StageInfo
            stage_scope = StageInfo.idToRDDNode[tasks[0].rdd.id].scope.api_callsite
        except:
            pass

        stage = self.idToStage[tasks[0].stage_id]
        stage.num_try += 1
        stage.taskcounters.append(taskset.counter)
        logger.info(
            'Got taskset %s with %d tasks for stage: %d '
            'at scope[%s] and rdd:%s',
            taskset.id,
            len(tasks),
            tasks[0].stage_id,
            stage_scope,
            tasks[0].rdd)

        need_revive = self.started
        if not self.started:
            self.start_driver()
        while not self.isRegistered:
            self.lock.release()
            time.sleep(0.01)
            self.lock.acquire()

        if need_revive:
            self.requestMoreResources()

    def requestMoreResources(self):
        logger.debug('reviveOffers')
        self.driver.reviveOffers()
        self.is_suppressed = False

    @safe
    def resourceOffers(self, driver, offers):
        rf = Dict()
        now = time.time()
        if not self.active_tasksets or (all(taskset.counter.launched == taskset.counter.n
                                            for taskset in self.active_tasksets.values())
                                        and self.last_task_launch_time is not None
                                        and self.last_task_launch_time + conf.TIME_TO_SUPPRESS < now):
            logger.debug('suppressOffers')
            driver.suppressOffers()
            self.is_suppressed = True
            rf.refuse_seconds = 60 * 5
            for o in offers:
                driver.declineOffer(o.id, rf)
            return

        start = time.time()
        filter_offer = []
        for o in offers:
            try:
                if conf.ban(o.hostname):
                    logger.debug("skip offer on banned node: %s", o.hostname)
                    driver.declineOffer(o.id, filters=Dict(refuse_seconds=0xFFFFFFFF))
                    continue
            except:
                logger.exception("bad ban() func in dpark.conf")

            group = (
                    self.getAttribute(
                        o.attributes,
                        'group') or 'None')
            if (self.group or group.startswith(
                    '_')) and group not in self.group:
                driver.declineOffer(o.id, filters=Dict(refuse_seconds=0xFFFFFFFF))
                continue
            unavailability = o.get('unavailability')
            if (unavailability is not None and
                    sec2nanosec(time.time() + conf.DEFAULT_TASK_TIME) >= unavailability['start']['nanoseconds']):
                logger.debug('the host %s plan to maintain, so skip it', o.hostname)
                driver.declineOffer(o.id, filters=Dict(refuse_seconds=600))
                continue
            if self.task_host_manager.is_unhealthy_host(o.hostname):
                logger.warning('the host %s is unhealthy so skip it', o.hostname)
                driver.declineOffer(o.id, filters=Dict(refuse_seconds=1800))
                continue
            self.task_host_manager.register_host(o.hostname)
            filter_offer.append(o)
        offers = filter_offer
        cpus = [self.getResource(o.resources, 'cpus') for o in offers]
        gpus = [self.getResource(o.resources, 'gpus') for o in offers]
        mems = [self.getResource(o.resources, 'mem')
                - (o.agent_id.value not in self.agent_id_to_ttids
                   and EXECUTOR_MEMORY or 0)
                for o in offers]
        # logger.debug('get %d offers (%s cpus, %s mem, %s gpus), %d tasksets',
        #             len(offers), sum(cpus), sum(mems), sum(gpus), len(self.active_tasksets))

        mesos_tasks = {}
        tasks = {}
        max_create_time = 0
        for taskset in self.active_tasksets.values():
            while True:
                host_offers = {}
                for i, o in enumerate(offers):
                    if self.agent_id_to_ttids.get(o.agent_id.value, 0) >= self.task_per_node:
                        logger.debug('the task limit exceeded at host %s',
                                     o.hostname)
                        continue
                    if (mems[i] < self.mem + EXECUTOR_MEMORY
                            or cpus[i] < self.cpus + EXECUTOR_CPUS):
                        continue
                    host_offers[o.hostname] = (i, o)
                assigned_list = taskset.taskOffer(host_offers, cpus, mems, gpus)
                if not assigned_list:
                    break

                for i, o, t in assigned_list:
                    t0 = time.time()
                    mesos_task = self.createTask(o, t)
                    max_create_time = max(max_create_time, time.time() - t0)
                    mesos_tasks.setdefault(o.id.value, []).append(mesos_task)
                    tasks.setdefault(o.id.value, []).append(t)
                    logger.debug('dispatch %s into %s', t, o.hostname)
                    ttid = mesos_task.task_id.value
                    agent_id = o.agent_id.value
                    taskset.ttids.add(ttid)
                    self.ttid_to_agent_id[ttid] = agent_id
                    self.agent_id_to_ttids[agent_id] = self.agent_id_to_ttids.get(agent_id, 0) + 1
                    cpus[i] -= min(cpus[i], t.cpus)
                    mems[i] -= t.mem
                    gpus[i] -= t.gpus

        used = time.time() - start
        if used > 10:
            logger.warning('use too much time in resourceOffers: %.2fs, %d offers,'
                           'assigned %d tasks, max_create_time = %ds',
                           used,
                           len(offers),
                           len(mesos_tasks),
                           max_create_time)

        for o in offers:
            oid = o.id.value
            if oid in mesos_tasks:
                driver.launchTasks(o.id, mesos_tasks[oid])
                for task in tasks[oid]:
                    task.stage_time = time.time()
            else:
                driver.declineOffer(o.id)

        if tasks:
            self.last_task_launch_time = time.time()

        # logger.debug('reply with %d tasks, %s cpus %s mem %s gpus left',
        #            sum(len(ts) for ts in tasks.values()),
        #             sum(cpus), sum(mems), sum(gpus))

    @safe
    def inverseOffers(self, driver, offers):
        for o in offers:
            driver.acceptInverseOffers(o.id)

    @safe
    def offerRescinded(self, driver, offer_id):
        logger.debug('rescinded offer: %s', offer_id)
        if self.active_tasksets:
            self.requestMoreResources()

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def getAttribute(self, attrs, name):
        for r in attrs:
            if r.name == name:
                return r.text.value

    def createTask(self, o, t):
        task = Dict()
        tid = t.try_id
        task.name = 'task %s' % tid
        task.task_id.value = tid
        task.agent_id.value = o.agent_id.value
        task.data = encode_data(
            compress(cPickle.dumps((t, tid), -1))
        )
        task.executor = self.executor
        if len(task.data) > 1000 * 1024:
            logger.warning('task too large: %s %d',
                           t, len(task.data))

        assert len(task.data) < (50 << 20), \
            'Task data too large: %s' % (len(task.data),)

        resources = task.resources = []

        cpu = Dict()
        resources.append(cpu)
        cpu.name = 'cpus'
        cpu.type = 'SCALAR'
        cpu.scalar.value = t.cpus

        mem = Dict()
        resources.append(mem)
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = t.mem

        gpu = Dict()
        resources.append(gpu)
        gpu.name = 'gpus'
        gpu.type = 'SCALAR'
        gpu.scalar.value = t.gpus

        return task

    @safe
    def statusUpdate(self, driver, status):

        def plot_progresses():
            if self.color:
                total = len(self.active_tasksets)
                logger.info('\x1b[2K\x1b[J\x1b[1A')
                for i, taskset_id in enumerate(self.active_tasksets):
                    if i == total - 1:
                        ending = '\x1b[%sA' % total
                    else:
                        ending = ''

                    tasksets = self.active_tasksets[taskset_id]
                    tasksets.progress(ending)

        mesos_task_id = status.task_id.value
        state = status.state
        source = status.source
        reason = status.get('reason')

        msg = status.get('message')  # type: str
        if source == 'SOURCE_EXECUTOR' and msg:
            reason, msg = msg.split(':', 1)

        data = status.get('data')
        if data is not None:
            data = cPickle.loads(decode_data(data))

        logger.debug('status update: %s %s %s %s', mesos_task_id, state, reason, msg)

        ttid = TTID(mesos_task_id)

        taskset = self.active_tasksets.get(ttid.taskset_id)  # type: TaskSet

        if taskset is None:
            if state == TaskState.running:
                logger.debug('kill task %s as its taskset has gone', mesos_task_id)
                self.driver.killTask(Dict(value=mesos_task_id))
            else:
                logger.debug('ignore task %s as its taskset has gone', mesos_task_id)
            return

        if mesos_task_id not in taskset.ttids:
            logger.debug('ignore task %s as it has finished or failed, new msg: %s', mesos_task_id, (state, reason))
            return

        if state == TaskState.running:
            taskset.statusUpdate(ttid.task_id, ttid.task_try, state)
            if taskset.counter.finished == 0:
                plot_progresses()
            return

        # terminal state
        taskset.ttids.discard(mesos_task_id)
        if mesos_task_id in self.ttid_to_agent_id:
            agent_id = self.ttid_to_agent_id[mesos_task_id]
            if agent_id in self.agent_id_to_ttids:
                self.agent_id_to_ttids[agent_id] -= 1
            del self.ttid_to_agent_id[mesos_task_id]

        if state == TaskState.finished:
            try:
                result, accUpdate, task_stats = data
                flag, data = result
                if flag >= 2:
                    try:
                        data = urllib.request.urlopen(data).read()
                    except IOError:
                        # try again
                        data = urllib.request.urlopen(data).read()
                    flag -= 2
                data = decompress(data)
                if flag == 0:
                    result = marshal.loads(data)
                else:
                    result = cPickle.loads(data)
                taskset.statusUpdate(ttid.task_id, ttid.task_try, state,
                                     result=result, update=accUpdate, stats=task_stats)
                plot_progresses()
            except Exception as e:
                logger.warning('error when cPickle.loads(): %s, data:%s', e, len(data))
                state = TaskState.failed
                taskset.statusUpdate(ttid.task_id, ttid.task_try, state,
                                     reason=TaskEndReason.load_failed, message='load failed: %s' % e)
        else:
            exception = data if source == 'SOURCE_EXECUTOR' else None  # type: Optional[Exception]
            taskset.statusUpdate(ttid.task_id, ttid.task_try, state, reason, msg, result=exception)

    @safe
    def tasksetFinished(self, taskset):
        logger.debug('taskset %s finished', taskset.id)
        if taskset.id in self.active_tasksets:
            self.last_finish_time = time.time()
            for mesos_task_id in taskset.ttids:
                self.driver.killTask(Dict(value=mesos_task_id))
            del self.active_tasksets[taskset.id]
            if not self.active_tasksets:
                self.agent_id_to_ttids.clear()

    @safe
    def error(self, driver, message):
        logger.error('Mesos error message: %s', message)
        raise RuntimeError(message)

    # @safe
    def stop(self):
        if not self.started:
            return
        logger.debug('stop scheduler')
        self.started = False
        self.isRegistered = False
        self.driver.stop(False)
        self.driver.join()
        self.driver = None

        self.out_logger.stop()
        self.err_logger.stop()

    def defaultParallelism(self):
        return 16

    def frameworkMessage(self, driver, executor_id, agent_id, data):
        logger.warning('[agent %s] %s', agent_id.value, data)

    def executorLost(self, driver, executor_id, agent_id, status):
        logger.warning(
            'executor at %s %s lost: %s',
            agent_id.value,
            executor_id.value,
            status)
        self.agent_id_to_ttids.pop(agent_id.value, None)

    def slaveLost(self, driver, agent_id):
        logger.warning('agent %s lost', agent_id.value)
        self.agent_id_to_ttids.pop(agent_id.value, None)

    def killTask(self, task_id, num_try):
        tid = Dict()
        tid.value = TTID.make_ttid(task_id, num_try)
        self.driver.killTask(tid)

from __future__ import absolute_import
import marshal
import time
import six
from six.moves import range, cPickle
import os
import os.path

import dpark.conf
from dpark.env import env
from dpark.utils import compress, DparkUserFatalError
from dpark.utils.memory import ERROR_TASK_OOM
from dpark.utils.log import get_logger
from dpark.serialize import marshalable, load_func, dump_func, dumps, loads
from dpark.shuffle import LocalFileShuffle, get_serializer, Merger, pack_header

logger = get_logger(__name__)


class TTID(object):
    """"Task Try ID

    1.2_3.4: ttid

    1: stage id, start from 1
    2: stage try counter (for fetch fail), start from 1
    3: task partition of the stage, start from 0
    4: task retry counter

    1.2: taskset id
    1.2_3: task id
    """

    def __init__(self, ttid):
        self.ttid = ttid
        self.taskset_id, part_try = ttid.split("_")
        self.stage_id, self.stage_try = list(map(int, self.taskset_id.split(".")))
        self.part, self.task_try = list(map(int, part_try.split(".")))
        self.task_id = ttid.rsplit(".", 1)[0]

    @staticmethod
    def make_taskset_id(stage_id, stage_num_try):
        return "{}.{}".format(stage_id, stage_num_try)

    @staticmethod
    def make_task_id(taskset_id, partition):
        return "{}_{}".format(taskset_id, partition)

    @staticmethod
    def make_ttid(task_id, task_num_try):
        return "{}.{}".format(task_id, task_num_try)


class TaskTry(object):

    def __init__(self, reason):
        self.reason = reason
        self.status = [(TaskState.staging, time.time())]

    def append(self, st):
        self.status.append((st, time.time()))

    def __str__(self):
        return self.reason + ":" + ",".join(list(map(lambda x: "%s@%s" % (x[0], int(x[1])), self.status)))


class DAGTask(object):
    def __init__(self, stage_id, taskset_id, partition):
        self.id = TTID.make_task_id(taskset_id, partition)
        self.stage_id = stage_id
        self.taskset_id = taskset_id
        self.partition = partition
        self.num_try = 0
        self.reason_next = TaskReason.first
        self.tries = {}

        self.status = None
        self.time_used = 0  # sum up time of mulity retry

        self.mem = 0
        self.cpus = 0
        self.gpus = 0

        self.stage_time = 0
        self.start_time = 0

    def __repr__(self):
        return '<task %s>'.format(self.id)

    @property
    def try_id(self):
        return TTID.make_ttid(self.id, self.num_try)

    def try_next(self):
        self.num_try += 1
        self.tries[self.num_try] = TaskTry(self.reason_next)

    def update_status(self, status, num_try):
        self.status = status
        self.tries[num_try].append(status)

    def run(self, task_try_id):
        try:
            if self.mem != 0:
                env.meminfo.start(task_try_id, int(self.mem))
                if dpark.conf.MULTI_SEGMENT_DUMP:
                    env.meminfo.check = False
            return self._run(task_try_id)
        except KeyboardInterrupt as e:
            if self.mem != 0 and env.meminfo.oom:
                os._exit(ERROR_TASK_OOM)
            else:
                raise e
        finally:
            if self.mem != 0:
                env.meminfo.check = True
                env.meminfo.stop()

    def _run(self, task_try_id):
        raise NotImplementedError

    def preferredLocations(self):
        raise NotImplementedError


class ResultTask(DAGTask):
    def __init__(self, stage_id, taskset_id, partition, rdd, func, locs, outputId):
        DAGTask.__init__(self, stage_id, taskset_id, partition)
        self.rdd = rdd
        self.func = func
        self.split = rdd.splits[partition]
        self.locs = locs
        self.outputId = outputId

    def _run(self, task_id):
        logger.debug("run task %s: %s", task_id, self)
        t0 = time.time()
        res = self.func(self.rdd.iterator(self.split))
        env.task_stats.secs_all = time.time() - t0
        return res

    def preferredLocations(self):
        return self.locs

    def __repr__(self):
        partition = getattr(self, 'partition', None)
        rdd = getattr(self, 'rdd', None)
        return "<ResultTask(%s) of %s" % (partition, rdd)

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['func']
        del d['rdd']
        del d['split']
        return d, dumps(self.rdd), dump_func(self.func), dumps(self.split)

    def __setstate__(self, state):
        d, rdd, func, split = state
        self.__dict__.update(d)
        self.rdd = loads(rdd)
        self.func = load_func(func)
        self.split = loads(split)


class ShuffleMapTask(DAGTask):
    def __init__(self, stage_id, taskset_id, partition, rdd, dep, locs):
        DAGTask.__init__(self, stage_id, taskset_id, partition)
        self.rdd = rdd
        self.shuffleId = dep.shuffleId
        self.aggregator = dep.aggregator
        self.partitioner = dep.partitioner
        self.rddconf = dep.rddconf
        self.split = rdd.splits[partition]
        self.locs = locs

    def __repr__(self):
        shuffleId = getattr(self, 'shuffleId', None)
        partition = getattr(self, 'partition', None)
        rdd = getattr(self, 'rdd', None)
        return '<ShuffleTask(%s, %s) of %s>' % (shuffleId, partition, rdd)

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['rdd']
        del d['split']
        return d, dumps(self.rdd), dumps(self.split)

    def __setstate__(self, state):
        d, rdd, split = state
        self.__dict__.update(d)
        self.rdd = loads(rdd)
        self.split = loads(split)

    def preferredLocations(self):
        return self.locs

    def _run(self, task_id):
        mem_limit = env.meminfo.mem_limit_soft
        t0 = time.time()
        logger.debug("run task with shuffle_flag %r" % (self.rddconf,))
        rdd = self.rdd
        meminfo = env.meminfo
        n = self.partitioner.numPartitions
        get_partition = self.partitioner.getPartition
        merge_value = self.aggregator.mergeValue
        create_combiner = self.aggregator.createCombiner
        dumper_cls = SortMergeBucketDumper if self.rddconf.sort_merge else BucketDumper
        dumper = dumper_cls(self.shuffleId, self.partition, n, self.rddconf)
        buckets = [{} for _ in range(n)]
        env.meminfo.ratio = min(float(n) / (n + 1), env.meminfo.ratio)

        last_i = 0
        for i, item in enumerate(rdd.iterator(self.split)):
            try:
                try:
                    k, v = item
                except:
                    raise DparkUserFatalError("item of {} should be (k, v) pair, got: {}".format(rdd.scope.key, item))

                bucket = buckets[get_partition(k)]
                r = bucket.get(k, None)
                if r is not None:
                    bucket[k] = merge_value(r, v)
                else:
                    bucket[k] = create_combiner(v)

                if dpark.conf.MULTI_SEGMENT_DUMP and meminfo.rss > mem_limit:
                    _log = logger.info if dpark.conf.LOG_ROTATE else logger.debug
                    _log("dump rotate %d with %d kv: mem %d MB, sort limit %d MB, limit %d MB",
                         env.task_stats.num_dump_rotate + 1,
                         i - last_i,
                         int(meminfo.rss) >> 20,
                         mem_limit >> 20,
                         int(meminfo.mem) >> 20)
                    dumper.dump(buckets, False)
                    [buckets[j].clear() for j in range(n)]
                    env.meminfo.after_rotate()
                    mem_limit = env.meminfo.mem_limit_soft
                    last_i = i
            except ValueError as e:
                logger.exception('The ValueError exception: %s at %s', str(e), str(rdd.scope.api_callsite))
                raise

        t1 = time.time()
        dumper.dump(buckets, True)
        dumper.commit(self.aggregator)
        del buckets
        env.task_stats.bytes_dump += dumper.get_size()
        env.task_stats.num_dump_rotate += 1
        t = time.time()
        env.task_stats.secs_dump += t - t1
        env.task_stats.secs_all = t - t0

        return LocalFileShuffle.getServerUri()


class BucketDumper(object):

    def __init__(self, shuffle_id, map_id, num_reduce, rddconf):
        self.shuffle_id = shuffle_id
        self.map_id = map_id
        self.num_reduce = n = num_reduce
        self.rddconf = rddconf
        self.paths = [None for _ in range(n)]

        # stats
        self.sizes = [0 for _ in range(n)]
        self.num_dump = 0

    def _get_path(self, i, size):
        return LocalFileShuffle.getOutputFile(self.shuffle_id, self.map_id, i, size)

    def _get_path_check_mem(self, i, size):
        if size > 0:
            if env.meminfo.rss + size > env.meminfo.mem_limit_soft:
                p = self._get_path(i, -1)
            else:
                p = self._get_path(i, size)
                if p.startswith("/dev/shm"):
                    env.meminfo.add(size)
        else:
            p = self._get_path(i, size)
        return p

    def get_size(self):
        return sum(self.sizes)

    @staticmethod
    def _mk_tmp(s, seq=None):
        if seq is not None:
            return "%s.tmp.%d" % (s, seq)
        else:
            return "%s.tmp" % (s,)

    def _get_next_tmp(self, reduce_id, is_final, size):
        i = reduce_id
        if is_final and self.num_dump == 0:
            # check memory
            self.paths[i] = p = self._get_path_check_mem(i, size)
        else:
            # dump to disk!
            p = self.paths[i]
            if not p:
                self.paths[i] = p = self._get_path(i, -1)
        return self._mk_tmp(p)

    def _pre_commit(self, aggregator):
        pass

    def _dump_empty_bucket(self, i):
        p = self._get_path(i, 1)
        logger.debug("dump empty %s", p)
        self._dump_bucket(self._prepare([])[0], p)

    def commit(self, aggregator):
        self._pre_commit(aggregator)
        for i in range(self.num_reduce):
            path = self.paths[i]
            if path:
                old = self._mk_tmp(path)
                os.rename(old, path)  # comment it to test fetch (404)
            else:
                self._dump_empty_bucket(i)

    def _prepare(self, items):
        items = list(items)
        try:
            if marshalable(items):
                is_marshal, d = True, marshal.dumps(items)
            else:
                is_marshal, d = False, cPickle.dumps(items, -1)
        except ValueError:
            is_marshal, d = False, cPickle.dumps(items, -1)
        data = compress(d)
        size = len(data)
        return (is_marshal, data), size

    def _dump_bucket(self, data, path):
        is_marshal, data = data
        if self.num_dump == 0 and os.path.exists(path):
            logger.warning("remove old dump %s", path)
            os.remove(path)
        with open(path, 'ab') as f:
            f.write(pack_header(len(data), is_marshal, False))
            f.write(data)
        return len(data)

    def dump(self, buckets, is_final):
        t = time.time()
        for i, bucket_dict in enumerate(buckets):
            if not bucket_dict:
                continue
            items = six.iteritems(bucket_dict)
            data, exp_size = self._prepare(items)
            path = self._get_next_tmp(i, is_final, exp_size)
            logger.debug("dump %s", path)
            size = self._dump_bucket(data, path)
            self.sizes[i] += size

        self.num_dump += 1
        t = time.time() - t
        env.task_stats.secs_dump += t
        env.task_stats.num_dump_rotate += 1


class SortMergeBucketDumper(BucketDumper):

    def _pre_commit(self, aggregator):
        for i in range(self.num_reduce):
            out_path = self.paths[i]
            if out_path:
                tmp = self._mk_tmp(out_path)

                in_path = [self._mk_tmp(out_path, i) for i in range(self.num_dump + 1)]
                in_path = [p for p in in_path if os.path.exists(p)]
                if len(in_path) == 1:
                    os.rename(in_path[0], tmp)
                else:
                    inputs = [get_serializer(self.rddconf).load_stream(open(p))
                              for p in in_path]
                    rddconf = self.rddconf.dup(op=dpark.conf.OP_GROUPBY)
                    merger = Merger.get(rddconf, aggregator=aggregator, api_callsite=self.__class__.__name__)
                    merger.merge(inputs)
                    with open(tmp, 'w') as f:
                        get_serializer(self.rddconf).dump_stream(merger, f)
            else:
                self._dump_empty_bucket(i)

    def _get_next_tmp(self, i, is_final, size):
        p = self.paths[i]
        if not p:
            self.paths[i] = p = self._get_path(i, -1)

        return self._mk_tmp(p, self.num_dump)

    def _prepare(self, items):
        return items, -1

    def _dump_bucket(self, items, path):
        serializer = get_serializer(self.rddconf)
        with open(path, 'wb') as f:
            serializer.dump_stream(sorted(items), f)
            size = f.tell()
        return size


class TaskState:
    # non terminal states
    staging = 'TASK_STAGING'
    running = 'TASK_RUNNING'

    # terminal states
    finished = 'TASK_FINISHED'
    failed = 'TASK_FAILED'
    killed = 'TASK_KILLED'
    lost = 'TASK_LOST'
    error = 'TASK_ERROR'


class TaskEndReason:
    # generated on the executor
    success = 'FINISHED_SUCCESS'
    other_ecs = 'FAILED_UNKNOWN_EXITCODE'
    load_failed = 'FAILED_PICKLE_LOAD'
    other_failure = 'FAILED_OTHER_FAILURE'
    fetch_failed = 'FAILED_FETCH_FAILED'
    task_oom = 'FAILED_TASK_OOM'
    recv_sig = 'FAILED_RECV_SIG'
    recv_sig_kill = 'FAILED_RECV_SIG_KILL'
    launch_failed = 'FAILED_LAUNCH_FAILED'

    # generated on the agent
    mesos_cgroup_oom = 'REASON_CONTAINER_LIMITATION_MEMORY'

    @classmethod
    def maybe_oom(cls, reason):
        return reason in (cls.task_oom, cls.recv_sig_kill, cls.mesos_cgroup_oom)


class TaskReason:
    first = "first"
    run_timeout = "run_timeout"
    stage_timeout = "stage_timout"
    fail = "fail"


class FetchFailed(Exception):

    def __init__(self, serverUri, shuffleId, mapId, reduceId):
        self.serverUri = serverUri
        self.shuffleId = shuffleId
        self.mapId = mapId
        self.reduceId = reduceId

    def __str__(self):
        return '<FetchFailed(%s, %d, %d, %d)>' % (
            self.serverUri, self.shuffleId, self.mapId, self.reduceId
        )

    def __reduce__(self):
        return FetchFailed, (self.serverUri, self.shuffleId,
                             self.mapId, self.reduceId)


class OtherFailure(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return '<OtherFailure %s>' % self.message
from __future__ import absolute_import
from __future__ import print_function
import os
import os.path
import random
import six
from six.moves import urllib, queue, range, zip, reduce, cPickle as pickle
import marshal
import struct
import time
import heapq
import uuid
import itertools
from operator import itemgetter
from itertools import islice
from functools import wraps

try:
    import cStringIO as StringIO
except ImportError:
    from six import BytesIO as StringIO

import dpark.conf
from dpark.utils import compress, decompress, spawn, mkdir_p, atomic_file
from dpark.utils.memory import ERROR_TASK_OOM
from dpark.utils.log import get_logger
from dpark.env import env
from dpark.tracker import GetValueMessage, SetValueMessage
from dpark.utils.heaponkey import HeapOnKey
from dpark.dependency import AggregatorBase
from dpark.utils.nested_groupby import GroupByNestedIter, cogroup_no_dup

logger = get_logger(__name__)

# readable
F_MAPPING = {
    (True, True): b'M',
    (False, True): b'P',
    (True, False): b'm',
    (False, False): b'p'
}

F_MAPPING_R = dict([(v, k) for k, v in F_MAPPING.items()])


def pack_header(length, is_marshal, is_sorted):
    flag = F_MAPPING[(is_marshal, is_sorted)]
    return flag + struct.pack("I", length)


def unpack_header(head):
    l = len(head)
    if l != 5:
        raise IOError("fetch bad head length %d" % (l,))
    flag = head[:1]
    is_marshal, is_sorted = F_MAPPING_R[flag]
    length, = struct.unpack("I", head[1:5])
    return length, is_marshal, is_sorted


class LocalFileShuffle:

    @classmethod
    def get_tmp(cls):
        dirs = env.get('WORKDIR')
        d = random.choice(dirs[1:]) if dirs[1:] else dirs[0]
        mkdir_p(d)
        return os.path.join(d, 'shuffle-%s.tmp' % uuid.uuid4().hex)

    @classmethod
    def getOutputFile(cls, shuffle_id, input_id, output_id, datasize=0):
        """
            datasize < 0: disk first
            datasize > 0: memfirst
            datasize = 0: read only, use link
        """
        shuffleDir = env.get('WORKDIR')
        path = os.path.join(shuffleDir[0], str(shuffle_id), str(input_id))
        mkdir_p(path)
        p = os.path.join(path, str(output_id))
        if datasize != 0 and len(shuffleDir) > 1:
            use_disk = datasize < 0
            if datasize > 0:
                st = os.statvfs(path)
                free = st.f_bfree * st.f_bsize
                ratio = st.f_bfree * 1.0 / st.f_blocks
                use_disk = free < max(datasize, 1 << 30) or ratio < 0.66

            if use_disk:
                d2 = os.path.join(
                    random.choice(shuffleDir[1:]),
                    str(shuffle_id), str(input_id))
                mkdir_p(d2)
                p2 = os.path.join(d2, str(output_id))
                if os.path.exists(p):
                    os.remove(p)
                os.symlink(p2, p)
                if os.path.islink(p2):
                    os.unlink(p2)  # p == p2
                return p2
        return p

    @classmethod
    def getServerUri(cls):
        return env.get('SERVER_URI')


def write_buf(stream, buf, is_marshal):
    buf = compress(buf)
    size = len(buf)
    stream.write(pack_header(size, is_marshal, True))
    stream.write(buf)
    return size + 4


class AutoBatchedSerializer(object):
    """
    Choose the size of batch automatically based on the size of object
    """

    size_loaded = 0

    def __init__(self, best_size=1 << 17):
        self.best_size = best_size
        self.max_num = 0
        self.max_size = 0
        self.use_marshal = True
        self.num_batch = 0
        self.file_size = 0

    def load_stream(self, stream):
        while True:
            head = stream.read(5)
            if not head:
                return
            length, is_marshal, is_sorted = unpack_header(head)
            assert (is_sorted)
            buf = stream.read(length)
            if len(buf) < length:
                raise IOError("length not match: expected %d, but got %d" % (length, len(buf)))

            buf = decompress(buf)
            AutoBatchedSerializer.size_loaded += len(buf)
            if is_marshal:
                vs = marshal.loads(buf)
            else:
                vs = pickle.loads(buf)
            for v in vs:
                yield v

    def dump_stream(self, iterator, stream):
        self._dump_stream(iter(iterator), stream)
        logger.debug("max batch num = %d, max batch size = %d", self.max_num, self.max_size)

    def _dump_stream(self, iterator, stream):
        batch_num = 1

        while True:
            vs = list(itertools.islice(iterator, batch_num))
            self.num_batch += 1
            if not vs:
                break
            batch_num = self._dump_batch(stream, vs, batch_num)

    def _dump_batch(self, stream, vs, batch_num):
        if self.use_marshal:
            try:
                buf = marshal.dumps(vs)
            except:
                buf = pickle.dumps(vs, -1)
                self.use_marshal = False
        else:
            buf = pickle.dumps(vs, -1)

        mem_size = len(buf)
        self.file_size += write_buf(stream, buf, self.use_marshal)

        if mem_size < self.best_size:
            batch_num *= 2
            if batch_num > self.max_num:
                self.max_num = batch_num
        else:
            if mem_size > self.best_size * 2 and batch_num > 1:
                batch_num //= 2
            if mem_size > self.max_size:
                self.max_size = mem_size
        return batch_num


class GroupByAutoBatchedSerializer(AutoBatchedSerializer):

    def _dump_stream(self, iterator, stream):
        batch_num = 1

        def _batching():
            batch = []
            num = 0

            for k, vs in iterator:
                n = len(vs)
                if n + num <= batch_num:
                    batch.append((k, vs))
                    num += n
                else:
                    if batch:
                        yield batch
                        batch = []
                        num = 0
                    if n >= batch_num:
                        sub_it = iter(vs)
                        while True:
                            sub_vs = list(itertools.islice(sub_it, batch_num))
                            if not sub_vs:
                                break
                            yield [(k, sub_vs)]
                    else:
                        batch.append((k, vs))
                        num = n
            if batch:
                yield batch

        for k_vs in _batching():
            self.num_batch += 1
            batch_num = self._dump_batch(stream, k_vs, batch_num)


def get_serializer(rddconf):
    if rddconf.iter_group and (rddconf.is_groupby or rddconf.is_cogroup):
        return GroupByAutoBatchedSerializer()
    else:
        return AutoBatchedSerializer()


def fetch_with_retry(f):
    MAX_RETRY = 3
    RETRY_INTERVALS = [1, 10]

    @wraps(f)
    def _(self):
        self.num_batch_done = 0
        while True:
            try:
                for items in islice(f(self), self.num_batch_done, None):
                    self.num_batch_done += 1
                    yield items
                if self.num_retry > 0:
                    logger.info("Fetch retry %d success for url %s, num_batch %d ", self.num_retry, self.url,
                                self.num_batch_done)
                break
            except Exception as e:
                self.num_retry += 1
                msg = "Fetch failed for url %s, tried %d/%d times. Exception: %s. " % (
                self.url, self.num_retry, MAX_RETRY, e)
                fail_fast = False
                emsg = str(e)
                if any([emsg.find(s) >= 0 for s in ["404"]]):
                    # "many open file",
                    fail_fast = True
                    msg += "no need to retry."
                if fail_fast or self.num_retry >= MAX_RETRY:
                    logger.warning(msg)
                    from dpark.task import FetchFailed
                    raise FetchFailed(self.uri, self.sid, self.mid, self.rid)
                else:
                    sleep_time = RETRY_INTERVALS[self.num_retry - 1]
                    msg += "sleep %d secs" % (sleep_time,)
                    logger.debug(msg)
                    time.sleep(sleep_time)

    return _


class RemoteFile(object):
    num_open = 0

    def __init__(self, uri, shuffle_id, map_id, reduce_id):
        self.uri = uri
        self.sid = shuffle_id
        self.mid = map_id
        self.rid = reduce_id
        if uri == LocalFileShuffle.getServerUri():
            # urllib can open local file
            self.url = 'file://' + LocalFileShuffle.getOutputFile(shuffle_id, map_id, reduce_id)
        else:
            self.url = "%s/%d/%d/%d" % (uri, shuffle_id, map_id, reduce_id)
        # self.url = self.url.replace("5055", "5075")  # test fetch retry
        logger.debug("fetch %s", self.url)

        self.num_retry = 0
        self.num_batch_done = 0

    def open(self):
        f = urllib.request.urlopen(self.url)
        if f.code == 404:
            f.close()
            raise IOError("not found")
        exp_size = int(f.headers['content-length'])
        return f, exp_size

    @fetch_with_retry
    def unsorted_batches(self):
        f = None
        # TEST_RETRY = True
        try:
            f, exp_size = self.open()
            total_size = 0

            while True:
                head = f.read(5)
                if len(head) == 0:
                    break
                length, is_marshal, is_sorted = unpack_header(head)
                assert (not is_sorted)
                total_size += length + 5
                d = f.read(length)
                if length != len(d):
                    raise IOError(
                        "length not match: expected %d, but got %d" %
                        (length, len(d)))
                d = decompress(d)
                if is_marshal:
                    items = marshal.loads(d)
                else:
                    try:
                        items = pickle.loads(d)
                    except:
                        time.sleep(1)
                        items = pickle.loads(d)
                yield items

                # if TEST_RETRY and self.num_retry == 0:
                #    raise Exception("test_retry")

            if total_size != exp_size:
                raise IOError(
                    "fetch size not match: expected %d, but got %d" %
                    (exp_size, total_size))

            env.task_stats.bytes_fetch += exp_size
        finally:
            if f:
                f.close()

    @fetch_with_retry
    def sorted_items(self):
        f = None
        try:
            serializer = AutoBatchedSerializer()
            self.num_open += 1
            f, exp_size = self.open()
            for obj in serializer.load_stream(f):
                yield obj
            env.task_stats.bytes_fetch += exp_size
        finally:
            # rely on GC to close if generator not exhausted
            # so Fetcher must not be an attr of RDD
            if f:
                f.close()
            self.num_open -= 1


class ShuffleFetcher(object):

    @classmethod
    def _get_uris(cls, shuffle_id):
        uris = env.mapOutputTracker.getServerUris(shuffle_id)
        mapid_uris = list(zip(list(range(len(uris))), uris))
        random.shuffle(mapid_uris)
        return mapid_uris

    @classmethod
    def get_remote_files(cls, shuffle_id, reduce_id):
        uris = cls._get_uris(shuffle_id)
        return [RemoteFile(uri, shuffle_id, map_id, reduce_id) for map_id, uri in uris]

    def fetch(self, shuffle_id, reduce_id, merge_func):
        raise NotImplementedError

    def stop(self):
        pass


class SimpleShuffleFetcher(ShuffleFetcher):

    def fetch(self, shuffle_id, reduce_id, merge_func):
        logger.debug(
            "Fetching outputs for shuffle %d, reduce %d",
            shuffle_id, reduce_id)
        for f in self.get_remote_files():
            for items in f.unsorted_batches():
                merge_func(items)


class ParallelShuffleFetcher(SimpleShuffleFetcher):

    def __init__(self, nthreads):
        self.nthreads = nthreads
        self._started = False

    def start(self):
        if self._started:
            return

        self._started = True
        self.requests = queue.Queue()
        self.results = queue.Queue(self.nthreads)
        self.threads = [spawn(self._fetch_thread)
                        for i in range(self.nthreads)]

    def _fetch_thread(self):
        from dpark.task import FetchFailed

        while True:
            f = self.requests.get()
            if f is None:
                break
            try:
                for items in f.unsorted_batches():
                    if not self._started:
                        break
                    self.results.put((items, f.mid))
                if not self._started:
                    break
                self.results.put(1)
            except FetchFailed as e:
                if not self._started:
                    break
                self.results.put(e)
                break

    def fetch(self, shuffle_id, reduce_id, merge_func):
        self.start()
        files = self.get_remote_files(shuffle_id, reduce_id)
        for f in files:
            self.requests.put(f)

        t = time.time()
        from dpark.task import FetchFailed
        num_done = 0
        while num_done < len(files):
            r = self.results.get()
            if r == 1:
                num_done += 1
            elif isinstance(r, FetchFailed):
                self.stop()
                raise r
            else:
                items, map_id = r
                merge_func(items, map_id)

        env.task_stats.secs_fetch = time.time() - t

    def stop(self):
        if not self._started:
            return

        logger.debug("stop parallel shuffle fetcher ...")
        self._started = False
        while not self.requests.empty():
            self.requests.get_nowait()
        for i in range(self.nthreads):
            self.requests.put(None)

        N = 5
        for _ in range(N):
            while not self.results.empty():
                self.results.get_nowait()
            for t in self.threads:
                t.join(1)
            if all([not t.isAlive() for t in self.threads]):
                return
        else:
            logger.info("FIXME: fail to join fetcher threads")


class SortShuffleFetcher(ShuffleFetcher):

    def get_iters(self, shuffle_id, reduce_id):
        return [f.sorted_items() for f in self.get_remote_files(shuffle_id, reduce_id)]

    def fetch(self, shuffle_id, reduce_id, merge_func):
        merge_func(self.get_iters(shuffle_id, reduce_id))


def heap_merged(items_lists, combiner):
    heap = []

    def pushback(_it, _i):
        try:
            _k, _v = next(_it)
            # put i before value, so do not compare the value
            heapq.heappush(heap, (_k, i, _v))
        except StopIteration:
            pass

    for i, it in enumerate(items_lists):
        pushback(it, i)

    if not heap:
        return

    last_key, i, last_value = heapq.heappop(heap)
    pushback(items_lists[i], i)

    while heap:
        k, i, v = heapq.heappop(heap)
        if k != last_key:
            yield last_key, last_value
            last_key, last_value = k, v
        else:
            last_value = combiner(last_value, v)
        pushback(items_lists[i], i)

    yield last_key, last_value


class SortedItemsOnDisk(object):

    def __init__(self, items, rddconf):
        self.path = path = LocalFileShuffle.get_tmp()
        with atomic_file(path, bufsize=4096) as f:
            if not isinstance(items, list):
                items = list(items)
            items.sort(key=itemgetter(0))
            serializer = get_serializer(rddconf)
            serializer.dump_stream(items, f)
            self.size = f.tell()
            self.num_batch = serializer.num_batch

    def __iter__(self):
        serializer = AutoBatchedSerializer()
        with open(self.path, 'rb') as f:
            for obj in serializer.load_stream(f):
                yield obj

    def __dealloc__(self):
        try:
            if os.path.exists(self.path):
                os.remove(self.path)
        except Exception:
            pass


class Merger(object):

    def __init__(self, rddconf, aggregator=None, size=None, api_callsite=None):
        self.rddconf = rddconf
        self.aggregator = aggregator
        self.size = size
        self.api_callsite = api_callsite

    @classmethod
    def get(cls, rddconf, aggregator=None, size=0, api_callsite=None):
        if rddconf.sort_merge:
            # all mergers keep order
            c = SortMerger
            if rddconf.is_cogroup:
                if rddconf.iter_group:
                    c = IterCoGroupSortMerger
            elif rddconf.is_groupby:
                if rddconf.iter_group:
                    c = IterGroupBySortMerger
        else:
            c = DiskHashMerger
            if rddconf.is_groupby:
                if rddconf.ordered_group:
                    c = OrderedGroupByDiskHashMerger
            elif rddconf.is_cogroup:
                if rddconf.ordered_group:
                    c = OrderedCoGroupDiskHashMerger
                else:
                    c = CoGroupDiskHashMerger
        logger.debug("%s %s", c, rddconf)
        return c(rddconf, aggregator, size, api_callsite)


class DiskHashMerger(Merger):

    def __init__(self, rddconf, aggregator=None, size=None, api_callsite=None):
        Merger.__init__(self, rddconf, aggregator, size, api_callsite)

        self.combined = {}

        self.use_disk = rddconf.disk_merge
        if self.use_disk:
            env.meminfo.ratio = rddconf.dump_mem_ratio

        self.archives = []

        self.rotate_time = 0
        self.last_rotate_ts = time.time()
        self.rotate_num = 0
        self.total_size = 0

    def _rotate(self):
        total_size = self.total_size
        t0 = time.time()
        time_since_last = t0 - self.last_rotate_ts
        dict_size = len(self.combined)
        rss_before = env.meminfo.rss_rt
        size = self._dump()
        self.total_size += size
        rss_after = env.meminfo.rss_rt
        t1 = time.time()
        rotate_time = t1 - t0
        self.last_rotate_ts = t1
        self.rotate_time += rotate_time
        self.rotate_num += 1
        max_rotate = 1000
        if self.rotate_num > max_rotate:
            logger.warnging('more than %d rotation. exit!', max_rotate)
            os._exit(ERROR_TASK_OOM)

        env.meminfo.after_rotate()

        _log = logger.info if dpark.conf.LOG_ROTATE else logger.debug
        _log('rotate %d: use %.2f sec, since last %.2f secs, dict_size 0x%x,'
             'mem %d -> %d MB, disk size +%d = %d MB',
             self.rotate_num, rotate_time, time_since_last, dict_size,
             rss_before >> 20, rss_after >> 20, size >> 20, total_size >> 20)

        return env.meminfo.mem_limit_soft

    def disk_size(self):
        return sum([a.size for a in self.archives])

    def _dump(self):
        import gc
        items = self.combined.items()
        f = SortedItemsOnDisk(items, self.rddconf)
        self.archives.append(f)
        del items

        if self.rddconf.is_groupby:
            for v in self.combined.itervalues():
                del v[:]
        self.combined.clear()
        gc.collect()
        return f.size

    def merge(self, items, map_id, dep_id=0):
        mem_limit = env.meminfo.mem_limit_soft
        use_disk = self.use_disk
        try:
            env.meminfo.check = use_disk
            self._merge(items, map_id, dep_id, use_disk, env.meminfo, mem_limit)
        finally:
            env.meminfo.check = True

    def _get_merge_function(self):
        return self.aggregator.mergeCombiners

    def _merge(self, items, map_id, dep_id, use_disk, meminfo, mem_limit):
        combined = self.combined
        merge_combiner = self.aggregator.mergeCombiners
        for k, v in items:
            o = combined.get(k)
            combined[k] = merge_combiner(o, v) if o is not None else v

            if use_disk and meminfo.rss > mem_limit:
                mem_limit = self._rotate()

    def __iter__(self):
        if not self.archives:
            return six.iteritems(self.combined)
        items = self.combined.items()
        items.sort(key=itemgetter(0))
        combined = items
        self.archives.append(iter(combined))
        iters = list(map(iter, self.archives))
        if self.rddconf.is_groupby and self.rddconf.iter_group:
            heap = HeapOnKey(key=lambda x: x[0], min_heap=True)
            it = GroupByNestedIter(heap.merge(iters), "")
        else:
            it = heap_merged(iters, self._get_merge_function())
        return it


class OrderedGroupByDiskHashMerger(DiskHashMerger):

    def _merge(self, items, map_id, dep_id, use_disk, meminfo, mem_limit):
        combined = self.combined
        for k, v in items:
            o = combined.get(k)
            iv = (map_id, v)
            if o is None:
                combined[k] = [iv]
            else:
                o.append(iv)
            if use_disk and meminfo.rss > mem_limit:
                mem_limit = self._rotate()

    def __iter__(self):
        it = DiskHashMerger.__iter__(self)
        merge_combiner = self.aggregator.mergeCombiners
        for k, ivs in it:
            ivs.sort(key=itemgetter(0))
            cb = reduce(merge_combiner, (v for _, v in ivs))
            yield k, cb


class CoGroupDiskHashMerger(DiskHashMerger):

    def __init__(self, rddconf, aggregator=None, size=None, api_callsite=None):
        DiskHashMerger.__init__(self, rddconf, aggregator, size, api_callsite)
        self.direct_upstreams = []

    def _get_merge_function(self):
        def _merge(x, y):
            for i in range(self.size):
                x[i].extend(y[i])
            return x

        return _merge

    def _merge(self, items, map_id, dep_id, use_disk, meminfo, mem_limit):
        combined = self.combined
        if map_id < 0:
            for k, v in items:
                t = combined.get(k)
                if t is None:
                    combined[k] = t = tuple([[] for _ in range(self.size)])
                t[dep_id].append(v)
                if use_disk and meminfo.rss > mem_limit:
                    mem_limit = self._rotate()
        else:
            for k, vs in items:
                t = combined.get(k)
                if t is None:
                    combined[k] = t = tuple([[] for _ in range(self.size)])
                t[dep_id].extend(vs)


class OrderedCoGroupDiskHashMerger(CoGroupDiskHashMerger):

    def _merge(self, items, map_id, dep_id, use_disk, meminfo, mem_limit):
        combined = self.combined
        if map_id < 0:
            self.upstreams.append(dep_id)
            for k, v in items:
                t = combined.get(k)
                if t is None:
                    combined[k] = t = tuple([[] for _ in range(self.size)])
                t[dep_id].append(v)
                if use_disk and meminfo.rss > mem_limit:
                    mem_limit = self._rotate()
        else:
            for k, vs in items:
                t = combined.get(k)
                if t is None:
                    combined[k] = t = tuple([[] for _ in range(self.size)])
                t[dep_id].append((map_id, vs))
                if use_disk and meminfo.rss > mem_limit:
                    mem_limit = self._rotate()

    def __iter__(self):
        it = DiskHashMerger.__iter__(self)

        direct_upstreams = self.direct_upstreams
        for k, groups in it:
            t = list([[] for _ in range(self.size)])
            for i, g in enumerate(groups):
                if g:
                    if i in direct_upstreams:
                        t[i] = g
                    else:
                        g.sort(key=itemgetter(0))
                        g1 = []
                        for _, vs in g:
                            g1.extend(vs)
                        t[i] = g1
            yield k, tuple(t)


class SortMergeAggregator(AggregatorBase):

    def __init__(self, mergeCombiners):
        # each item is a combiner
        self.mergeValue = mergeCombiners

    def createCombiner(self, v):
        return v


class CoGroupSortMergeAggregator(AggregatorBase):
    def __init__(self, size):
        self.size = size

    def createCombiner(self, v):
        # v = (rdd_index, value)
        values = tuple([[] for _ in range(self.size)])
        values[v[0]].extend(v[1])
        return values

    def mergeValue(self, c, v):
        c[v[0]].extend(v[1])
        return c


class SortMerger(Merger):

    def __init__(self, rddconf, aggregator=None, size=None, api_callsite=None):
        Merger.__init__(self, rddconf, aggregator, size, api_callsite)

        if aggregator:
            self.aggregator = SortMergeAggregator(self.aggregator.mergeCombiners)
        else:
            self.aggregator = CoGroupSortMergeAggregator(size)
        self.combined = iter([])

        self.paths = []

    def _merge_sorted(self, iters):
        heap = HeapOnKey(key=lambda x: x[0], min_heap=True)
        merged = heap.merge(iters)
        return self.aggregator.aggregate_sorted(merged)

    def _disk_merge_sorted(self, iters):
        t = time.time()
        s = AutoBatchedSerializer()
        iters = iter(iters)
        while True:
            batch = list(islice(iters, 100))
            if not batch:
                break
            path = LocalFileShuffle.get_tmp()
            with open(path, 'wb') as f:
                s.dump_stream(self._merge_sorted(batch), f)
            self.paths.append(path)
            env.task_stats.num_fetch_rotate += 1

        files = [s.load_stream(open(p)) for p in self.paths]
        env.task_stats.secs_fetch = time.time() - t
        return self._merge_sorted(files)

    def merge(self, iters):
        if self.rddconf.disk_merge or len(iters) > dpark.conf.MAX_OPEN_FILE:
            merged = self._disk_merge_sorted(iters)
        else:
            merged = self._merge_sorted(iters)

        self.combined = merged

    def __iter__(self):
        return self.combined


class IterGroupBySortMerger(SortMerger):

    def _merge_sorted(self, iters):
        heap = HeapOnKey(key=lambda x: x[0], min_heap=True)
        return GroupByNestedIter(heap.merge(iters), self.api_callsite)


class IterCoGroupSortMerger(SortMerger):

    def _merge_sorted(self, iters):
        # each item like <key, values>
        return cogroup_no_dup(list(map(iter, iters)))


class BaseMapOutputTracker(object):

    def registerMapOutputs(self, shuffle_id, locs):
        pass

    def getServerUris(self):
        pass

    def stop(self):
        pass


class MapOutputTracker(BaseMapOutputTracker):

    def __init__(self):
        self.client = env.trackerClient
        logger.debug("MapOutputTracker started")

    def registerMapOutputs(self, shuffle_id, locs):
        self.client.call(SetValueMessage('shuffle:%s' % shuffle_id, locs))

    def getServerUris(self, shuffle_id):
        locs = self.client.call(GetValueMessage('shuffle:%s' % shuffle_id))
        logger.debug("Fetch done: %s", locs)
        return locs


def test():
    from dpark.utils import compress
    import logging
    logging.basicConfig(level=logging.DEBUG)
    from dpark.env import env
    env.start()

    path = LocalFileShuffle.getOutputFile(1, 0, 0)
    d = compress(pickle.dumps({'key': 'value'}, -1))
    f = open(path, 'w')
    f.write(pack_header(len(d), False, False) + d)
    f.close()

    uri = LocalFileShuffle.getServerUri()
    env.mapOutputTracker.registerMapOutputs(1, [uri])
    fetcher = SimpleShuffleFetcher()

    def func(it):
        k, v = next(it)
        assert k == 'key'
        assert v == 'value'

    fetcher.fetch(1, 0, func)

    tracker = MapOutputTracker()
    tracker.registerMapOutputs(2, [None, uri, None, None, None])
    assert tracker.getServerUris(2) == [None, uri, None, None, None]
    tracker.stop()


if __name__ == '__main__':
    from dpark.shuffle import test

    test()

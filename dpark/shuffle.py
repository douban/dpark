from __future__ import absolute_import
from __future__ import print_function
import os
import os.path
import sys
import time
import random
from six.moves import urllib
import marshal
import struct
import time
import six.moves.cPickle as pickle
import gzip
import zlib
import six.moves.queue
import heapq
import uuid
import gc
import six
import itertools
from six.moves import range
from six.moves import zip

try:
    import cStringIO as StringIO
except ImportError:
    from six import BytesIO as StringIO

from dpark.util import decompress, spawn, mkdir_p, atomic_file, get_logger
from dpark.env import env
from dpark.tracker import GetValueMessage, SetValueMessage
from dpark.heaponkey import HeapOnKey
from dpark.dependency import AggregatorBase
from dpark.nested_groupby import GroupByNestedIter, cogroup_no_dup


MAX_SHUFFLE_MEMORY = 2000  # 2 GB

logger = get_logger(__name__)


class LocalFileShuffle:
    @classmethod
    def getOutputFile(cls, shuffle_id, input_id, output_id, datasize=0):
        shuffleDir = env.get('WORKDIR')
        path = os.path.join(shuffleDir[0], str(shuffle_id), str(input_id))
        mkdir_p(path)
        p = os.path.join(path, str(output_id))
        if datasize > 0 and len(shuffleDir) > 1:
            # datasize > 0 means its writing
            st = os.statvfs(path)
            free = st.f_bfree * st.f_bsize
            ratio = st.f_bfree * 1.0 / st.f_blocks
            if free < max(datasize, 1 << 30) or ratio < 0.66:
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


class BadShuffleStreamException(Exception):
    pass


def write_buf(stream, buf, is_mashal):
    buf = zlib.compress(buf, 1)
    size = len(buf)
    stream.write(struct.pack("!I?", size, is_mashal))
    stream.write(buf)


class AutoBatchedSerializer(object):
    """
    Choose the size of batch automatically based on the size of object
    """

    size_loaded = 0

    def __init__(self, best_size=1 << 16):
        self.best_size = best_size
        self.max_num = 0
        self.max_size = 0
        self.use_marshal = True

    def load_stream(self, stream):
        while True:
            head = stream.read(5)
            if not head:
                return
            length, is_mashal = struct.unpack("!I?", head)
            buf = stream.read(length)
            if len(buf) < length:
                raise BadShuffleStreamException
            buf = zlib.decompress(buf)
            AutoBatchedSerializer.size_loaded += len(buf)
            if is_mashal:
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
            if not vs:
                break
            batch_num = self._dump_batch(stream, vs, batch_num)

    def _dump_batch(self, stream, vs, batch_num):
        if self.use_marshal:
            try:
                buf = marshal.dump(vs)
            except:
                buf = pickle.dumps(vs, -1)
                self.use_marshal = False
        else:
            buf = pickle.dumps(vs, -1)

        mem_size = len(buf)
        write_buf(stream, buf, self.use_marshal)

        if mem_size < self.best_size:
            batch_num *= 2
            if batch_num > self.max_num:
                self.max_num = batch_num
        else:
            if mem_size > self.best_size * 4 and batch_num > 1:
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
                        while(True):
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
            batch_num = self._dump_batch(stream, k_vs, batch_num)


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
        logger.debug("fetch %s", self.url)
        self.max_retry = 3
        self.num_retry = 0

    def _fetch_dct(self):
        f = urllib.request.urlopen(self.url)
        if f.code == 404:
            f.close()
            raise IOError("not found")

        d = f.read()
        flag = d[:1]
        length, = struct.unpack("I", d[1:5])
        if length != len(d):
            raise ValueError(
                "length not match: expected %d, but got %d" %
                (length, len(d)))
        env.task_stats.bytes_shuffle_read += length
        d = decompress(d[5:])
        f.close()
        if flag == b'm':
            d = marshal.loads(d)
        elif flag == b'p':
            d = pickle.loads(d)
        else:
            raise ValueError("invalid flag")
        return d

    def fetch_dct(self):
        while True:
            try:
                return self._fetch_dct()
            except Exception as e:
                self.on_fail(e)

    def iter_sorted(self):
        n_skip = 0
        while True:
            try:
                i = 0
                for i, obj in enumerate(self._iter_sorted()):
                    if i < n_skip:
                        continue
                    yield obj
                return
            except BadShuffleStreamException:
                raise
            except Exception as e:
                n_skip = max(i, n_skip)
                self.on_fail(e)  # may raise exception

    def _iter_sorted(self):
        f = urllib.request.urlopen(self.url)
        if f.code == 404:
            f.close()
            raise IOError("not found")

        self.num_open += 1
        serializer = AutoBatchedSerializer()
        try:
            for obj in serializer.load_stream(f):
                yield obj
        finally:
            # rely on GC to close if generator not exhausted
            # so Fetcher must not be an attr of RDD
            f.close()
            self.num_open -= 1

    def on_fail(self, e):
        self.num_retry += 1
        msg = "Fetch failed for url %s, %d/%d. exception: %s. " % (self.url, self.num_retry, self.max_retry, e)
        fail_fast = False
        if isinstance(e, IOError) and str(e).find("many open file") >= 0:
            fail_fast = True
        if fail_fast or self.num_retry >= self.max_retry:
            msg += "GIVE UP!"
            logger.warning(msg)
            from dpark.schedule import FetchFailed
            raise FetchFailed(self.uri, self.sid, self.mid, self.rid)
        else:
            logger.debug(msg)
            time.sleep(2 ** self.num_retry * 0.1)


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
            merge_func(six.iteritems(f.fetch_dct()))


class ParallelShuffleFetcher(SimpleShuffleFetcher):

    def __init__(self, nthreads):
        self.nthreads = nthreads
        self._started = False

    def start(self):
        if self._started:
            return

        self._started = True
        self.requests = six.moves.queue.Queue()
        self.results = six.moves.queue.Queue(self.nthreads)
        self.threads = [spawn(self._worker_thread)
                        for i in range(self.nthreads)]

    def _worker_thread(self):
        from dpark.schedule import FetchFailed
        while True:
            f = self.requests.get()
            if f is None:
                break
            try:
                self.results.put(f.fetch_dct())
            except FetchFailed as e:
                self.results.put(e)

    def fetch(self, shuffle_id, reduce_id, func):
        self.start()
        st = time.time()
        files = self.get_remote_files(shuffle_id, reduce_id)
        for f in files:
            self.requests.put(f)

        from dpark.schedule import FetchFailed
        for i in range(len(files)):
            r = self.results.get()
            if isinstance(r, FetchFailed):
                self.stop()  # restart
                raise r

            func(six.iteritems(r))
        env.task_stats.merge_time = time.time() - st

    def stop(self):
        if not self._started:
            return

        logger.debug("stop parallel shuffle fetcher ...")
        self._started = False
        while not self.requests.empty():
            self.requests.get_nowait()
        while not self.results.empty():
            self.results.get_nowait()
        for i in range(self.nthreads):
            self.requests.put(None)
        for t in self.threads:
            t.join()


class SortedShuffleFetcher(ShuffleFetcher):

    def get_iters(self, shuffle_id, reduce_id):
        return [f.iter_sorted() for f in self.get_remote_files(shuffle_id, reduce_id)]

    def fetch(self, shuffle_id, reduce_id, merge_func):
        merge_func(self.get_iters(shuffle_id, reduce_id))


class Merger(object):

    def __init__(self, aggregator):
        self.createCombiner = aggregator.createCombiner
        self.mergeCombiners = aggregator.mergeCombiners
        self.combined = {}

    def merge(self, items):
        combined = self.combined
        mergeCombiners = self.mergeCombiners
        for k, v in items:
            o = combined.get(k)
            combined[k] = mergeCombiners(o, v) if o is not None else v

    def __iter__(self):
        return six.iteritems(self.combined)


class SortMergeAggregator(AggregatorBase):

    def __init__(self, mergeCombiners):
        self.mergeValue = mergeCombiners

    def createCombiner(self, v):
        return v


class SortedMerger(Merger):
    def __init__(self, aggregator):
        self.aggregator = SortMergeAggregator(aggregator.mergeCombiners)
        self.combined = iter([])

    def merge(self, iters):
        heap = HeapOnKey(key=lambda x: x[0], min_heap=True)
        self.combined = self.aggregator.aggregate_sorted(heap.merge(iters))

    def __iter__(self):
        return self.combined


class SortedGroupMerger(Merger):

    def __init__(self, rdd_name):
        self.rdd_name = rdd_name
        self.combined = None

    def merge(self, iters):
        heap = HeapOnKey(key=lambda x: x[0], min_heap=True)
        self.combined = GroupByNestedIter(heap.merge(iters), self.rdd_name)

    def __iter__(self):
        return self.combined


class CoGroupMerger(object):

    def __init__(self, size):
        self.size = size
        self.combined = {}

    def get_seq(self, k):
        return self.combined.setdefault(
            k, tuple([[] for _ in range(self.size)]))

    def append(self, i, items):
        for k, v in items:
            self.get_seq(k)[i].append(v)

    def extend(self, i, items):
        for k, v in items:
            self.get_seq(k)[i].extend(v)

    def __iter__(self):
        return six.iteritems(self.combined)


class CoGroupSortMergeAggregator(AggregatorBase):
    def __init__(self, size):
        self.size = size

    def createCombiner(self, v):
        values = tuple([[] for _ in range(self.size)])
        values[v[0]].extend(v[1])
        return values

    def mergeValue(self, c, v):
        c[v[0]].extend(v[1])
        return c


class CoGroupSortedMerger(SortedMerger):

    def __init__(self, size):
        self.aggregator = CoGroupSortMergeAggregator(size)
        self.combined = iter([])


class StreamCoGroupSortedMerger(SortedMerger):

    def __init__(self):
        self.combined = None

    def merge(self, iters):
        # each item like <key, values>
        self.combined = cogroup_no_dup(list(map(iter, iters)))

    def __iter__(self):
        return self.combined


def heap_merged(items_lists, combiner, max_memory):
    heap = []

    def pushback(it, i):
        try:
            k, v = next(it)
            # put i before value, so do not compare the value
            heapq.heappush(heap, (k, i, v))
        except StopIteration:
            pass

    for i, it in enumerate(items_lists):
        it.set_bufsize((max_memory * 1024) / len(items_lists))
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


class SortedItems(object):

    def __init__(self, items):
        self.bufsize = 4096 * 1024
        self.buf = None
        self.offset = 0
        dirs = env.get('WORKDIR')
        self.path = path = os.path.join(
            random.choice(dirs[1:]) if dirs[1:] else dirs[0],
            'shuffle-%s.tmp.gz' % uuid.uuid4().hex)

        with atomic_file(path, bufsize=self.bufsize) as f:
            f = gzip.GzipFile(fileobj=f)
            items = sorted(items, key=lambda k_v: k_v[0])
            try:
                for i in items:
                    s = marshal.dumps(i)
                    f.write(struct.pack("I", len(s)))
                    f.write(s)
                self.loads = marshal.loads
            except Exception:
                f.rewind()
                for i in items:
                    s = six.moves.cPickle.dumps(i)
                    f.write(struct.pack("I", len(s)))
                    f.write(s)
                self.loads = six.moves.cPickle.loads
            f.close()

    def set_bufsize(self, bufsize):
        self.bufsize = min(max(int(bufsize / 4096), 1), 1024) * 4096

    def fill(self):
        with gzip.open(self.path) as f:
            if self.offset != 0:
                f.seek(self.offset)

            self.buf = StringIO.StringIO(f.read(self.bufsize))
            self.offset = f.tell()

    def read(self, size):
        r = self.buf.read(size)
        if len(r) < size:
            self.fill()
            r += self.buf.read(size - len(r))

        return r

    def next(self):
        if self.buf is None:
            self.fill()

        b = self.read(4)
        if not b:
            try:
                if os.path.exists(self.path):
                    os.remove(self.path)
            except Exception:
                pass
            return

        sz, = struct.unpack("I", b)
        return self.loads(self.read(sz))

    def __dealloc__(self):
        try:
            if os.path.exists(self.path):
                os.remove(self.path)
        except Exception:
            pass


class DiskMerger(Merger):

    def __init__(self, aggregator):
        Merger.__init__(self, aggregator, mem)
        self.mem = 0.8 * mem or MAX_SHUFFLE_MEMORY
        self.archives = []
        self.base_memory = self.get_used_memory()
        self.max_merge = None
        self.merged = 0

    def get_used_memory(self):
        try:
            import psutil
            return psutil.Process().memory_info().rss >> 20
        except Exception:
            return 0

    def merge(self, items):
        Merger.merge(self, items)

        self.merged += 1
        if self.max_merge is None:
            current_mem = max(self.get_used_memory() - self.base_memory,
                              sys.getsizeof(self.combined) >> 20)
            if current_mem > self.mem:
                logger.warning(
                    'Too much memory for shuffle, using disk-based shuffle')
                self.max_merge = self.merged

        if self.max_merge is not None and self.merged >= self.max_merge:
            self.rotate()
            self.merged = 0

    def rotate(self):
        self.archives.append(SortedItems(six.iteritems(self.combined)))
        self.combined = {}
        gc.collect()

    def __iter__(self):
        if not self.archives:
            return six.iteritems(self.combined)

        if self.combined:
            self.rotate()
        return heap_merged(self.archives, self.mergeCombiners, self.mem)


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
    from dpark.util import compress
    import logging
    logging.basicConfig(level=logging.DEBUG)
    from dpark.env import env
    env.start()

    l = []
    for i in range(10):
        d = list(zip(list(range(10000)), list(range(10000))))
        random.shuffle(d)
        l.append(SortedItems(d))
    hl = heap_merged(l, lambda x, y: x + y, MAX_SHUFFLE_MEMORY)
    for i in range(10):
        print(i, next(hl))

    path = LocalFileShuffle.getOutputFile(1, 0, 0)
    d = compress(six.moves.cPickle.dumps({'key': 'value'}, -1))
    f = open(path, 'w')
    f.write('p' + struct.pack('I', 5 + len(d)) + d)
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

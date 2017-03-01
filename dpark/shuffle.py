import os
import os.path
import sys
import random
import urllib
import marshal
import struct
import time
import cPickle
import gzip
import Queue
import heapq
import uuid
import gc

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from dpark.util import decompress, spawn, mkdir_p, atomic_file, get_logger
from dpark.env import env
from dpark.tracker import GetValueMessage, SetValueMessage

MAX_SHUFFLE_MEMORY = 2000  # 2 GB

logger = get_logger(__name__)


class LocalFileShuffle:
    serverUri = None
    shuffleDir = None

    @classmethod
    def initialize(cls, isMaster):
        cls.shuffleDir = [p for p in env.get('WORKDIR')
                          if os.path.exists(os.path.dirname(p))]
        if not cls.shuffleDir:
            return
        cls.serverUri = env.get('SERVER_URI')
        logger.debug("shuffle dir: %s", cls.shuffleDir)

    @classmethod
    def getOutputFile(cls, shuffleId, inputId, outputId, datasize=0):
        path = os.path.join(cls.shuffleDir[0], str(shuffleId), str(inputId))
        mkdir_p(path)
        p = os.path.join(path, str(outputId))
        if datasize > 0 and len(cls.shuffleDir) > 1:
            # datasize > 0 means its writing
            st = os.statvfs(path)
            free = st.f_bfree * st.f_bsize
            ratio = st.f_bfree * 1.0 / st.f_blocks
            if free < max(datasize, 1 << 30) or ratio < 0.66:
                d2 = os.path.join(
                    random.choice(cls.shuffleDir[1:]),
                    str(shuffleId), str(inputId))
                mkdir_p(d2)
                p2 = os.path.join(d2, str(outputId))
                if os.path.exists(p):
                    os.remove(p)
                os.symlink(p2, p)
                if os.path.islink(p2):
                    os.unlink(p2)  # p == p2
                return p2
        return p

    @classmethod
    def getServerUri(cls):
        return cls.serverUri


class ShuffleFetcher:

    def fetch(self, shuffleId, reduceId, func):
        raise NotImplementedError

    def stop(self):
        pass


class SimpleShuffleFetcher(ShuffleFetcher):

    def fetch_one(self, uri, shuffleId, part, reduceId):
        if uri == LocalFileShuffle.getServerUri():
            # urllib can open local file
            url = LocalFileShuffle.getOutputFile(shuffleId, part, reduceId)
        else:
            url = "%s/%d/%d/%d" % (uri, shuffleId, part, reduceId)
        logger.debug("fetch %s", url)

        tries = 2
        while True:
            try:
                f = urllib.urlopen(url)
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
                d = decompress(d[5:])
                f.close()
                if flag == 'm':
                    d = marshal.loads(d)
                elif flag == 'p':
                    d = cPickle.loads(d)
                else:
                    raise ValueError("invalid flag")
                return d
            except Exception as e:
                logger.debug("Fetch failed for shuffle %d,"
                             " reduce %d, %d, %s, %s, try again",
                             shuffleId, reduceId, part, url, e)
                tries -= 1
                if not tries:
                    logger.warning("Fetch failed for shuffle %d,"
                                   " reduce %d, %d, %s, %s",
                                   shuffleId, reduceId, part, url, e)
                    from dpark.schedule import FetchFailed
                    raise FetchFailed(uri, shuffleId, part, reduceId)
                time.sleep(2 ** (2 - tries) * 0.1)

    def fetch(self, shuffleId, reduceId, func):
        logger.debug(
            "Fetching outputs for shuffle %d, reduce %d",
            shuffleId, reduceId)
        serverUris = env.mapOutputTracker.getServerUris(shuffleId)
        parts = zip(range(len(serverUris)), serverUris)
        random.shuffle(parts)
        for part, uri in parts:
            d = self.fetch_one(uri, shuffleId, part, reduceId)
            func(d.iteritems())


class ParallelShuffleFetcher(SimpleShuffleFetcher):

    def __init__(self, nthreads):
        self.nthreads = nthreads
        self.start()

    def start(self):
        self.requests = Queue.Queue()
        self.results = Queue.Queue(self.nthreads)
        self.threads = [spawn(self._worker_thread)
                        for i in range(self.nthreads)]

    def _worker_thread(self):
        from dpark.schedule import FetchFailed
        while True:
            r = self.requests.get()
            if r is None:
                break

            uri, shuffleId, part, reduceId = r
            try:
                d = self.fetch_one(*r)
                self.results.put((shuffleId, reduceId, part, d))
            except FetchFailed as e:
                self.results.put(e)

    def fetch(self, shuffleId, reduceId, func):
        logger.debug(
            "Fetching outputs for shuffle %d, reduce %d",
            shuffleId, reduceId)
        serverUris = env.mapOutputTracker.getServerUris(shuffleId)
        if not serverUris:
            return

        parts = zip(range(len(serverUris)), serverUris)
        random.shuffle(parts)
        for part, uri in parts:
            self.requests.put((uri, shuffleId, part, reduceId))

        from dpark.schedule import FetchFailed
        for i in xrange(len(serverUris)):
            r = self.results.get()
            if isinstance(r, FetchFailed):
                self.stop()  # restart
                self.start()
                raise r

            sid, rid, part, d = r
            func(d.iteritems())

    def stop(self):
        logger.debug("stop parallel shuffle fetcher ...")
        while not self.requests.empty():
            self.requests.get_nowait()
        while not self.results.empty():
            self.results.get_nowait()
        for i in range(self.nthreads):
            self.requests.put(None)
        for t in self.threads:
            t.join()


class Merger(object):

    def __init__(self, rdd):
        self.mergeCombiners = rdd.aggregator.mergeCombiners
        self.combined = {}

    def merge(self, items):
        combined = self.combined
        mergeCombiners = self.mergeCombiners
        for k, v in items:
            o = combined.get(k)
            combined[k] = mergeCombiners(o, v) if o is not None else v

    def __iter__(self):
        return self.combined.iteritems()


class CoGroupMerger(object):

    def __init__(self, rdd):
        self.size = rdd.size
        self.combined = {}

    def get_seq(self, k):
        return self.combined.setdefault(
            k, tuple([[] for i in range(self.size)]))

    def append(self, i, items):
        for k, v in items:
            self.get_seq(k)[i].append(v)

    def extend(self, i, items):
        for k, v in items:
            self.get_seq(k)[i].extend(v)

    def __iter__(self):
        return self.combined.iteritems()


def heap_merged(items_lists, combiner, max_memory):
    heap = []

    def pushback(it, i):
        try:
            k, v = it.next()
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
        dirs = LocalFileShuffle.shuffleDir
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
                    s = cPickle.dumps(i)
                    f.write(struct.pack("I", len(s)))
                    f.write(s)
                self.loads = cPickle.loads
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

            raise StopIteration

        sz, = struct.unpack("I", b)
        return self.loads(self.read(sz))

    def __dealloc__(self):
        try:
            if os.path.exists(self.path):
                os.remove(self.path)
        except Exception:
            pass


class DiskMerger(Merger):

    def __init__(self, rdd):
        Merger.__init__(self, rdd)
        self.total = len(rdd)
        self.mem = 0.8 * rdd.mem or MAX_SHUFFLE_MEMORY
        self.archives = []
        self.base_memory = self.get_used_memory()
        self.max_merge = None
        self.merged = 0

    def get_used_memory(self):
        try:
            import psutil
            return psutil.Process().rss >> 20
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
        self.archives.append(SortedItems(self.combined.iteritems()))
        self.combined = {}
        gc.collect()

    def __iter__(self):
        if not self.archives:
            return self.combined.iteritems()

        if self.combined:
            self.rotate()
        return heap_merged(self.archives, self.mergeCombiners, self.mem)


class BaseMapOutputTracker(object):

    def registerMapOutputs(self, shuffleId, locs):
        pass

    def getServerUris(self):
        pass

    def stop(self):
        pass


class MapOutputTracker(BaseMapOutputTracker):

    def __init__(self):
        self.client = env.trackerClient
        logger.debug("MapOutputTracker started")

    def registerMapOutputs(self, shuffleId, locs):
        self.client.call(SetValueMessage('shuffle:%s' % shuffleId, locs))

    def getServerUris(self, shuffleId):
        locs = self.client.call(GetValueMessage('shuffle:%s' % shuffleId))
        logger.debug("Fetch done: %s", locs)
        return locs


def test():
    from dpark.util import compress
    import logging
    logging.basicConfig(level=logging.DEBUG)
    from dpark.env import env
    env.start(True)

    l = []
    for i in range(10):
        d = zip(range(10000), range(10000))
        random.shuffle(d)
        l.append(SortedItems(d))
    hl = heap_merged(l, lambda x, y: x + y, MAX_SHUFFLE_MEMORY)
    for i in range(10):
        print i, hl.next()

    path = LocalFileShuffle.getOutputFile(1, 0, 0)
    d = compress(cPickle.dumps({'key': 'value'}, -1))
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

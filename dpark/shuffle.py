import os, os.path
import random
import urllib
import logging
import marshal
import struct
import time
import cPickle
import gzip
import Queue
import heapq
import platform

from dpark.util import decompress, spawn
from dpark.env import env
from dpark.tracker import GetValueMessage, SetValueMessage

MAX_SHUFFLE_MEMORY = 2000  # 2 GB

logger = logging.getLogger(__name__)

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
        if not os.path.exists(path):
            try: os.makedirs(path)
            except OSError: pass
        p = os.path.join(path, str(outputId))

        if datasize > 0 and len(cls.shuffleDir) > 1:
            st = os.statvfs(path)
            free = st.f_bfree * st.f_bsize
            ratio = st.f_bfree * 1.0 / st.f_blocks
            if free < max(datasize, 1<<30) or ratio < 0.66:
                d2 = os.path.join(random.choice(cls.shuffleDir[1:]), str(shuffleId), str(inputId))
                if not os.path.exists(d2):
                    try: os.makedirs(d2)
                    except IOError: pass
                assert os.path.exists(d2), 'create %s failed' % d2
                p2 = os.path.join(d2, str(outputId))
                os.symlink(p2, p)
                if os.path.islink(p2):
                    os.unlink(p2) # p == p2
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
                    raise ValueError("length not match: expected %d, but got %d" % (length, len(d)))
                d = decompress(d[5:])
                f.close()
                if flag == 'm':
                    d = marshal.loads(d)
                elif flag == 'p':
                    d = cPickle.loads(d)
                else:
                    raise ValueError("invalid flag")
                return d
            except Exception, e:
                logger.debug("Fetch failed for shuffle %d, reduce %d, %d, %s, %s, try again",
                        shuffleId, reduceId, part, url, e)
                tries -= 1
                if not tries:
                    logger.warning("Fetch failed for shuffle %d, reduce %d, %d, %s, %s",
                            shuffleId, reduceId, part, url, e)
                    from dpark.schedule import FetchFailed
                    raise FetchFailed(uri, shuffleId, part, reduceId)
                time.sleep(2**(2-tries)*0.1)

    def fetch(self, shuffleId, reduceId, func):
        logger.debug("Fetching outputs for shuffle %d, reduce %d", shuffleId, reduceId)
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
        self.threads = [spawn(self._worker_thread) for i in range(self.nthreads)]

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
            except FetchFailed, e:
                self.results.put(e)

    def fetch(self, shuffleId, reduceId, func):
        logger.debug("Fetching outputs for shuffle %d, reduce %d", shuffleId, reduceId)
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
                self.stop() # restart
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

    def __init__(self, total, mergeCombiner):
        self.mergeCombiner = mergeCombiner
        self.combined = {}

    def merge(self, items):
        combined = self.combined
        mergeCombiner = self.mergeCombiner
        for k,v in items:
            o = combined.get(k)
            combined[k] = mergeCombiner(o, v) if o is not None else v

    def __iter__(self):
        return self.combined.iteritems()

class CoGroupMerger(object):
    def __init__(self, size):
        self.size = size
        self.combined = {}

    def get_seq(self, k):
        return self.combined.setdefault(k, tuple([[] for i in range(self.size)]))

    def append(self, i, items):
        for k, v in items:
            self.get_seq(k)[i].append(v)

    def extend(self, i, items):
        for k, v in items:
            self.get_seq(k)[i].extend(v)

    def __iter__(self):
        return self.combined.iteritems()

def heap_merged(items_lists, combiner):
    heap = []
    def pushback(it):
        try:
            k,v = it.next()
            # put i before value, so do not compare the value
            heapq.heappush(heap, (k, i, v))
        except StopIteration:
            pass
    for i, it in enumerate(items_lists):
        if isinstance(it, list):
            items_lists[i] = it = (k for k in it)
        pushback(it)
    if not heap: return

    last_key, i, last_value = heapq.heappop(heap)
    pushback(items_lists[i])

    while heap:
        k, i, v = heapq.heappop(heap)
        if k != last_key:
            yield last_key, last_value
            last_key, last_value = k, v
        else:
            last_value = combiner(last_value, v)
        pushback(items_lists[i])

    yield last_key, last_value

class sorted_items(object):
    next_id = 0
    @classmethod
    def new_id(cls):
        cls.next_id += 1
        return cls.next_id

    def __init__(self, items):
        self.id = self.new_id()
        self.path = path = os.path.join(LocalFileShuffle.shuffleDir,
            'shuffle-%d-%d.tmp.gz' % (os.getpid(), self.id))
        f = gzip.open(path, 'wb+')

        items = sorted(items)
        try:
            for i in items:
                s = marshal.dumps(i)
                f.write(struct.pack("I", len(s)))
                f.write(s)
            self.loads = marshal.loads
        except Exception, e:
            f.rewind()
            for i in items:
                s = cPickle.dumps(i)
                f.write(struct.pack("I", len(s)))
                f.write(s)
            self.loads = cPickle.loads
        f.close()

        self.f = gzip.open(path)
        self.c = 0

    def __iter__(self):
        self.f = gzip.open(self.path)
        self.c = 0
        return self

    def next(self):
        f = self.f
        b = f.read(4)
        if not b:
            f.close()
            if os.path.exists(self.path):
                os.remove(self.path)
            raise StopIteration
        sz, = struct.unpack("I", b)
        self.c += 1
        return self.loads(f.read(sz))

    def __dealloc__(self):
        self.f.close()
        if os.path.exists(self.path):
            os.remove(self.path)


class DiskMerger(Merger):
    def __init__(self, total, combiner):
        Merger.__init__(self, total, combiner)
        self.total = total
        self.archives = []
        self.base_memory = self.get_used_memory()
        self.max_merge = None
        self.merged = 0

    def get_used_memory(self):
        if platform.system() == 'Linux':
            for line in open('/proc/self/status'):
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) >> 10
        return 0

    def merge(self, items):
        Merger.merge(self, items)

        self.merged += 1
        #print 'used', self.merged, self.total, self.get_used_memory() - self.base_memory, self.base_memory
        if self.max_merge is None:
            if self.merged < self.total/5 and self.get_used_memory() - self.base_memory > MAX_SHUFFLE_MEMORY:
                self.max_merge = self.merged

        if self.max_merge is not None and self.merged >= self.max_merge:
            t = time.time()
            self.rotate()
            self.merged = 0
            #print 'after rotate', self.get_used_memory() - self.base_memory, time.time() - t

    def rotate(self):
        self.archives.append(sorted_items(self.combined.iteritems()))
        self.combined = {}

    def __iter__(self):
        if not self.archives:
            return self.combined.iteritems()

        if self.combined:
            self.rotate()
        return heap_merged(self.archives, self.mergeCombiner)

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
    l = []
    for i in range(10):
        d = zip(range(10000), range(10000))
        l.append(sorted_items(d))
    hl = heap_merged(l, lambda x,y:x+y)
    for i in range(10):
        print i, hl.next()

    import logging
    logging.basicConfig(level=logging.INFO)
    from dpark.env import env
    import cPickle
    env.start(True)

    path = LocalFileShuffle.getOutputFile(1, 0, 0)
    f = open(path, 'w')
    f.write(cPickle.dumps([('key','value')], -1))
    f.close()

    uri = LocalFileShuffle.getServerUri()
    env.mapOutputTracker.registerMapOutputs(1, [uri])
    fetcher = SimpleShuffleFetcher()
    def func(k,v):
        assert k=='key'
        assert v=='value'
    fetcher.fetch(1, 0, func)

    tracker = MapOutputTracker(True)
    tracker.registerMapOutputs(2, [None, uri, None, None, None])
    assert tracker.getServerUris(2) == [None, uri, None, None, None]
    ntracker = MapOutputTracker(False)
    assert ntracker.getServerUris(2) == [None, uri, None, None, None]
    ntracker.stop()
    tracker.stop()

if __name__ == '__main__':
    test()

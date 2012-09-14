import os, os.path
import random
import urllib
import logging
import marshal
import struct
import socket
import time
import cPickle
import gzip
import threading
import Queue
import heapq
import platform

import zmq

from util import decompress
from env import env
from cache import CacheTrackerServer, CacheTrackerClient

MAX_SHUFFLE_MEMORY = 2000  # 2 GB

logger = logging.getLogger("shuffle")

class LocalFileShuffle:
    serverUri = None
    shuffleDir = None
    @classmethod
    def initialize(cls, isMaster):
        cls.shuffleDir = env.get('WORKDIR')
        if not cls.shuffleDir:
            return
        cls.serverUri = env.get('SERVER_URI', 'file://' + cls.shuffleDir)
        logger.debug("shuffle dir: %s", cls.shuffleDir)

    @classmethod
    def getOutputFile(cls, shuffleId, inputId, outputId):
        path = os.path.join(cls.shuffleDir, str(shuffleId), str(inputId))
        if not os.path.exists(path):
            try: os.makedirs(path)
            except OSError: pass
        return os.path.join(path, str(outputId))

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
            urlopen, url = (file, LocalFileShuffle.getOutputFile(shuffleId, part, reduceId))
        else:
            urlopen, url = (urllib.urlopen, "%s/%d/%d/%d" % (uri, shuffleId, part, reduceId))
        logger.debug("fetch %s", url)
        
        tries = 4
        while True:
            try:
                f = urlopen(url)
                d = f.read()
                flag = d[:1]
                length, = struct.unpack("I", d[1:5])
                if length != len(d):
                    raise IOError("length not match: expected %d, but got %d" % (length, len(d)))
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
                    logger.error("Fetch failed for shuffle %d, reduce %d, %d, %s, %s", 
                            shuffleId, reduceId, part, url, e)
                    raise
                time.sleep(2**(3-tries))

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
        self.requests = Queue.Queue()
        self.results = Queue.Queue(1)
        for i in range(nthreads):
            t = threading.Thread(target=self._worker_thread)
            t.daemon = True
            t.start()

    def _worker_thread(self):
        while True:
            r = self.requests.get()
            if r is None:
                self.results.put(r)
                break

            uri, shuffleId, part, reduceId = r
            try:
                d = self.fetch_one(*r)
                self.results.put((shuffleId, reduceId, part, d))
            except Exception, e:
                self.results.put(e)

    def fetch(self, shuffleId, reduceId, func):
        logger.debug("Fetching outputs for shuffle %d, reduce %d", shuffleId, reduceId)
        serverUris = env.mapOutputTracker.getServerUris(shuffleId)
        parts = zip(range(len(serverUris)), serverUris)
        random.shuffle(parts)
        for part, uri in parts:
            self.requests.put((uri, shuffleId, part, reduceId))
        
        for i in xrange(len(serverUris)):
            r = self.results.get()
            if isinstance(r, Exception):
                raise r

            sid, rid, part, d = r
            func(d.iteritems())

    def stop(self):
        logger.debug("stop parallel shuffle fetcher ...")
        for i in range(self.nthreads):
            self.requests.put(None)
        for i in range(self.nthreads):
            self.results.get()


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

        try:
            for i in sorted(items):
                s = marshal.dumps(i)
                f.write(struct.pack("I", len(s)))
                f.write(s)
            self.loads = marshal.loads
        except Exception, e:
            for i in sorted(items):
                s = pickle.dumps(i)
                f.write(struct.pack("I", len(s)))
                f.write(s)
            self.loads = pickle.loads
        f.close()

        self.f = gzip.open(path)
        self.c = 0

    def __iter__(self):
        self.f = gzip.open(path)
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
        f.close()
        if os.path.exists(self.path):
            os.remove(self.path)


class DiskMerger(Merger):
    def __init__(self, total, combiner):
        Merger.__init__(self, total, combiner)
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

class MapOutputTrackerMessage: pass
class GetMapOutputLocations:
    def __init__(self, shuffleId):
        self.shuffleId = shuffleId
class StopMapOutputTracker: pass        


class MapOutputTrackerServer(CacheTrackerServer):
    def __init__(self, serverUris):
        CacheTrackerServer.__init__(self)
        self.serverUris = serverUris
    
    def stop(self):
        sock = env.ctx.socket(zmq.REQ)
        sock.connect(self.addr)
        sock.send_pyobj(StopMapOutputTracker())
        self.t.join()

    def run(self):
        sock = env.ctx.socket(zmq.REP)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        self.addr = "tcp://%s:%d" % (socket.gethostname(), port)
        logger.debug("MapOutputTrackerServer started at %s", self.addr)

        def reply(msg):
            sock.send_pyobj(msg)

        while True:
            msg = sock.recv_pyobj()
            #logger.debug("MapOutputTrackerServer recv %s", msg)
            if isinstance(msg, GetMapOutputLocations):
                reply(self.serverUris.get(msg.shuffleId, []))
            elif isinstance(msg, StopMapOutputTracker):
                reply('OK')
                break
            else:
                logger.error('unexpected mapOutputTracker msg: %s %s', msg, type(msg))
                reply('ERROR')
        sock.close()
        logger.debug("MapOutputTrackerServer stopped %s", self.addr)

class MapOutputTrackerClient(CacheTrackerClient):
    pass

class MapOutputTracker(object):
    def __init__(self, isMaster, addr=None):
        self.isMaster = isMaster
        self.serverUris = {}
        self.fetching = set()
        self.generation = 0
        if isMaster:
            self.server = MapOutputTrackerServer(self.serverUris)
            self.server.start()
            addr = self.server.addr
            env.register('MapOutputTrackerAddr', addr)
        else:
            addr = env.get('MapOutputTrackerAddr')
        self.client = MapOutputTrackerClient(addr)
        logger.debug("MapOutputTracker started")

    def clear(self):
        self.serverUris.clear()
        self.fetching.clear()
        self.server.clear()

    def registerMapOutput(self, shuffleId, numMaps, mapId, serverUri):
        self.serverUris.setdefault(shuffleId, [None] * numMaps)[mapId] = serverUri

    def registerMapOutputs(self, shuffleId, locs):
        self.serverUris[shuffleId] = locs

    def unregisterMapOutput(self, shuffleId, mapId, serverUri):
        locs = self.serverUris.get(shuffleId)
        if locs is not None:
            if locs[mapId] == serverUri:
                locs[mapId] = None
            self.incrementGeneration()
        raise Exception("unregisterMapOutput called for nonexistent shuffle ID")

    def getServerUris(self, shuffleId):
        while shuffleId in self.fetching:
            logger.debug("wait for fetching mapOutput of %s", shuffleId)
            time.sleep(0.01)
        locs = self.serverUris.get(shuffleId)
        if locs:
            logger.debug("got mapOutput in cache: %s", locs)
            return locs
        logger.debug("Don't have map outputs for %d, fetching them", shuffleId)
        self.fetching.add(shuffleId)
        locs = self.client.call(GetMapOutputLocations(shuffleId))
        logger.debug("Fetch done: %s", locs)
        self.serverUris[shuffleId] = locs
        self.fetching.remove(shuffleId)
        return locs

    def getMapOutputUri(self, serverUri, shuffleId, mapId, reduceId):
        return "%s/shuffle/%s/%s/%s" % (serverUri, shuffleId, mapId, reduceId)

    def stop(self):
        self.serverUris.clear()
        self.client.stop()
        if self.isMaster:
            self.server.stop()

    def incrementGeneration(self):
        self.generation += 1

    def getGeneration(self):
        return self.generation

    def updateGeneration(self, newGen):
        if newGen > self.generation:
            logger.debug("Updating generation to %d and clearing cache", newGen)
            self.generation = newGen
            self.serverUris.clear()


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
    from env import env
    import cPickle
    env.start(True)
    
    path = LocalFileShuffle.getOutputFile(1, 0, 0) 
    f = open(path, 'w')
    f.write(cPickle.dumps([('key','value')], -1))
    f.close()
    
    uri = LocalFileShuffle.getServerUri()
    env.mapOutputTracker.registerMapOutput(1, 1, 0, uri)
    fetcher = SimpleShuffleFetcher()
    def func(k,v):
        assert k=='key'
        assert v=='value'
    fetcher.fetch(1, 0, func) 

    tracker = MapOutputTracker(True)
    tracker.registerMapOutput(2, 5, 1, uri)
    assert tracker.getServerUris(2) == [None, uri, None, None, None]
    ntracker = MapOutputTracker(False)
    assert ntracker.getServerUris(2) == [None, uri, None, None, None]
    ntracker.stop()
    tracker.stop()

if __name__ == '__main__':
    test()

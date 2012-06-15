import os, os.path
import random
import urllib
import logging
import marshal
import struct
import socket
import time
import cPickle
import zlib as comp
import threading
import Queue

import zmq

from env import env
from cache import CacheTrackerServer, CacheTrackerClient

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
            os.makedirs(path)
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
        
        tries = 3
        while True:
            try:
                f = urlopen(url)
                d = f.read()
                flag = d[:1]
                length, = struct.unpack("I", d[1:5])
                if length != len(d):
                    raise IOError("length not match: expected %d, but got %d" % (length, len(d)))
                d = comp.decompress(d[5:])
                f.close()
                if flag == 'm':
                    d = marshal.loads(d)
                elif flag == 'p':
                    d = cPickle.loads(d)
                else:
                    raise ValueError("invalid flag")
                return d
            except Exception, e:
                logger.warning("Fetch failed for shuffle %d, reduce %d, %d, %s, %s, try again", shuffleId, reduceId, part, url, e)
                tries -= 1
                if not tries:
                    logger.error("Fetch failed for shuffle %d, reduce %d, %d, %s, %s", shuffleId, reduceId, part, url, e)
                    raise
                time.sleep(1)

    def fetch(self, shuffleId, reduceId, func):
        logger.debug("Fetching outputs for shuffle %d, reduce %d", shuffleId, reduceId)
        serverUris = env.mapOutputTracker.getServerUris(shuffleId)
        parts = zip(range(len(serverUris)), serverUris)
        random.shuffle(parts)
        for part, uri in parts:
            d = self.fetch_one(uri, shuffleId, part, reduceId) 
            for k,v in d.iteritems():
                func(k,v)


class ParallelShuffleFetcher(SimpleShuffleFetcher):
    def __init__(self, nthreads):
        self.nthreads = nthreads
        self.requests = Queue.Queue()
        self.results = Queue.Queue()
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
        
        #completed = set() 
        for i in xrange(len(serverUris)):
            r = self.results.get()
            if isinstance(r, Exception):
                raise r

            sid, rid, part, d = r
            #assert shuffleId == sid
            #assert rid == reduceId
            for k,v in d.iteritems():
                func(k,v)
            #completed.add(part)

    def stop(self):
        logger.debug("stop parallel shuffle fetcher ...")
        for i in range(self.nthreads):
            self.requests.put(None)
        for i in range(self.nthreads):
            self.results.get()


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
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REQ)
        sock.connect(self.addr)
        sock.send_pyobj(StopMapOutputTracker())
        self.t.join()

    def run(self):
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REP)
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

class MapOutputTracker:
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

import os, os.path
import random
import urllib
import logging
import marshal
import struct
import socket
import time
import cPickle

import zmq

from env import env
from cache import CacheTrackerServer, CacheTrackerClient

logger = logging.getLogger("shuffle")

class LocalFileShuffle:
    serverUri = None
    shuffleDir = None
    @classmethod
    def initialize(cls, isMaster, port):
        shuffleDir = env.get('WORKDIR')
        if not shuffleDir:
            return
        cls.shuffleDir = shuffleDir
        if port is None:
            while not os.path.exists(shuffleDir):
                time.sleep(0.1) # HACK for moosefs
            cls.serverUri = 'file://' + cls.shuffleDir
        else:
            cls.serverUri = 'http://%s:%d' % (socket.gethostname(), port)
        logger.debug("shuffle dir: %s", shuffleDir)

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
    def fetch(self, shuffleId, reduceId, func):
        logger.debug("Fetching outputs for shuffle %d, reduce %d", shuffleId, reduceId)
        splitsByUri = {}
        serverUris = env.mapOutputTracker.getServerUris(shuffleId)
        for i, uri in enumerate(serverUris):
            splitsByUri.setdefault(uri, []).append(i)
        for uri, parts in splitsByUri.items():
            for part in parts:
                if uri == LocalFileShuffle.getServerUri():
                    url = (file, LocalFileShuffle.getOutputFile(shuffleId, part, reduceId))
                else:
                    url = (urllib.urlopen, "%s/%d/%d/%d" % (uri, shuffleId, part, reduceId))
                logger.debug("fetch %s", url[1])
                
                tries = 3
                while True:
                    try:
                        f = url[0](url[1])
                        flag = f.read(1)
                        if flag == 'm':
                            d = marshal.loads(f.read())
                        elif flag == 'p':
                            d = cPickle.loads(f.read())
                        else:
                            raise ValueError("invalid flag")
                        f.close()
                        break
                    except IOError, e:
                        if not os.path.exists(uri): raise
                        logger.warning("Fetch failed for shuffle %d, reduce %d, %d, %s, %s, try again", shuffleId, reduceId, part, url, e)
                        tries -= 1
                        if not tries:
                            logger.error("Fetch failed for shuffle %d, reduce %d, %d, %s, %s", shuffleId, reduceId, part, url, e)
                            raise
                        time.sleep(1)
                
                for k,v in d.iteritems():
                    func(k,v)

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

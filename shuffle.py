import os, os.path
import random
import logging
import urllib
import logging
import pickle
import zmq

from env import env
from cache import *

class LocalFileShuffle:
    initialized = False

    @classmethod
    def initializeIfNeeded(cls):
        if cls.initialized:
            return
        localDirRoot = "/tmp/spark/"
        shuffleDir = os.path.join(localDirRoot, str(os.getpid()), "shuffle")
        if not os.path.exists(shuffleDir):
            try:
                os.makedirs(shuffleDir)
            except:
                pass
        else:
            pass # TODO: clean dir

        logging.info("shuffle dir: %s", shuffleDir)

        cls.shuffleDir = shuffleDir
        cls.serverUri = "file:///" + shuffleDir
        cls.initialized = True

    @classmethod
    def getOutputFile(cls, shuffleId, inputId, outputId):
        cls.initializeIfNeeded()
        path = os.path.join(cls.shuffleDir, str(shuffleId), str(inputId))
        if not os.path.exists(path):
            os.makedirs(path)
        return os.path.join(path, str(outputId))

    @classmethod
    def getServerUri(cls):
        cls.initializeIfNeeded()
        return cls.serverUri

    nextShuffleId = 0
    @classmethod
    def newShuffleId(cls):
        cls.nextShuffleId += 1
        return cls.nextShuffleId


class ShuffleFetcher:
    def fetch(self, shuffleId, reduceId, func):
        raise NotImplementedError
    def stop(self):
        pass

class SimpleShuffleFetcher(ShuffleFetcher):
    def fetch(self, shuffleId, reduceId, func):
        logging.info("Fetching outputs for shuffle %d, reduce %d", shuffleId, reduceId)
        splitsByUri = {}
        serverUris = env.mapOutputTracker.getServerUris(shuffleId)
        for i, uri in enumerate(serverUris):
            splitsByUri.setdefault(uri, []).append(i)
        for uri, ids in splitsByUri.items():
            for id in ids:
                try:
                    url = "%s/%d/%d/%d" % (uri, shuffleId, id, reduceId)
                    logging.info("fetch %s", url)
                    for k,v in pickle.loads(urllib.urlopen(url).read()):
                        logging.info("read %s : %s", k, v)
                        func(k,v)
                except IOError, e:
                    logging.error("Fetch failed for shuffle %d, reduce %d, %d, %s", shuffleId, reduceId, i, url)
                    raise 


class MapOutputTrackerMessage: pass
class GetMapOutputLocations:
    def __init__(self, shuffleId):
        self.shuffleId = shuffleId
class StopMapOutputTracker: pass        

ctx = zmq.Context()

class MapOutputTrackerServer(CacheTrackerServer):
    def __init__(self, serverUris):
        CacheTrackerServer.__init__(self)
        self.serverUris = serverUris
    
    def stop(self):
        sock = ctx.socket(zmq.REQ)
        sock.connect(self.addr)
        sock.send(cPickle.dumps(StopMapOutputTracker(), -1))
        self.t.join()

    def run(self):
        sock = ctx.socket(zmq.REP)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        self.addr = "tcp://%s:%d" % (socket.gethostname(), port)
        logging.info("MapOutputTrackerServer started at %s", self.addr)
        def reply(msg):
            sock.send(cPickle.dumps(msg))
        while True:
            msg = cPickle.loads(sock.recv())
            logging.info("MapOutputTrackerServer recv %s", msg)
            if isinstance(msg, GetMapOutputLocations):
                reply(self.serverUris.get(msg.shuffleId))
            elif isinstance(msg, StopMapOutputTracker):
                reply('OK')
                break
            else:
                logging.error('unexpected mapOutputTracker msg: %s %s', msg, type(msg))
                reply('ERROR')
        sock.close()
        logging.info("MapOutputTrackerServer stopped %s", self.addr)

class MapOutputTrackerClient(CacheTrackerClient):
    pass

class MapOutputTracker:
    def __init__(self, isMaster):
        self.isMaster = isMaster
        self.serverUris = {}
        self.fetching = set()
        self.generation = 0
        if isMaster:
            self.server = MapOutputTrackerServer(self.serverUris)
            self.server.start()
            addr = self.server.addr
            os.environ['MapOutputTracker'] = addr
        else:
            addr = os.environ['MapOutputTracker']
        self.client = MapOutputTrackerClient(addr)

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
            logging.info("wait for fetching mapOutput of %s", shuffleId)
            time.sleep(0.01)
        locs = self.serverUris.get(shuffleId)
        if locs:
            logging.info("got mapOutput in cache: %s", locs)
            return locs
        logging.info("Don't have map outputs for %d, fetching them", shuffleId)
        self.fetching.add(shuffleId)
        locs = self.client.call(GetMapOutputLocations(shuffleId))
        logging.info("Fetch done: %s", locs)
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
            logging.info("Updating generation to %d and clearing cache", newGen)
            self.generation = newGen
            self.serverUris.clear()


def test():
    import logging
    logging.basicConfig(level=logging.INFO)
    from env import env
    env.start(True)
    
    path = LocalFileShuffle.getOutputFile(1, 0, 0) 
    f = open(path, 'w')
    f.write(pickle.dumps([('key','value')]))
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

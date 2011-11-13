import os, os.path
import random
import logging
import urllib
import logging
import marshal
import struct
import socket

import zmq

from env import env
from cache import *

class LocalFileShuffle:
    serverUri = None
    shuffleDir = None
    @classmethod
    def initialize(cls, isMaster):
        shuffleDir = env.get('WORKDIR')
        while not os.path.exists(shuffleDir):
            time.sleep(0.1) # HACK for moosefs
        cls.shuffleDir = shuffleDir
        cls.serverUri = shuffleDir
        logging.debug("shuffle dir: %s", shuffleDir)

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
        logging.debug("Fetching outputs for shuffle %d, reduce %d", shuffleId, reduceId)
        splitsByUri = {}
        serverUris = env.mapOutputTracker.getServerUris(shuffleId)
        for i, uri in enumerate(serverUris):
            splitsByUri.setdefault(uri, []).append(i)
        for uri, parts in splitsByUri.items():
            for part in parts:
                try:
                    url = "%s/%d/%d/%d" % (uri, shuffleId, part, reduceId)
                    logging.debug("fetch %s", url)
                    f = open(url, 'rb')
                    flag = f.read(1)
                    if flag == 'm':
                        d = marshal.load(f)
                    elif flag == 'p':
                        d = cPickle.load(f)
                    else:
                        raise ValueError("invalid flag")
                    f.close()
                    for k,v in d.iteritems():
                        func(k,v)
                except IOError, e:
                    logging.error("Fetch failed for shuffle %d, reduce %d, %d, %s", shuffleId, reduceId, part, url)
                    raise 


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
        logging.debug("MapOutputTrackerServer started at %s", self.addr)

        def reply(msg):
            sock.send_pyobj(msg)

        while True:
            msg = sock.recv_pyobj()
            #logging.debug("MapOutputTrackerServer recv %s", msg)
            if isinstance(msg, GetMapOutputLocations):
                reply(self.serverUris.get(msg.shuffleId, []))
            elif isinstance(msg, StopMapOutputTracker):
                reply('OK')
                break
            else:
                logging.error('unexpected mapOutputTracker msg: %s %s', msg, type(msg))
                reply('ERROR')
        sock.close()
        logging.debug("MapOutputTrackerServer stopped %s", self.addr)

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
        logging.debug("MapOutputTracker started")

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
            logging.debug("wait for fetching mapOutput of %s", shuffleId)
            time.sleep(0.01)
        locs = self.serverUris.get(shuffleId)
        if locs:
            logging.debug("got mapOutput in cache: %s", locs)
            return locs
        logging.debug("Don't have map outputs for %d, fetching them", shuffleId)
        self.fetching.add(shuffleId)
        locs = self.client.call(GetMapOutputLocations(shuffleId))
        logging.debug("Fetch done: %s", locs)
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
            logging.debug("Updating generation to %d and clearing cache", newGen)
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

import os
import weakref
import socket
import threading
import multiprocessing
import logging
import cPickle
import time

import zmq

import shareddict
from env import env

mmapCache = shareddict.SharedDicts(512)

class Cache:
    map = {}

    nextKeySpaceId = 0
    @classmethod
    def newKeySpaceId(cls):
        cls.nextKeySpaceId += 1
        return cls.nextKeySpaceId
   
    def newKeySpace(self):
        return KeySpace(self, self.newKeySpaceId())

    def get(self, key): 
        return self.map.get(key)
    
    def put(self, key, value):
        self.map[key] = value

class KeySpace(Cache):
    def __init__(self, cache, id):
        self.map = cache
        self.id = id

    def newkey(self, key):
        return "%d:%s" % (self.id, key)

    def get(self, key):
        return self.map.get(self.newkey(key))

    def put(self, key, value):
        return self.map.put(self.newkey(key), value)


class LocalCache(Cache):
    '''cache obj in current process'''
    def __init__(self, cache):
        self.cache = cache
        self.map = {}

    def get(self, key):
        r = self.map.get(key)
        if r is None:
            r = self.cache.get(key)
            if r is not None:
                self.map[key] = r
        return r

    def put(self, key, value):
        self.map[key] = value
        self.cache.put(key, value)


class CacheTrackerMessage:pass
class AddedToCache(CacheTrackerMessage):
    def __init__(self, rddId, partition, host):
        self.rddId = rddId
        self.partition = partition
        self.host = host
class DroppedFromCache(CacheTrackerMessage):
    def __init__(self, rddId, partition, host):
        self.rddId = rddId
        self.partition = partition
        self.host = host
class MemoryCacheLost(CacheTrackerMessage):
    def __init__(self, host):
        self.host = host
class RegisterRDD(CacheTrackerMessage):
    def __init__(self, rddId, numPartitions):
        self.rddId = rddId
        self.numPartitions = numPartitions
class GetCacheLocations(CacheTrackerMessage):pass
class StopCacheTracker(CacheTrackerMessage):pass

class CacheTrackerServer:
    def __init__(self):
        self.addr = None

    def start(self):
        self.t = threading.Thread(target=self.run)
        self.t.daemon = True
        self.t.start()
        while self.addr is None:
            time.sleep(0.01)

    def stop(self):
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REQ)
        sock.connect(self.addr)
        sock.send_pyobj(StopCacheTracker())
        self.t.join()

    def run(self):
        locs = {}
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REP)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        self.addr = "tcp://%s:%d" % (socket.gethostname(), port)
        logging.debug("CacheTrackerServer started at %s", self.addr)
        def reply(msg):
            sock.send_pyobj(msg)
        while True:
            msg = sock.recv_pyobj()
            if isinstance(msg, RegisterRDD):
                locs[msg.rddId] = [[] for i in range(msg.numPartitions)]
                reply('OK')
            elif isinstance(msg, AddedToCache):
                locs[msg.rddId][msg.partition].append(msg.host)
                reply('OK')
            elif isinstance(msg, DroppedFromCache):
                locs[msg.rddId][msg.partition].remove(msg.host)
                reply('OK')
            elif isinstance(msg, MemoryCacheLost):
                for k,v in locs.iteritems():
                    for l in v:
                        l.remove(msg.host)
                reply('OK')
            elif isinstance(msg, GetCacheLocations):
                reply(locs)
            elif isinstance(msg, StopCacheTracker):
                reply('OK')
                break
            else:
                logging.error("unexpected msg %s %s", msg, type(msg))
                reply('ERROR')
        sock.close()
        logging.debug("stop CacheTrackerServer %s", self.addr)

class CacheTrackerClient:
    def __init__(self, addr):
        ctx = zmq.Context()
        self.sock = ctx.socket(zmq.REQ)
        self.sock.connect(addr)

    def call(self, msg):
        self.sock.send_pyobj(msg)
        return self.sock.recv_pyobj()

    def stop(self):
        self.sock.close()
        #logging.debug("stop %s", self.__class__)

class CacheTracker:
    def __init__(self, isMaster):
        self.isMaster = isMaster
        self.registeredRddIds = set()
        if isMaster:
            self.cache = Cache()
        else:
            self.cache = LocalCache(mmapCache).newKeySpace()

        if isMaster:
            self.server = CacheTrackerServer()
            self.server.start()
            addr = self.server.addr
            env.register('CacheTrackerAddr', addr)
        else:
            addr = env.get('CacheTrackerAddr')

        self.client = CacheTrackerClient(addr)

    def registerRDD(self, rddId, numPartitions):
        if rddId not in self.registeredRddIds:
            logging.debug("Registering RDD ID %d with cache", rddId)
            self.registeredRddIds.add(rddId)
            self.client.call(RegisterRDD(rddId, numPartitions))

    def getLocationsSnapshot(self):
        return self.client.call(GetCacheLocations())

    def getOrCompute(self, rdd, split):
        key = "%s:%s" % (rdd.id, split.index)
        cachedVal = self.cache.get(key)
        while cachedVal == 'loading':
            time.sleep(0.01)
            cachedVal = self.cache.get(key)
        if cachedVal is not None:
            logging.debug("Found partition in cache! %s", key)
            #self.client.call("found " + key) 
            return cachedVal
        logging.debug("partition not in cache, %s", key)
        #self.client.call("not found " + key)
        self.cache.put(key, 'loading')
        r = list(rdd.compute(split))
        self.cache.put(key, r)
        host = socket.gethostname()
        self.client.call(AddedToCache(rdd.id, split.index, host))
        return r

    def stop(self):
        self.client.stop()
        if self.isMaster:
            self.server.stop()

    def __getstate__(self):
        raise Exception("!!!")

def set_cache():
    cache = mmapCache
    cache.put('a','b')
    return True

def get_cache():
    cache = mmapCache
    return cache.get('a')

def test():
    logging.basicConfig(level=logging.DEBUG)
    cache = mmapCache
    pool = multiprocessing.Pool(2)
    assert pool.apply(set_cache) == True
    assert pool.apply(get_cache) == 'b'
    pool.close()
    pool.join()
    assert cache.get('a') == 'b'
    
    from context import DparkContext
    dc = DparkContext("local")
    nums = dc.parallelize(range(100), 10)
    cache = mmapCache
    tracker = CacheTracker(True)
    tracker.registerRDD(nums.id, len(nums))
    split = nums.splits[0]
    print tracker.getOrCompute(nums, split)
    print tracker.getOrCompute(nums, split)
    print tracker.getLocationsSnapshot()
    tracker.stop()

if __name__ == '__main__':
    test()

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

    def pop(self, key, default=None):
        return self.map.pop(key, default)

    def clear(self):
        return self.map.clear()

class KeySpace(Cache):
    def __init__(self, cache, id):
        self.map = cache
        self.id = id

    def get(self, key):
        return self.map.get((self.id, key))

    def put(self, key, value):
        return self.map.put((self.id, key), value)

    def pop(self, key):
        return self.map.pop((self.id, key))

manager = multiprocessing.Manager()
class ProcessCache(Cache):
    map = manager.dict()

class SerializingCache(Cache):
    def __init__(self, cache):
        self.map = cache

    def get(self, key):
        b = self.map.get(key)
        return b and cPickle.loads(b) or None

    def put(self, key, value):
        try:
            v = cPickle.dumps(value)
            self.map.put(key, v)
        except Exception, e:
            logging.error("cache key %s err", key)




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
        sock.send(cPickle.dumps(StopCacheTracker(), -1))
        self.t.join()

    def run(self):
        locs = {}
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REP)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        self.addr = "tcp://%s:%d" % (socket.gethostname(), port)
        logging.debug("CacheTrackerServer started at %s", self.addr)
        def reply(msg):
            sock.send(cPickle.dumps(msg))
        while True:
            msg = cPickle.loads(sock.recv())
            #logging.debug("CacheTracker recv %s", msg)
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
        self.addr = addr
        ctx = zmq.Context()
        self.sock = ctx.socket(zmq.REQ)
        self.sock.connect(addr)

    def call(self, msg):
        self.sock.send(cPickle.dumps(msg, -1))
        r = cPickle.loads(self.sock.recv())
        return r

    def stop(self):
        self.sock.close()
        logging.debug("stop %s", self.__class__)

class CacheTracker:
    def __init__(self, isMaster, addr=None):
        self.isMaster = isMaster
        self.registeredRddIds = set()
        if isMaster:
            self.cache = Cache()
        else:
            self.cache = SerializingCache(mmapCache) #.newKeySpace()
        if isMaster:
            self.server = CacheTrackerServer()
            self.server.start()
            addr = self.server.addr
            os.environ['CacheTracker'] = addr
        elif addr is None:
            addr = os.environ['CacheTracker']
        self.addr = addr
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
            return cachedVal
        logging.debug("partition not in cache, %s", key)
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
    cache = ProcessCache()
    cache.put('a','b')
    return True

def get_cache():
    cache = ProcessCache()
    return cache.get('a')

def test():
    logging.basicConfig(level=logging.DEBUG)
    cache = ProcessCache()
    pool = multiprocessing.Pool(2)
    assert pool.apply(set_cache) == True
    assert pool.apply(get_cache) == 'b'
    pool.close()
    pool.join()
    assert cache.get('a') == 'b'
    
    from context import SparkContext
    sc = SparkContext("local")
    nums = sc.parallelize(range(100), 10)
    cache = BoundedMemoryCache(cache)
    tracker = CacheTracker(True, cache)
    tracker.registerRDD(nums.id, len(nums.splits))
    split = nums.splits[0]
    print tracker.getOrCompute(nums, split)
    print tracker.getOrCompute(nums, split)
    print tracker.getLocationsSnapshot()
    tracker.stop()

if __name__ == '__main__':
    test()

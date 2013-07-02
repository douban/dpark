import os
import socket
import multiprocessing
import logging
import marshal
import cPickle
import time
import shutil
import struct 
import urllib

import msgpack
import zmq

from dpark.shareddict import SharedDicts
from dpark.env import env
from dpark.util import spawn

logger = logging.getLogger("cache")

class Cache:
    data = {}

    def get(self, key): 
        return self.data.get(key)
    
    def put(self, key, value):
        if value is not None:
            v = list(value)
            self.data[key] = v
            return v
        else:
            self.data.pop(key, None)

    def clear(self):
        self.data.clear()

class DiskCache(Cache):
    def __init__(self, tracker, path):
        if not os.path.exists(path):
            try: os.makedirs(path)
            except: pass
        self.tracker = tracker
        self.root = path

    def get_path(self, key):
        return os.path.join(self.root, '%s_%s' % key)

    def get(self, key):
        p = self.get_path(key)
        #if os.path.exists(p):
        #    return self.load(p)

        # load from other node
        if not env.get('SERVER_URI'):
            return
        rdd_id, index = key
        locs = self.tracker.getCacheUri(rdd_id, index)
        if not locs:
            return

        serve_uri = locs[-1]
        uri = '%s/cache/%s' % (serve_uri, os.path.basename(p))
        try:
            return self.load(uri)
        except IOError, e:
            self.tracker.removeHost(rdd_id, index, serve_uri)
            logger.warning('load from cache %s failed: %s', uri, e)

    def put(self, key, value):
        p = self.get_path(key)
        if value is not None:
            return self.save(self.get_path(key), value)
        else:
            os.remove(p)

    def clear(self):
        shutil.rmtree(self.root)

    def load(self, path):
        if path.startswith('http://'):
            f = urllib.urlopen(path)
            if f.code == 404:
                f.close()
                raise IOError("%s not found" % url)
        else:
            f = open(path, 'rb')
        count, = struct.unpack("I", f.read(4))
        if not count: return
        unpacker = msgpack.Unpacker(f, use_list=False)
        for i in xrange(count):
            _type, data = unpacker.next()
            if _type == 0:
                yield marshal.loads(data)
            else:
                yield cPickle.loads(data)
        f.close()

    def save(self, path, items):
        # TODO: purge old cache
        tp = "%s.%d" % (path, os.getpid())
        with open(tp, 'wb') as f:
            c = 0
            f.write(struct.pack("I", c))
            try_marshal = True
            for v in items:
                if try_marshal:
                    try:
                        r = 0, marshal.dumps(v)
                    except Exception:
                        r = 1, cPickle.dumps(v, -1)
                        try_marshal = False
                else:
                    r = 1, cPickle.dumps(v, -1)
                f.write(msgpack.packb(r)) 
                c += 1
                yield v
            
            bytes = f.tell()
            if bytes > 10<<20:
                logger.warning("cached result is %dMB (larger than 10MB)", bytes>>20)
            # count    
            f.seek(0)
            f.write(struct.pack("I", c))
        
        os.rename(tp, path)


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
class RegisterRDD(CacheTrackerMessage):
    def __init__(self, rddId, numPartitions):
        self.rddId = rddId
        self.numPartitions = numPartitions
class GetCacheUri(CacheTrackerMessage):
    def __init__(self, rddId, partition):
        self.rddId = rddId
        self.index = partition
class StopCacheTracker(CacheTrackerMessage):pass

class CacheTrackerServer(object):
    def __init__(self, locs):
        self.addr = None
        self.locs = locs
        self.thread = None

    def start(self):
        self.thread = spawn(self.run)
        while self.addr is None:
            time.sleep(0.01)

    def stop(self):
        sock = env.ctx.socket(zmq.REQ)
        sock.connect(self.addr)
        sock.send_pyobj(StopCacheTracker())
        sock.close()
        self.thread.join()

    def run(self):
        locs = self.locs
        sock = env.ctx.socket(zmq.REP)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        self.addr = "tcp://%s:%d" % (socket.gethostname(), port)
        logger.debug("CacheTrackerServer started at %s", self.addr)
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
                if msg.host in locs[msg.rddId][msg.partition]:
                    locs[msg.rddId][msg.partition].remove(msg.host)
                reply('OK')
            elif isinstance(msg, GetCacheUri):
                if msg.rddId in locs:
                    reply(locs[msg.rddId][msg.index])
                else:
                    reply([])
            elif isinstance(msg, StopCacheTracker):
                reply('OK')
                break
            else:
                logger.error("unexpected msg %s %s", msg, type(msg))
                reply('ERROR')
        sock.close()
        logger.debug("stop CacheTrackerServer %s", self.addr)

class CacheTrackerClient:
    def __init__(self, addr):
        self.addr = addr
        self.sock = None

    def call(self, msg):
        if self.sock is None:
            self.sock = env.ctx.socket(zmq.REQ)
            self.sock.connect(self.addr)

        self.sock.send_pyobj(msg)
        return self.sock.recv_pyobj()

    def stop(self):
        if self.sock:
            self.sock.close()
        #logger.debug("stop %s", self.__class__)

class LocalCacheTracker(object):
    def __init__(self, isMaster):
        self.isMaster = isMaster
        self.locs = {}
        self.cache = Cache()

    def clear(self):
        self.cache.clear()

    def registerRDD(self, rddId, numPartitions):
        if rddId not in self.locs:
            logger.debug("Registering RDD ID %d with cache", rddId)
            self.locs[rddId] = [[] for i in range(numPartitions)]
    
    def getLocationsSnapshot(self):
        return self.locs

    def getCachedLocs(self, rdd_id, index):
        def parse_hostname(uri):
            if uri.startswith('http://'):
                h = uri.split(':')[1].rsplit('/', 1)[-1]
                return h
            return ''
        return map(parse_hostname, self.locs[rdd_id][index])
    
    def getCacheUri(self, rdd_id, index):
        return self.locs[rdd_id][index]

    def addHost(self, rdd_id, index, host):
        self.locs[rdd_id][index].append(host)

    def removeHost(self, rdd_id, index, host):
        if host in self.locs[rdd_id][index]:
            self.locs[rdd_id][index].remove(host)

    def getOrCompute(self, rdd, split):
        key = (rdd.id, split.index)
        cachedVal = self.cache.get(key)
        if cachedVal is not None:
            logger.debug("Found partition in cache! %s", key)
            return cachedVal
        
        logger.debug("partition not in cache, %s", key)
        r = self.cache.put(key, rdd.compute(split))
        serve_uri = env.get('SERVER_URI')
        if serve_uri:
            self.addHost(rdd.id, split.index, serve_uri)
        return r

    def stop(self):
        self.clear()


class CacheTracker(LocalCacheTracker):
    def __init__(self, isMaster):
        LocalCacheTracker.__init__(self, isMaster)
        if isMaster:
            self.server = CacheTrackerServer(self.locs)
            self.server.start()
            addr = self.server.addr
            env.register('CacheTrackerAddr', addr)
        else:
            cachedir = os.path.join(env.get('WORKDIR')[0], 'cache')
            self.cache = DiskCache(self, cachedir)
            addr = env.get('CacheTrackerAddr')

        self.client = CacheTrackerClient(addr)

    def getCacheUri(self, rdd_id, index):
        return self.client.call(GetCacheUri(rdd_id, index))

    def addHost(self, rdd_id, index, host):
        return self.client.call(AddedToCache(rdd_id, index, host))

    def removeHost(self, rdd_id, index, host):
        return self.client.call(DroppedFromCache(rdd_id, index, host)) 

    def stop(self):
        self.clear()
        self.client.stop()
        if self.isMaster:
            self.server.stop()

    def __getstate__(self):
        raise Exception("!!!")


def test():
    logging.basicConfig(level=logging.DEBUG)
    from dpark.context import DparkContext
    dc = DparkContext("local")
    dc.start()
    nums = dc.parallelize(range(100), 10)
    tracker = CacheTracker(True)
    tracker.registerRDD(nums.id, len(nums))
    split = nums.splits[0]
    print list(tracker.getOrCompute(nums, split))
    print list(tracker.getOrCompute(nums, split))
    print tracker.getLocationsSnapshot()
    tracker.stop()

if __name__ == '__main__':
    test()

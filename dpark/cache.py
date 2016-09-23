import os
import marshal
import cPickle
import shutil
import struct
import urllib

import msgpack

from dpark.env import env
from dpark.util import mkdir_p, atomic_file, get_logger
from dpark.tracker import GetValueMessage, AddItemMessage, RemoveItemMessage

logger = get_logger(__name__)

class Cache:
    data = {}

    def get(self, key):
        return self.data.get(key)

    def put(self, key, value, is_iterator=False):
        if value is not None:
            if is_iterator:
                value = list(value)
            self.data[key] = value
            return value
        else:
            self.data.pop(key, None)

    def clear(self):
        self.data.clear()

class DiskCache(Cache):
    def __init__(self, tracker, path):
        try:
            mkdir_p(path)
        except:
            pass
        self.tracker = tracker
        self.root = path

    def get_path(self, key):
        return os.path.join(self.root, '%s_%s' % key)

    def get(self, key):
        p = self.get_path(key)
        if os.path.exists(p):
            return self.load(open(p, 'rb'))

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
            f = urllib.urlopen(uri)
        except IOError:
            logger.warning('urlopen cache uri %s failed', uri)
            raise
        if f.code == 404:
            logger.warning('load from cache %s failed', uri)
            self.tracker.removeHost(rdd_id, index, serve_uri)
            f.close()
            return
        return self.load(f)

    def put(self, key, value, is_iterator=False):
        p = self.get_path(key)
        if value is not None:
            return self.save(self.get_path(key), value)
        else:
            os.remove(p)

    def clear(self):
        try:
            shutil.rmtree(self.root)
        except OSError, e:
            pass

    def load(self, f):
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
        with atomic_file(path) as f:
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


class BaseCacheTracker(object):
    cache = None

    def registerRDD(self, rddId, numPartitions):
        pass

    def getLocationsSnapshot(self):
        pass

    def getCachedLocs(self, rdd_id, index):
        pass

    def getCacheUri(self, rdd_id, index):
        pass

    def addHost(self, rdd_id, index, host):
        pass

    def removeHost(self, rdd_id, index, host):
        pass

    def clear(self):
        self.cache.clear()

    def getOrCompute(self, rdd, split):
        key = (rdd.id, split.index)
        cachedVal = self.cache.get(key)
        if cachedVal is not None:
            logger.debug("Found partition in cache! %s", key)
            for i in cachedVal:
                yield i

        else:
            logger.debug("partition not in cache, %s", key)
            for i in self.cache.put(key, rdd.compute(split), is_iterator=True):
                yield i

            serve_uri = env.get('SERVER_URI')
            if serve_uri:
                self.addHost(rdd.id, split.index, serve_uri)

    def stop(self):
        self.clear()

class CacheTracker(BaseCacheTracker):
    def __init__(self):
        cachedir = os.path.join(env.get('WORKDIR')[0], 'cache')
        self.cache = DiskCache(self, cachedir)
        self.client = env.trackerClient
        if env.isMaster:
            self.locs = env.trackerServer.locs
        self.rdds = {}

    def registerRDD(self, rddId, numPartitions):
        self.rdds[rddId] = numPartitions

    def getLocationsSnapshot(self):
        result = {}
        for rdd_id, partitions in self.rdds.items():
            result[rdd_id] = [self.locs.get('cache:%s-%s' % (rdd_id, index), [])
                    for index in xrange(partitions)]

        return result

    def getCachedLocs(self, rdd_id, index):
        def parse_hostname(uri):
            if uri.startswith('http://'):
                h = uri.split(':')[1].rsplit('/', 1)[-1]
                return h
            return ''
        return map(parse_hostname, self.locs.get('cache:%s-%s' % (rdd_id, index), []))

    def getCacheUri(self, rdd_id, index):
        return self.client.call(GetValueMessage('cache:%s-%s' % (rdd_id, index)))

    def addHost(self, rdd_id, index, host):
        return self.client.call(AddItemMessage('cache:%s-%s' % (rdd_id, index), host))

    def removeHost(self, rdd_id, index, host):
        return self.client.call(RemoveItemMessage('cache:%s-%s' % (rdd_id, index), host))

    def getOrCompute(self, rdd, split):
        key = (rdd.id, split.index)
        cachedVal = self.cache.get(key)
        if cachedVal is not None:
            logger.debug("Found partition in cache! %s", key)
            for i in cachedVal:
                yield i

        else:
            logger.debug("partition not in cache, %s", key)
            for i in self.cache.put(key, rdd.compute(split), is_iterator=True):
                yield i

            serve_uri = env.get('SERVER_URI')
            if serve_uri:
                self.addHost(rdd.id, split.index, serve_uri)

    def __getstate__(self):
        raise Exception("!!!")


def test():
    import logging
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

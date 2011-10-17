import os, logging
import threading

class SparkEnv(threading.local):
    def __init__(self):
        self.started = False

    def start(self, isMaster):
        if getattr(self, 'started', False):
            return
        logging.info("start env  in %s", os.getpid())
        from cache import BoundedMemoryCache, CacheTracker
        from shuffle import MapOutputTracker, SimpleShuffleFetcher
        self.cache = BoundedMemoryCache()
        self.cacheTracker = CacheTracker(isMaster, self.cache)
        self.mapOutputTracker = MapOutputTracker(isMaster)
        self.shuffleFetcher = SimpleShuffleFetcher()
        self.started = True

    def stop(self):
        if not getattr(self, 'started', False):
            return
        logging.info("stop env  in %s", os.getpid())
        self.cacheTracker.stop()
        self.mapOutputTracker.stop()
        self.shuffleFetcher.stop()
        self.started = False

env = SparkEnv()

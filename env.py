import os, logging
import threading

class SparkEnv(threading.local):
    def __init__(self):
        self.started = False

    def start(self, isMaster, cacheAddr=None, outputAddr=None, shuffleDir=None):
        if getattr(self, 'started', False):
            return
        logging.debug("start env in %s: %s %s %s %s", os.getpid(),
                isMaster, cacheAddr, outputAddr, shuffleDir)
        from cache import BoundedMemoryCache, CacheTracker
        from shuffle import LocalFileShuffle, MapOutputTracker, SimpleShuffleFetcher
        self.cache = BoundedMemoryCache()
        self.cacheTracker = CacheTracker(isMaster, self.cache, cacheAddr)
        self.mapOutputTracker = MapOutputTracker(isMaster, outputAddr)
        
        if shuffleDir:
            LocalFileShuffle.initialize(shuffleDir)
            self.shuffleDir = shuffleDir
        self.shuffleFetcher = SimpleShuffleFetcher()
        self.started = True

    def stop(self):
        if not getattr(self, 'started', False):
            return
        logging.debug("stop env  in %s", os.getpid())
        self.cacheTracker.stop()
        self.mapOutputTracker.stop()
        self.shuffleFetcher.stop()
        self.started = False

env = SparkEnv()

import os, logging

class SparkEnv:
    def __init__(self):
        self.started = False

    def start(self, isMaster):
        if self.started:
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
        if not self.started:
            return
        logging.info("stop env  in %s", os.getpid())
        self.cacheTracker.stop()
        self.mapOutputTracker.stop()
        self.shuffleFetcher.stop()
        self.started = False

env = SparkEnv()

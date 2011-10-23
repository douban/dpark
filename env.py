import os, logging
import threading

class SparkEnv:
    environ = {}
    @classmethod
    def register(cls, name, value):
        cls.environ[name] = value
    @classmethod
    def get(cls, name, default=None):
        return cls.environ.get(name, default)

    def __init__(self):
        self.started = False

    def start(self, isMaster, environ={}):
        if getattr(self, 'started', False):
            return
        logging.debug("start env in %s: %s %s", os.getpid(),
                isMaster, environ)
        self.environ.update(environ)

        from cache import CacheTracker
        self.cacheTracker = CacheTracker(isMaster)
        
        from shuffle import LocalFileShuffle, MapOutputTracker, SimpleShuffleFetcher
        LocalFileShuffle.initialize()
        self.mapOutputTracker = MapOutputTracker(isMaster)
        self.shuffleFetcher = SimpleShuffleFetcher()

        from broadcast import Broadcast
        Broadcast.initialize(isMaster)

        self.started = True
        logging.debug("env started") 
    
    def stop(self):
        if not getattr(self, 'started', False):
            return
        logging.debug("stop env in %s", os.getpid())
        self.cacheTracker.stop()
        self.mapOutputTracker.stop()
        self.shuffleFetcher.stop()
        self.started = False

env = SparkEnv()

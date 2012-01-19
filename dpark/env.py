import os, logging
import time
import socket
import threading

class DparkEnv:
    environ = {}
    @classmethod
    def register(cls, name, value):
        cls.environ[name] = value
    @classmethod
    def get(cls, name, default=None):
        return cls.environ.get(name, default)

    def __init__(self):
        self.started = False

    def start(self, isMaster, environ={}, isLocal=False):
        if getattr(self, 'started', False):
            return
        logging.debug("start env in %s: %s %s", os.getpid(),
                isMaster, environ)
        if isMaster:
            root = '/tmp/dpark'
            if not isLocal:
                if os.path.exists('/home2/dpark'):
                    root = '/home2/dpark'
                elif os.path.exists('/mfs/tmp'):
                    root = '/mfs/tmp/dpark'
            name = '%s-%s-%d' % (time.strftime("%Y%m%d-%H%M%S"),
                socket.gethostname(), os.getpid())
            self.workdir = os.path.join(root, name)
            os.makedirs(self.workdir)
            self.environ['WORKDIR'] = self.workdir
        else:
            self.environ.update(environ)

        from cache import CacheTracker
        self.cacheTracker = CacheTracker(isMaster)
        
        from shuffle import LocalFileShuffle, MapOutputTracker, SimpleShuffleFetcher
        LocalFileShuffle.initialize(isMaster)
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
        
        logging.debug("cleaning workdir ...")
        try:
            for root,dirs,names in os.walk(self.workdir, topdown=False):
                for name in names:
                    path = os.path.join(root, name)
                    os.remove(path)
                for d in dirs:
                    os.rmdir(os.path.join(root,d))
            os.rmdir(self.workdir)
        except OSError:
            pass
        logging.debug("done.")

        self.started = False

env = DparkEnv()

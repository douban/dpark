import os, logging
import time
import socket
import shutil

import zmq

from dpark import util
import dpark.conf as conf

logger = logging.getLogger("env")

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
        if self.started:
            return
        logger.debug("start env in %s: %s %s", os.getpid(),
                isMaster, environ)
        self.isMaster = isMaster
        self.isLocal = isLocal
        if isMaster:
            roots = conf.DPARK_WORK_DIR
            if isinstance(roots, str):
                roots = roots.split(',')
            if isLocal:
                root = roots[0] # for local mode 
                if not os.path.exists(root):
                    os.mkdir(root, 0777)
                    os.chmod(root, 0777) # because of umask

            name = '%s-%s-%d' % (time.strftime("%Y%m%d-%H%M%S"),
                socket.gethostname(), os.getpid())
            self.workdir = [os.path.join(root, name) for root in roots]
            for d in self.workdir:
                if not os.path.exists(d):
                    try: os.makedirs(d)
                    except OSError: pass
            self.environ['WORKDIR'] = self.workdir
            self.environ['COMPRESS'] = util.COMPRESS
        else:
            self.environ.update(environ)
            if self.environ['COMPRESS'] != util.COMPRESS:
                raise Exception("no %s available" % self.environ['COMPRESS'])

        self.ctx = zmq.Context()

        from dpark.cache import CacheTracker, LocalCacheTracker
        if isLocal:
            self.cacheTracker = LocalCacheTracker(isMaster)
        else:
            self.cacheTracker = CacheTracker(isMaster)

        from dpark.shuffle import LocalFileShuffle, MapOutputTracker, LocalMapOutputTracker
        LocalFileShuffle.initialize(isMaster)
        if isLocal:
            self.mapOutputTracker = LocalMapOutputTracker(isMaster)
        else:
            self.mapOutputTracker = MapOutputTracker(isMaster)
        from dpark.shuffle import SimpleShuffleFetcher, ParallelShuffleFetcher
        #self.shuffleFetcher = SimpleShuffleFetcher()
        self.shuffleFetcher = ParallelShuffleFetcher(2)

        from dpark.broadcast import TheBroadcast
        TheBroadcast.initialize(isMaster)

        self.started = True
        logger.debug("env started")

    def stop(self):
        if not getattr(self, 'started', False):
            return
        logger.debug("stop env in %s", os.getpid())
        self.shuffleFetcher.stop()
        self.cacheTracker.stop()
        self.mapOutputTracker.stop()
        from dpark.broadcast import TheBroadcast
        TheBroadcast.shutdown()
       
        logger.debug("cleaning workdir ...")
        for d in self.workdir:
            shutil.rmtree(d, True)
        logger.debug("done.")

        self.started = False

env = DparkEnv()

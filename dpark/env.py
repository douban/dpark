import os, logging
import time
import socket
import threading
import zmq

import util

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
        if isMaster:
            if os.environ.has_key('DPARK_WORK_DIR'):
                root = os.environ['DPARK_WORK_DIR']
            else:
                root = '/tmp/dpark'

            if not os.path.exists(root):
                os.mkdir(root, 0777)
                os.chmod(root, 0777) # because of umask
            name = '%s-%s-%d' % (time.strftime("%Y%m%d-%H%M%S"),
                socket.gethostname(), os.getpid())
            self.workdir = os.path.join(root, name)
            os.makedirs(self.workdir)
            self.environ['WORKDIR'] = self.workdir
            self.environ['COMPRESS'] = util.COMPRESS
        else:
            self.environ.update(environ)
            if self.environ['COMPRESS'] != util.COMPRESS:
                raise Exception("no %s available" % self.environ['COMPRESS'])

        self.ctx = zmq.Context()

        from cache import CacheTracker, LocalCacheTracker
        if isLocal:
            self.cacheTracker = LocalCacheTracker(isMaster)
        else:
            self.cacheTracker = CacheTracker(isMaster)

        from shuffle import LocalFileShuffle, MapOutputTracker, LocalMapOutputTracker
        LocalFileShuffle.initialize(isMaster)
        if isLocal:
            self.mapOutputTracker = LocalMapOutputTracker(isMaster)
        else:
            self.mapOutputTracker = MapOutputTracker(isMaster)
        from shuffle import SimpleShuffleFetcher, ParallelShuffleFetcher
        #self.shuffleFetcher = SimpleShuffleFetcher()
        self.shuffleFetcher = ParallelShuffleFetcher(2)

        from broadcast import Broadcast
        Broadcast.initialize(isMaster)

        self.started = True
        logger.debug("env started") 
    
    def stop(self):
        if not getattr(self, 'started', False):
            return
        logger.debug("stop env in %s", os.getpid())
        self.cacheTracker.stop()
        self.mapOutputTracker.stop()
        self.shuffleFetcher.stop()
       
        if self.isMaster:
            logger.debug("cleaning workdir ...")
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
            logger.debug("done.")

        self.started = False

env = DparkEnv()

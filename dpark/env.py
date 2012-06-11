import os, logging
import time
import socket
import threading

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
        if getattr(self, 'started', False):
            return
        logger.debug("start env in %s: %s %s", os.getpid(),
                isMaster, environ)
        if isMaster:
            if isLocal:
                root = '/tmp/dpark'
                self.dfs = True
            elif os.environ.has_key('DPARK_SHARE_DIR'):
                root = os.environ['DPARK_SHARE_DIR']
                self.dfs = True
            elif os.environ.has_key('DPARK_WORK_DIR'):
                root = os.environ['DPARK_WORK_DIR']
                self.dfs = False
            elif os.path.exists('/data1/dpark'):
                root = '/data1/dpark'
                self.dfs = False
            else:
                if os.path.exists('/home2/dpark'):
                    root = '/home2/dpark'
                elif os.path.exists('/mfs/tmp'):
                    root = '/mfs/tmp/dpark'
                else:
                    root = '/tmp/dpark'
                self.dfs = True

            if not os.path.exists(root):
                os.mkdir(root, 0777)
                os.chmod(root, 0777) # because of umask
            name = '%s-%s-%d' % (time.strftime("%Y%m%d-%H%M%S"),
                socket.gethostname(), os.getpid())
            self.workdir = os.path.join(root, name)
            os.makedirs(self.workdir)
            self.environ['WORKDIR'] = self.workdir
            self.environ['DPARK_HAS_DFS'] = str(self.dfs)
        else:
            self.environ.update(environ)
            self.dfs = (self.environ['DPARK_HAS_DFS'] == 'True')

        from cache import CacheTracker
        self.cacheTracker = CacheTracker(isMaster)
        
        from shuffle import LocalFileShuffle, MapOutputTracker, SimpleShuffleFetcher
        LocalFileShuffle.initialize(isMaster)
        self.mapOutputTracker = MapOutputTracker(isMaster)
        self.shuffleFetcher = SimpleShuffleFetcher()

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

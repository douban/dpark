import os, logging
import time
import socket
import shutil

import zmq

from dpark import util
import dpark.conf as conf

logger = logging.getLogger(__name__)

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

    def start(self, isMaster, environ={}):
        if self.started:
            return
        logger.debug("start env in %s: %s %s", os.getpid(),
                isMaster, environ)
        self.isMaster = isMaster
        if isMaster:
            roots = conf.DPARK_WORK_DIR
            if isinstance(roots, str):
                roots = roots.split(',')
            name = '%s-%s-%d' % (time.strftime("%Y%m%d-%H%M%S"),
                socket.gethostname(), os.getpid())
            self.workdir = [os.path.join(root, name) for root in roots]
            for d in self.workdir:
                if not os.path.exists(d):
                    try: os.makedirs(d)
                    except OSError: pass
            self.environ['SERVER_URI'] = 'file://' + self.workdir[0]
            self.environ['WORKDIR'] = self.workdir
            self.environ['COMPRESS'] = util.COMPRESS
        else:
            self.environ.update(environ)
            if self.environ['COMPRESS'] != util.COMPRESS:
                raise Exception("no %s available" % self.environ['COMPRESS'])

        self.ctx = zmq.Context()


        from dpark.tracker import TrackerServer, TrackerClient
        if isMaster:
            self.trackerServer = TrackerServer()
            self.trackerServer.start()
            addr = self.trackerServer.addr
            env.register('TrackerAddr', addr)
        else:
            addr = env.get('TrackerAddr')

        self.trackerClient = TrackerClient(addr)

        from dpark.cache import CacheTracker
        self.cacheTracker = CacheTracker()

        from dpark.shuffle import LocalFileShuffle, MapOutputTracker
        LocalFileShuffle.initialize(isMaster)
        self.mapOutputTracker = MapOutputTracker()
        from dpark.shuffle import SimpleShuffleFetcher, ParallelShuffleFetcher
        #self.shuffleFetcher = SimpleShuffleFetcher()
        self.shuffleFetcher = ParallelShuffleFetcher(2)

        from dpark.broadcast import start_manager
        start_manager(isMaster)

        self.started = True
        logger.debug("env started")

    def stop(self):
        if not getattr(self, 'started', False):
            return
        logger.debug("stop env in %s", os.getpid())
        self.shuffleFetcher.stop()
        self.cacheTracker.stop()
        self.mapOutputTracker.stop()
        if self.isMaster:
            self.trackerServer.stop()
        from dpark.broadcast import stop_manager
        stop_manager()

        logger.debug("cleaning workdir ...")
        for d in self.workdir:
            shutil.rmtree(d, True)
        logger.debug("done.")

        self.started = False

env = DparkEnv()

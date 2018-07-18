from __future__ import absolute_import
import os
import socket
import shutil
import uuid
import tempfile

from dpark import utils
from dpark.utils.log import get_logger
from dpark.utils.memory import MemoryChecker
import dpark.conf as conf

logger = get_logger(__name__)


class TaskStats(object):

    def __init__(self):
        self._reset()

    def _reset(self):
        self.bytes_max_rss = 0
        self.secs_all = 0

        # broadcast
        self.secs_broadcast = 0

        # shuffle: fetch and merge -> run and merge ->  dump
        self.bytes_fetch = 0
        self.bytes_dump = 0
        self.secs_fetch = 0  # 0 for sort merge if not use disk
        self.secs_dump = 0

        # rotate
        self.num_fetch_rotate = 0  # 0 if all in memory
        self.num_dump_rotate = 0  # 1 if all in memory


class DparkEnv:
    environ = {}
    trackerServer = None

    @classmethod
    def register(cls, name, value):
        cls.environ[name] = value

    @classmethod
    def get(cls, name, default=None):
        return cls.environ.get(name, default)

    def __init__(self):
        self.meminfo = MemoryChecker()
        self.started = False
        self.task_stats = TaskStats()
        name = self.get('DPARK_ID')
        if name is None:
            name = '%s-%s' % (socket.gethostname(), uuid.uuid4())
            self.register('DPARK_ID', name)

        self.workdir = self.get('WORKDIR')
        if self.workdir is None:
            roots = conf.DPARK_WORK_DIR
            if isinstance(roots, str):
                roots = roots.split(',')

            if not roots:
                logger.warning('Cannot get WORKDIR, use temp dir instead.')
                roots = [tempfile.gettempdir()]

            self.workdir = [os.path.join(root, name) for root in roots]
            self.register('WORKDIR', self.workdir)

        if 'SERVER_URI' not in self.environ:
            self.register('SERVER_URI', 'file://' + self.workdir[0])

        compress = self.get('COMPRESS')
        if compress is None:
            self.register('COMPRESS', utils.COMPRESS)
            compress = self.get('COMPRESS')

        if compress != utils.COMPRESS:
            raise Exception("no %s available" % compress)

        self.trackerClient = None
        self.cacheTracker = None
        self.shuffleFetcher = None
        self.mapOutputTracker = None

    def start(self):
        if self.started:
            return
        self.started = True
        logger.debug("start env in %s", os.getpid())
        for d in self.workdir:
            utils.mkdir_p(d)

        if 'TRACKER_ADDR' not in self.environ:
            from dpark.tracker import TrackerServer
            self.trackerServer = TrackerServer()
            self.trackerServer.start()
            self.register('TRACKER_ADDR', self.trackerServer.addr)

        from dpark.tracker import TrackerClient
        addr = self.get('TRACKER_ADDR')
        self.trackerClient = TrackerClient(addr)

        from dpark.cache import CacheTracker
        self.cacheTracker = CacheTracker()

        from dpark.shuffle import MapOutputTracker
        self.mapOutputTracker = MapOutputTracker()
        from dpark.shuffle import ParallelShuffleFetcher
        self.shuffleFetcher = ParallelShuffleFetcher(2)

        from dpark.broadcast import start_guide_manager, GUIDE_ADDR
        if GUIDE_ADDR not in self.environ:
            start_guide_manager()

        logger.debug("env started")

    def stop(self):
        if not getattr(self, 'started', False):
            return
        self.started = False
        logger.debug("stop env in %s", os.getpid())
        self.trackerClient.stop()
        self.shuffleFetcher.stop()
        self.cacheTracker.stop()
        self.mapOutputTracker.stop()
        if self.trackerServer is not None:
            self.trackerServer.stop()
            self.environ.pop('TRACKER_ADDR', None)

        from dpark.broadcast import stop_manager
        stop_manager()

        logger.debug("cleaning workdir ...")
        for d in self.workdir:
            shutil.rmtree(d, True)
        logger.debug("done.")


env = DparkEnv()

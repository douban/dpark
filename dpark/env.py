from __future__ import absolute_import
import os
import socket
import shutil
import random
import signal
import uuid as uuid_pkg

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


def prepare_file_open(base, subpath):
    path = os.path.join(base, subpath)
    if os.path.exists(path):
        os.remove(path)
    else:
        utils.mkdir_p(os.path.dirname(path))
    return path


class WorkDir(object):

    def __init__(self):
        self.workdirs = []
        self.inited = False
        self.seq_id = 0

    @property
    def next_id(self):
        self.seq_id += 1
        return self.seq_id

    @property
    def main(self):
        return self.workdirs[0]

    def init(self, dpark_id):
        if self.inited:
            return
        roots = conf.DPARK_WORK_DIR.split(",")
        self.workdirs = []
        es = {}
        for i, root in enumerate(roots):
            try:
                while not os.path.exists(root):
                    os.makedirs(root)
                    os.chmod(root, 0o777)  # because umask
                workdir = os.path.join(root, dpark_id)
                self.workdirs.append(workdir)
            except Exception as e:
                es[root] = e

        if not self.workdirs:
            raise Exception("workdirs not available: {}".format(es))

        utils.mkdir_p(self.main)  # executor will loc it
        self.inited = True

    def export(self, tmppath, subpath):
        if not os.path.exists(tmppath):
            raise Exception("tmppath %s for % not exists", tmppath, subpath)
        path = os.path.join(self.main, subpath)
        if os.path.exists(path):
            logger.warning("rm old localfile %s", path)
            os.remove(path)
        dirpath = os.path.dirname(path)
        while not os.path.exists(dirpath):
            utils.mkdir_p(dirpath)
        logger.debug("export %s %s", tmppath, path)
        os.symlink(tmppath, path)
        return path

    def get_path(self, subpath):
        return os.path.join(self.main, subpath)

    def _use_memdir(self, datasize):
        if env.meminfo.rss + datasize > env.meminfo.mem_limit_soft:
            return False

        st = os.statvfs(self.main)
        free = st.f_bfree * st.f_bsize
        ratio = st.f_bfree * 1.0 / st.f_blocks
        if free < max(datasize, 1 << 30) or ratio < 0.66:
            return False

        env.meminfo.add(datasize)
        return True

    def _choose_disk_workdir(self):
        disk_dirs = list(self.workdirs[1:])
        if not disk_dirs:
            return self.workdirs[0]

        random.shuffle(disk_dirs)
        for d in disk_dirs:
            try:
                if not os.path.exists(d):
                    utils.mkdir_p(d)
                return d
            except:
                pass
        else:
            logger.warning("_choose_disk_workdir fail")
            return self.workdirs[0]

    @classmethod
    def _get_tmp_subpath(cls, prefix):
        uuid = uuid_pkg.uuid4().hex  # hex is short
        subpath = '{}-{}-{}.temp'.format(prefix, uuid, os.getpid())
        return subpath

    def alloc_tmp_dir(self, prefix, mem=False):
        root = self.main if mem else self._choose_disk_workdir()
        path = os.path.join(root, self._get_tmp_subpath(prefix))
        os.makedirs(path)
        return path

    def alloc_tmp_file(self, prefix, mem_first=False, datasize=0):
        root = self.main if (mem_first and self._use_memdir(datasize)) else self._choose_disk_workdir()
        path = os.path.join(root, self._get_tmp_subpath(prefix))
        return path

    def clean_up(self):
        for d in self.workdirs:
            while os.path.exists(d):
                try:
                    shutil.rmtree(d, True)
                except:
                    pass

    def setup_cleaner_process(self):
        ppid = os.getpid()
        pid = os.fork()
        if pid == 0:
            os.setsid()
            pid = os.fork()
            if pid == 0:
                try:
                    import psutil
                except ImportError:
                    os._exit(1)

                try:
                    psutil.Process(ppid).wait()
                    os.killpg(ppid, signal.SIGKILL)  # kill workers
                except Exception:
                    pass  # make sure to exit

                finally:
                    self.clean_up()
            os._exit(0)
        os.wait()


class DparkEnv(object):

    SERVER_URI = "SERVER_URI"
    COMPRESS = "COMPRESS"
    TRACKER_ADDR = "TRACKER_ADDR"
    DPARK_ID = "DPARK_ID"

    def __init__(self):
        self.environ = {}
        self.task_stats = TaskStats()
        self.workdir = WorkDir()

        self.master_started = False
        self.slave_started = False

        # master
        self.trackerServer = None
        self.cacheTrackerServer = None

        # slave
        #   trackerServer clients
        self.trackerClient = None
        self.cacheTracker = None
        #   threads
        self.meminfo = MemoryChecker()
        self.shuffleFetcher = None

    def register(self, name, value):
        self.environ[name] = value

    def get(self, name, default=None):
        return self.environ.get(name, default)

    @property
    def server_uri(self):
        return self.environ[self.SERVER_URI]

    def start_master(self):
        if self.master_started:
            return
        self.master_started = True
        logger.debug("start master env in pid %s", os.getpid())

        self._start_common()
        self.register(self.COMPRESS, utils.COMPRESS)

        dpark_id = '{}-{}'.format(socket.gethostname(), uuid_pkg.uuid4())
        logger.info("dpark_id = %s", dpark_id)
        self.register(self.DPARK_ID, dpark_id)

        self.workdir.init(dpark_id)

        self.register(self.SERVER_URI, 'file://' + self.workdir.main)  # overwrited when executor starts

        # start centra servers in scheduler
        from dpark.tracker import TrackerServer
        self.trackerServer = TrackerServer()
        self.trackerServer.start()
        self.register(self.TRACKER_ADDR, self.trackerServer.addr)

        from dpark.cache import CacheTrackerServer
        self.cacheTrackerServer = CacheTrackerServer()

        from dpark.broadcast import start_guide_manager
        start_guide_manager()

        self._start_common()

        logger.debug("master env started")

    def _start_common(self):
        if not self.shuffleFetcher:
            from dpark.shuffle import ParallelShuffleFetcher
            self.shuffleFetcher = ParallelShuffleFetcher(2)  # start lazy, use also in scheduler(rdd.iterator)

    def start_slave(self):
        """Called after env is updated."""
        if self.slave_started:
            return
        self.slave_started = True

        self._start_common()

        compress = self.get(self.COMPRESS)
        if compress != utils.COMPRESS:
            raise Exception("no %s available" % compress)

        # init clients
        from dpark.tracker import TrackerClient
        self.trackerClient = TrackerClient(self.get(self.TRACKER_ADDR))

        from dpark.cache import CacheTracker
        self.cacheTracker = CacheTracker()

        self.workdir.init(env.get(self.DPARK_ID))
        logger.debug("env started")

    def stop(self):
        logger.debug("stop env in pid %s", os.getpid())

        if self.master_started:
            if self.trackerServer is not None:
                self.trackerServer.stop()
                self.environ.pop(self.TRACKER_ADDR, None)
            self.master_started = False

        if self.slave_started:
            self.trackerClient.stop()
            self.cacheTracker.stop()
            self.slave_started = False

        if self.slave_started or self.master_started:
            self.shuffleFetcher.stop()

        from dpark.broadcast import stop_manager
        stop_manager()

        logger.debug("cleaning workdir ...")
        self.workdir.clean_up()
        logger.debug("env stopped.")


env = DparkEnv()

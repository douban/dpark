import os
import sys
import time
import psutil
import resource
import threading
import platform

from dpark.utils.log import get_logger

logger = get_logger(__name__)

ERROR_TASK_OOM = 3


class MemoryChecker(object):
    """ value in MBytes
        only used in mesos task
        start early
    """

    def __init__(self):
        self.rss = 0
        self._stop = False
        self.mf = None
        self.check = True
        self.addation = 0
        self.mem = 100 << 30
        self.ratio = 0.8
        self.thread = None
        self.task_id = None
        self.oom = False

    @property
    def mem_limit_soft(self):
        return int(self.mem * self.ratio)

    def add(self, n):
        self.addation += n

    @property
    def rss_rt(self):
        return self.mf().rss + self.addation

    @classmethod
    def maxrss(cls):
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024

    def _kill(self, rss, from_main_thread):
        template = "task used too much memory: %d MB > %d MB * 1.5," \
                   "kill it. use -M argument or taskMemory " \
                   "to request more memory."
        msg = template % (rss >> 20, self.mem >> 20)

        logger.warning(msg)
        if from_main_thread:
            os._exit(ERROR_TASK_OOM)
        else:
            if sys.version[0] == 3:
                import _thread
            else:
                import thread as _thread
            self.oom = True
            _thread.interrupt_main()

    def after_rotate(self):
        limit = self.mem_limit_soft
        self.rss = rss = self.rss_rt
        if rss > limit * 0.9:
            if rss > self.mem * 1.5:
                self._kill(rss, from_main_thread=True)
            else:
                new_limit = max(limit, rss) * 1.1
                self.ratio = new_limit / self.mem * 1.1
                logger.info('after rotate, rss = %d MB, enlarge soft memory limit %d -> %d MB, origin = %d MB',
                            rss >> 20,
                            limit >> 20,
                            self.mem_limit_soft >> 20,
                            self.mem >> 20)

    def _start(self):
        p = psutil.Process()

        logger.debug("start mem check thread")
        if hasattr(p, "memory_info"):
            self.mf = getattr(p, "memory_info")
        else:
            self.mf = getattr(p, 'get_memory_info')
        mf = self.mf

        def check_mem():
            while not self._stop:
                rss = self.rss = (mf().rss + self.addation)  # 1ms
                if self.check and rss > self.mem * 1.5:
                    self._kill(rss, from_main_thread=False)
                time.sleep(0.1)

        self.thread = t = threading.Thread(target=check_mem)
        t.daemon = True
        t.start()

    def start(self, task_id, mem_limit_mb):
        self._stop = False
        self.mem = int(mem_limit_mb) << 20
        self.task_id = task_id
        if not self.thread:
            self._start()
        self.thread.name = "task-%s-checkmem" % (task_id,)

    def stop(self):
        self._stop = True
        self.thread.join()
        self.thread = None


def set_oom_score(score=100):
    if platform.system() == 'Linux':
        pid = os.getpid()
        entry = "oom_score_adj"
        path = "/proc/{}/{}".format(pid, entry)

        try:
            with open(path, "w") as f:
                f.write("{}".format(score))
        except:
            pass

# util
from __future__ import absolute_import
import os
import re
import sys
import threading
import errno
import uuid
import time
import tempfile
import logging
import os.path
from contextlib import contextmanager
from zlib import compress as _compress
from dpark.crc32c import crc32c
from threading import Thread
import psutil
import resource

ERROR_TASK_OOM = 3

try:
    from dpark.portable_hash import portable_hash as _hash
except ImportError:
    import pyximport
    pyximport.install(inplace=True)
    from dpark.portable_hash import portable_hash as _hash

try:
    import pwd
    def getuser():
        return pwd.getpwuid(os.getuid()).pw_name
except:
    import getpass
    def getuser():
        return getpass.getuser()

COMPRESS = 'zlib'
def compress(s):
    return _compress(s, 1)

try:
    from dpark.lz4wrapper import compress, decompress
    COMPRESS = 'lz4'
except ImportError:
    try:
        from snappy import compress, decompress
        COMPRESS = 'snappy'
    except ImportError:
        pass

def spawn(target, *args, **kw):
    t = threading.Thread(target=target, name=target.__name__, args=args, kwargs=kw)
    t.daemon = True
    t.start()
    return t

# hash(None) is id(None), different from machines
# http://effbot.org/zone/python-hash.htm
def portable_hash(value):
    return _hash(value)

# similar to itertools.chain.from_iterable, but faster in PyPy
def chain(it):
    for v in it:
        for vv in v:
            yield vv

def izip(*its):
    its = [iter(it) for it in its]
    try:
        while True:
            yield tuple([next(it) for it in its])
    except StopIteration:
        pass

def mkdir_p(path):
    "like `mkdir -p`"
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def memory_str_to_mb(str):
    lower = str.lower()
    if lower[-1].isalpha():
        number, unit = float(lower[:-1]), lower[-1]
    else:
        number, unit = float(lower), 'm'
    scale_factors = {
        'k': 1. / 1024,
        'm': 1,
        'g': 1024,
        't': 1024 * 1024,
    }
    return number * scale_factors[unit]

MIN_REMAIN_RECURSION_LIMIT = 80
def recurion_limit_breaker(f):
    def _(*a, **kw):
        try:
            sys._getframe(sys.getrecursionlimit() - MIN_REMAIN_RECURSION_LIMIT)
        except ValueError:
            return f(*a, **kw)

        def __():
            result = []
            finished = []
            cond = threading.Condition(threading.Lock())
            def _run():
                it = iter(f(*a, **kw))
                with cond:
                    while True:
                        while result:
                            cond.wait()
                        try:
                            result.append(next(it))
                            cond.notify()
                        except StopIteration:
                            break

                    finished.append(1)
                    cond.notify()


            t = spawn(_run)

            with cond:
                while True:
                    while not finished and not result:
                        cond.wait()

                    if result:
                        yield result.pop()
                        cond.notify()

                    if finished:
                        assert not result
                        break

            t.join()

        return __()

    return _

class AbortFileReplacement(Exception):
    pass

@contextmanager
def atomic_file(filename, mode='w+b', bufsize=-1):
    path, name = os.path.split(filename)
    path = path or None
    prefix = '.%s.' % (name,) if name else '.'
    suffix = '.%s.tmp' % (uuid.uuid4().hex,)
    tempname = None
    try:
        try:
            mkdir_p(path)
        except (IOError, OSError):
            time.sleep(1) # there are dir cache in mfs for 1 sec
            mkdir_p(path)

        with tempfile.NamedTemporaryFile(
            mode=mode, suffix=suffix, prefix=prefix,
            dir=path, delete=False) as f:
            tempname = f.name
            yield f

        os.chmod(tempname, 0o644)
        os.rename(tempname, filename)
    except AbortFileReplacement:
        pass
    finally:
        try:
            if tempname:
                os.remove(tempname)
        except OSError:
            pass


RESET = "\033[0m"
BOLD = "\033[1m"
BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = [
    "\033[1;%dm" % i for i in range(30, 38)
]

PALLETE = {
    'RESET': RESET,
    'BOLD': BOLD,
    'BLACK': BLACK,
    'RED': RED,
    'GREEN': GREEN,
    'YELLOW': YELLOW,
    'BLUE': BLUE,
    'MAGENTA': MAGENTA,
    'CYAN': CYAN,
    'WHITE': WHITE,
}

COLORS = {
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE,
    'CRITICAL': YELLOW,
    'ERROR': RED
}

FORMAT_PATTERN = re.compile('|'.join('{%s}' % k for k in PALLETE))
def formatter_message(message, use_color = True):
    if use_color:
        return FORMAT_PATTERN.sub(
            lambda m: PALLETE[m.group(0)[1:-1]],
            message
        )

    return FORMAT_PATTERN.sub('', message)

class ColoredFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, use_color = True):
        if fmt:
            fmt = formatter_message(fmt, use_color)

        logging.Formatter.__init__(self, fmt=fmt, datefmt=datefmt)
        self.use_color = use_color

    def format(self, record):
        record = logging.makeLogRecord(record.__dict__)
        levelname = record.levelname
        if self.use_color and levelname in COLORS:
            levelname_color = COLORS[levelname] + levelname + RESET
            record.levelname = levelname_color

        record.msg = formatter_message(record.msg, self.use_color)
        return logging.Formatter.format(self, record)

USE_UTF8 = getattr(sys.stderr, 'encoding', None) == 'UTF-8'

ASCII_BAR = ('[ ', ' ]', '#', '-', '-\\|/-\\|')
UNICODE_BAR = (u'[ ', u' ]', u'\u2589', u'-',
    u'-\u258F\u258E\u258D\u258C\u258B\u258A')

def make_progress_bar(ratio, size=14):
    if USE_UTF8:
        L, R, B, E, F = UNICODE_BAR
    else:
        L, R, B, E, F = ASCII_BAR

    if size > 4:
        n = size - 4
        with_border = True
    else:
        n = size
        with_border = False

    p = n * ratio
    blocks = int(p)
    if p > blocks:
        frac = int((p - blocks) * 7)
        blanks = n - blocks - 1
        C = F[frac]
    else:
        blanks = n - blocks
        C = ''

    if with_border:
        return ''.join([L, B * blocks, C, E * blanks, R])
    else:
        return ''.join([B * blocks, C, E * blanks])

def init_dpark_logger(log_level, use_color=None):
    log_format = '{GREEN}%(asctime)-15s{RESET}' \
            ' [%(levelname)s] [%(threadName)s] [%(name)-9s:%(lineno)d] %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    logger = get_logger('dpark')
    logger.propagate = False

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(ColoredFormatter(log_format, datefmt, use_color))

    logger.addHandler(handler)
    logger.setLevel(max(log_level, logger.level))


def get_logger(name):
    """ Always use logging.Logger class.

    The user code may change the loggerClass (e.g. pyinotify),
    and will cause exception when format log message.
    """
    old_class = logging.getLoggerClass()
    logging.setLoggerClass(logging.Logger)
    logger = logging.getLogger(name)
    logging.setLoggerClass(old_class)
    return logger


def masked_crc32c(s):
    crc = crc32c(s)
    return (((crc >> 15) | (crc << 17)) + 0xa282ead8) & 0xffffffff
logger = get_logger(__name__)


src_dir = os.path.dirname(os.path.abspath(__file__))
STACK_FILE_NAME = 0
STACK_LINE_NUM = 1
STACK_FUNC_NAME = 2


def get_user_call_site():
    import traceback
    stack = traceback.extract_stack(sys._getframe())
    for i in range(1, len(stack)):
        callee_path = stack[i][STACK_FILE_NAME]
        if src_dir == os.path.dirname(os.path.abspath(callee_path)):
            caller_path = stack[i-1][STACK_FILE_NAME]
            caller_lineno = stack[i-1][STACK_LINE_NUM]
            dpark_func_name = stack[i][STACK_FUNC_NAME]
            user_call_site = '%s:%d ' % (caller_path, caller_lineno)
            return dpark_func_name, user_call_site
    return "<func>", " <root>"


class Scope(object):
    def __init__(self):
        fn, pos = get_user_call_site()
        self.dpark_func_name = fn
        self.call_site = "@".join([fn, pos])


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
        return (self.mf().rss + self.addation)

    @classmethod
    def maxrss(cls):
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024

    def _kill(self, rss, from_main_thread):
        tmpl = "task used too much memory: %dMB > %dMB * 1.5," \
                "kill it. use -M argument or taskMemory " \
                "to request more memory."
        msg = tmpl % (rss >> 20, self.mem >> 20)

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
        self.thread.name = "task-%s-checkmem" % (task_id, )

    def stop(self):
        self._stop = True
        self.thread.join()
        self.thread = None


def profile(func):
    import functools

    @functools.wraps(func)
    def inner(*args, **kwargs):
        import cProfile
        import pstats
        profiler = cProfile.Profile()
        profiler.enable()
        try:
            retval = func(*args, **kwargs)
        finally:
            profiler.disable()

        stats = pstats.Stats(profiler)
        stats.strip_dirs()
        stats.sort_stats('time', 'calls')
        stats.print_stats(20)
        stats.sort_stats('cumulative')
        stats.print_stats(20)
        return retval
    return inner

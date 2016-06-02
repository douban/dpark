# util
import os
import sys
import threading
import errno
import uuid
import time
import tempfile
import logging
from contextlib import contextmanager
from zlib import compress as _compress, decompress
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
    from lz4 import compress, decompress
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
            yield tuple([it.next() for it in its])
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

MIN_REMAIN_RECURSION_LIMIT = 100
def recurion_limit_breaker(f):
    def _(*a, **kw):
        depth = 0
        frame = sys._getframe()
        while frame is not None:
            frame = frame.f_back
            depth += 1

        if depth < sys.getrecursionlimit() - MIN_REMAIN_RECURSION_LIMIT:
            result = f(*a, **kw)
        else:
            result = []
            def _run():
                for r in f(*a, **kw):
                    result.append(r)

            spawn(_run).join()

        for r in result:
            yield r

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
            dir=path, delete=False, bufsize=bufsize) as f:
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


def init_dpark_logger(log_level):
    log_format = '%(asctime)-15s [%(levelname)s] [%(name)-9s] %(message)s'
    logging.basicConfig(format=log_format, level=log_level)

    logger = get_logger('dpark')
    logger.propagate = False

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(log_format))

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


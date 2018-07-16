# util
from __future__ import absolute_import
import os
import sys
import threading
import errno
import uuid
import time
import tempfile
import os.path
from contextlib import contextmanager
from zlib import compress as _compress
from dpark.crc32c import crc32c


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


def masked_crc32c(s):
    crc = crc32c(s)
    return (((crc >> 15) | (crc << 17)) + 0xa282ead8) & 0xffffffff


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

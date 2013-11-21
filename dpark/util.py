# util 
import types
from zlib import compress as _compress, decompress
import threading
import warnings
try:
    from dpark.portable_hash import portable_hash as _hash
except ImportError:
    import pyximport
    pyximport.install(inplace=True)
    from dpark.portable_hash import portable_hash as _hash

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

try:
    from setproctitle import getproctitle, setproctitle
except ImportError:
    def getproctitle():
        return ''
    def setproctitle(x):
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

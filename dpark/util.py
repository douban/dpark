# util 
import types
from zlib import compress as _compress, decompress
import threading

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

def ilen(x):
    try:
        return len(x)
    except TypeError:
        return sum(1 for i in x)

def spawn(target, *args, **kw):
    t = threading.Thread(target=target, name=target.__name__, args=args, kwargs=kw)
    t.daemon = True
    t.start()
    return t

# http://effbot.org/zone/python-hash.htm
def portable_hash(value):
    if value is None:
        return 0
    if type(value) is types.TupleType:
        h = 0x345678
        for i in value:
            h = ((1000003 * h) & 0xffffffff) ^ portable_hash(i)
        return h
    return hash(value)

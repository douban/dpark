# util 
from zlib import compress as _compress, decompress

def compress(s):
    return _compress(s, 1)
try:
    from lz4 import compress, decompress
except ImportError:    
    print 'no lz4 available'
    #try:
    #    from snappy import compress, decompress
    #except ImportError:
    #    pass

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

from cStringIO import StringIO

from consts import *
from master import MasterConn
from cs import *

MFS_ROOT_INODE = 1

class MooseFS:
    def __init__(self, host='mfsmaster', port=9421):
        self.mc = MasterConn(host, port)
        self.inode_cache = {}

    def _lookup(self, parent, name):
        cache = self.inode_cache.setdefault(parent, {})
        info = cache.get(name)
        if info is not None:
            return info
        
        info, err = self.mc.lookup(parent, name)
        if info is not None:
            cache[name] = info
        return info

    def lookup(self, path, followSymlink=True):
        parent = MFS_ROOT_INODE
        info = None
        ps = path.split('/')
        for i,n in enumerate(ps):
            if not n: continue
            info = self._lookup(parent, n)
            if not info:
                raise Exception("not found")
            if info.is_symlink() and followSymlink:
                target = self.mc.read_link(info.inode)
                if not target.startswith('/'):
                    target = os.path.join('/'.join(ps[:i]),
                        target)
                return self.lookup(target, True)
            parent = info.inode
        if info is None:
            info = self.mc.getattr(parent)
        return info

    def open(self, path):
        info = self.lookup(path)
        return File(info.inode, path, info)

    def close(self):
        self.mc.close()


class File:
    def __init__(self, inode, path, info):
        self.inode = inode
        self.path = path
        self.info = info
        self.cscache = {}
        self.roff = 0
        self.rbuf = ''
        self.reader = None
        self.generator = StringIO("")

    @property
    def length(self):
        return self.info.length

    def __len__(self):
        return (self.length + CHUNKSIZE - 1) / self.length

    def get_chunk(self, i):
        chunk = self.cscache.get(index)
        if not chunk:
            chunk = mfs.mc.readchunk(self.inode, index)
            self.cscache[index] = chunk
        return chunk

    def locs(self):
        return []
        return [[host for host,_ in self.get_chunk(i).addrs]
                 for i in range(len(self))]

    def seek(self, offset):
        off = offset - self.roff
        if off > 0 and off < len(self.rbuf):
            self.rbuf = self.rbuf[off:]
        else:
            self.rbuf = ''
        self.roff = offset
        self.reader = None
        self.generator = StringIO("")

    def read(self, n):
        buf = []
        while n > 0:
            nbuf = len(self.rbuf)
            if nbuf >= n:
                buf.append(self.rbuf[:n])
                self.rbuf = self.rbuf[n:]
                self.roff += n
                break

            if nbuf > 0:
                buf.append(self.rbuf)
                n -= nbuf
                self.rbuf = ''
                self.roff += nbuf

            self.fill_buffer()
            if not self.rbuf:
                break

        return ''.join(buf)

    def fill_buffer(self):
        if self.reader is None:
            if self.roff < self.length:
                self.reader = self.chunk_reader(self.roff)
            else:
                return
        try:
            buf = self.reader.next()
            if self.rbuf:
                self.rbuf += buf
            else:
                self.rbuf = buf
        except StopIteration:
            self.reader = None

    def chunk_reader(self, roff):
        index = roff / CHUNKSIZE
        offset = roff % CHUNKSIZE
        chunk = self.get_chunk(index)
        if offset > chunk.length:
            return
        for host, port in chunk.addrs:
            try:
                for block in read_chunk(host, port, chunk.id,
                    chunk.version, chunk.length-offset, offset):
                    yield block
                    offset += len(block)
                    if offset == self.length:
                        return
            except Exception:
                raise
  
    def __iter__(self):
        return self

    def next(self):
        try:
            line = self.generator.next()
        except StopIteration:
            self.generator = None
            line = ""

        while not line or line[-1] != '\n':
            data = self.read(1024*1024*4)
            if not data:
                if line:
                    return line
                else:
                    raise StopIteration
            self.generator = StringIO(data)
            line += self.generator.next()
        return line

    def close(self):
        self.roff = 0
        self.rbuf = None

mfs = MooseFS()

def init(host, port=9421):
    global mfs 
    if mfs is not None:
        mfs.close()
    mfs = MooseFS(host, port)

def mfsopen(path):
    return mfs.open(path)

def _test():
    init("localhost")
    f = open('/mfs2/test.csv')
    f.seek(1024)
    d = f.read(1024)
    
    f2 = mfsopen('/test.csv')
    f2.seek(1024)
    d2 = f2.read(1024)
    assert d == d2

    f.seek(0)
    f2.seek(0)
    import csv
    for row in csv.reader(f2):
        pass

if __name__ == '__main__':
    _test()

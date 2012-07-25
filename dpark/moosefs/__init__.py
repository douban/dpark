import os
from cStringIO import StringIO

from consts import *
from master import MasterConn
from cs import read_chunk, read_chunk_from_local

MFS_ROOT_INODE = 1

class MooseFS(object):
    def __init__(self, host='mfsmaster', port=9421):
        self.host = host
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
        for i, n in enumerate(ps):
            if not n: continue
            info = self._lookup(parent, n)
            if not info:
                return
            if info.is_symlink() and followSymlink:
                target = self.mc.readlink(info.inode)
                if not target.startswith('/'):
                    target = os.path.join('/'.join(ps[:i]),
                        target)
                return self.lookup(target, True)
            parent = info.inode
        if info is None and parent == MFS_ROOT_INODE:
            info = self.mc.getattr(parent)
        return info

    def open(self, path):
        info = self.lookup(path)
        if not info:
            raise Exception("not found")
        return File(info.inode, path, info, self.host)

    def listdir(self, path):
        info = self.lookup(path)
        if not info:
            raise Exception("not found")
        files = self.mc.getdirplus(info.inode)
        for i in files.values():
            self.inode_cache.setdefault(info.inode, {})[i.name] = i
        return files

    def close(self):
        self.mc.close()


class File(object):
    def __init__(self, inode, path, info, master):
        self.inode = inode
        self.path = path
        self.info = info
        self.length = info.length
        self.master = master
        self.cscache = {}

    def __len__(self):
        return (self.length + CHUNKSIZE - 1) / CHUNKSIZE

    def get_chunk(self, i):
        chunk = self.cscache.get(i)
        if not chunk:
            chunk = get_mfs(self.master).mc.readchunk(self.inode, i)
            self.cscache[i] = chunk
        return chunk

    def locs(self, i=None):
        if i is None:
            return [[host for host, _ in self.get_chunk(i).addrs]
                     for i in range(len(self))]
        return [host for host, _ in self.get_chunk(i).addrs]


class ReadableFile(File):
    def __init__(self, f):
        self.__dict__.update(f.__dict__)
        self.roff = 0
        self.rbuf = ''
        self.reader = None
        self.generator = None

    def seek(self, offset, length=0):
        off = offset - self.roff
        if off > 0 and off < len(self.rbuf):
            self.rbuf = self.rbuf[off:]
        else:
            self.rbuf = ''
        self.roff = offset
        self.reader = None
        self.generator = None
        if length > 0 and length < self.length:
            self.length = length

    def read(self, n):
        if n == -1:
            if not self.rbuf:
                self.fill_buffer()
            v = self.rbuf
            self.roff += len(v)
            self.rbuf = ''
            return v

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
            self.rbuf = self.reader.next()
        except StopIteration:
            self.reader = None
            self.fill_buffer()

    def chunk_reader(self, roff):
        index = roff / CHUNKSIZE
        offset = roff % CHUNKSIZE
        chunk = self.get_chunk(index)
        length = min(self.length - index * CHUNKSIZE, CHUNKSIZE)
        if offset > length:
            return
            
        for block in read_chunk_from_local(chunk.id,
                chunk.version, length-offset, offset):
            yield block
            offset += len(block)
            if offset >= length:
                return

        for host, port in chunk.addrs:
            # give up after two continuous errors
            nerror = 0
            while nerror < 2:
                try:
                    for block in read_chunk(host, port, chunk.id,
                        chunk.version, length-offset, offset):
                        yield block
                        offset += len(block)
                        if offset >= length:
                            return
                        nerror = 0
                    break
                except IOError, e:
                    #print 'read chunk error from ', host, port, chunk.id, chunk.version, offset, e
                    nerror += 1

        raise Exception("unexpected error: %d %d %s < %s" % (roff, index, offset, length))
        
    def __iter__(self):
        return self

    def next(self):
        if self.generator is not None:
            try:
                line = self.generator.next()
            except StopIteration:
                self.generator = None
                line = ""
        else:
            line = ""

        while not line or line[-1] != '\n':
            data = self.read(-1)
            if not data:
                break
            self.generator = StringIO(data)
            line += self.generator.next()
        return line

    def close(self):
        self.roff = 0
        self.rbuf = ''
        self.reader = None
        self.generator = None

_mfs = {}

def get_mfs(master):
    if master in _mfs:
        return _mfs[master]
    _mfs[master] = MooseFS(master)
    return _mfs[master]

def mfsopen(path, master='mfsmaster'):
    return get_mfs(master).open(path)

def listdir(path, master='mfsmaster'):
    return get_mfs(master).listdir(path)

def walk(path, master='mfsmaster'):
    ds = [path]
    while ds:
        root = ds.pop()
        cs = listdir(root, master)
        dirs = [name for name, info in cs.iteritems() 
                if info.type == TYPE_DIRECTORY
                    and name not in '..']
        files = [i.name for i in cs.values() if i.type == TYPE_FILE]
        yield root, dirs, files
        for d in dirs:
            ds.append(os.path.join(root, d))

def _test():
    f = open('/mfs2/test.csv')
    f.seek(1024)
    d = f.read(1024)
    
    f2 = mfsopen('/test.csv')
    print 'f2 locs', f2.locs()
    f2.seek(1024)
    d2 = f2.read(1024)
    assert d == d2

    f.seek(0)
    f2.seek(0)
    import csv
    for _ in csv.reader(f2):
        break

    #print listdir('/')
    for root, dirs, names in walk('/'):
        print root, dirs, names
        for n in names:
            print n, mfsopen(os.path.join(root, n)).locs()

if __name__ == '__main__':
    _test()

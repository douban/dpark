import os
import socket
from cStringIO import StringIO
import logging

from consts import *
from master import MasterConn
from cs import read_chunk, read_chunk_from_local

MFS_ROOT_INODE = 1

logger = logging.getLogger(__name__)

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
        for i in files.itervalues():
            self.inode_cache.setdefault(info.inode, {})[i.name] = i
        return files
    
    def walk(self, path, followlinks=False):
        ds = [path]
        while ds:
            root = ds.pop()
            cs = self.listdir(root)
            dirs, files = [], []
            for name, info in cs.iteritems():
                if name in '..': continue
                if info.type == TYPE_DIRECTORY:
                    dirs.append(name)
                elif info.type == TYPE_FILE:
                    files.append(name)
                elif followlinks and info.type == TYPE_SYMLINK:
                    target = os.path.join(root, self.mc.readlink(info.inode))
                    info = self.lookup(target)
                    if info:
                        if info.type == TYPE_DIRECTORY:
                            dirs.append(name)
                        elif info.type == TYPE_FILE:
                            files.append(name)
                        else:
                            pass # TODO: symlink to symlink
            yield root, dirs, files
            for d in sorted(dirs, reverse=True):
                ds.append(os.path.join(root, d))


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

    def seek(self, offset, whence=0):
        if whence == 1:
            offset = self.roff + offset
        elif whence == 2:
            offset = self.length + offset
        assert offset >= 0, 'offset should greater than 0'

        off = offset - self.roff
        if off > 0 and off < len(self.rbuf):
            self.rbuf = self.rbuf[off:]
        else:
            self.rbuf = ''
            self.reader = None
        
        self.roff = offset
        self.generator = None

    def tell(self):
        return self.roff

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
        
        local_ip = socket.gethostbyname(socket.gethostname())
        if any(ip == local_ip for ip,port in chunk.addrs):
            try:
                for block in read_chunk_from_local(chunk.id,
                        chunk.version, length-offset, offset):
                    yield block
                    offset += len(block)
                    if offset >= length:
                        return
            except Exception, e:
                logger.warning("read chunk %d from local: %s", chunk.id, e)

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
        # TODO: speedup
        line = ""
        while True:
            data = self.read(-1)
            if not data:
                break
            generator = StringIO(data)
            assert '\n' not in line, line
            line += generator.next()
            if line.endswith('\n'):
                yield line
                line = ''

                ll = list(generator)
                if not ll: continue

                for line in ll[:-1]:
                    yield line
                line = ll[-1]    
                if line.endswith('\n'):
                    yield line
                    line = ''

        if line:
            yield line

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

def walk(path, master='mfsmaster', followlinks=False):
    return get_mfs(master).walk(path, followlinks)

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

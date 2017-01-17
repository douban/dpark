import os
import socket
from cStringIO import StringIO

from consts import *
from master import MasterConn
from cs import read_chunk, read_chunk_from_local

from dpark.util import get_logger

MFS_ROOT_INODE = 1

logger = get_logger(__name__)


class CrossSystemSymlink(Exception):
    def __init__(self, src, dst):
        self.src = src
        self.dst = dst

    def __str__(self):
        return '%s -> %s' % (self.src, self.dst)


class MooseFS(object):
    def __init__(self, host='mfsmaster', port=9421, mountpoint='/mfs'):
        self.host = host
        self.mountpoint = mountpoint
        self.mc = MasterConn(host, port)
        self.symlink_cache = {}

    def _lookup(self, parent, name):
        return self.mc.lookup(parent, name)[0]

    def readlink(self, inode):
        target = self.symlink_cache.get(inode)
        if target is None:
            target = self.mc.readlink(inode)
            self.symlink_cache[inode] = target
        return target

    def lookup(self, path, followSymlink=True):
        parent = MFS_ROOT_INODE
        info = None
        ps = path.split('/')
        for i, n in enumerate(ps):
            if not n:
                continue
            info = self._lookup(parent, n)
            if not info:
                return
            while info.is_symlink() and followSymlink:
                target = self.readlink(info.inode)
                if not target.startswith('/'):
                    target = os.path.join('/'.join(ps[:i]), target)
                    info = self.lookup(target, followSymlink)
                elif target.startswith(self.mountpoint):
                    info = self.lookup(target[len(self.mountpoint):], followSymlink)
                else:
                    raise CrossSystemSymlink(path, os.path.join(target, *ps[i+1:]))
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
        return self.mc.getdirplus(info.inode)

    def walk(self, path, followlinks=False):
        ds = [path]
        while ds:
            root = ds.pop()
            cs = self.listdir(root)
            dirs, files = [], []
            for name, info in cs.iteritems():
                if name in '..':
                    continue
                while followlinks and info and info.type == TYPE_SYMLINK:
                    target = self.readlink(info.inode)
                    if target.startswith('/'):
                        if not target.startswith(self.mountpoint):
                            if os.path.exists(target):
                                if os.path.isdir(target):
                                    dirs.append(target)
                                else:
                                    files.append(target)
                            info = None  # ignore broken symlink
                            break
                        else:
                            target = target[len(self.mountpoint):]
                            # use relative path for internal symlinks
                            name = ('../' * len(filter(None, root.split('/')))) + target
                    else:
                        name = target
                        target = os.path.join(root, target)
                    info = self.lookup(target)

                if info:
                    if info.type == TYPE_DIRECTORY:
                        if name not in dirs:
                            dirs.append(name)
                    elif info.type == TYPE_FILE:
                        if name not in files:
                            files.append(name)

            yield root, dirs, files
            for d in sorted(dirs, reverse=True):
                if not d.startswith('/'):  # skip external links
                    ds.append(os.path.join(root, d))

    def close(self):
        self.mc.terminate()


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
            n = (self.length - 1) / CHUNKSIZE + 1
            return [[host for host, _ in self.get_chunk(i).addrs]
                    for i in range(n)]
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
        if any(ip == local_ip for ip, port in chunk.addrs):
            try:
                if _local_aware:
                    for block in read_chunk_from_local(chunk.id,
                                                       chunk.version,
                                                       length-offset,
                                                       offset):
                        yield block
                        offset += len(block)
                        if offset >= length:
                            return
                for i in range(len(chunk.addrs)):
                    ip, port = chunk.addrs[i]
                    if ip == local_ip:
                        if i != 0:
                            chunk.addrs[0], chunk.addrs[i] = chunk.addrs[i], chunk.addrs[0]
                        break
            except Exception, e:
                logger.warning("fail to read chunk %d of %s from local, \
                                exception: %s",
                               chunk.id, self.path, e)

        last_exception = None
        last_host = None
        for host, port in chunk.addrs:
            # give up after two continuous errors
            nerror = 0
            while nerror < 2:
                try:
                    for block in read_chunk(host, port, chunk.id,
                                            chunk.version,
                                            length-offset,
                                            offset):
                        yield block
                        offset += len(block)
                        if offset >= length:
                            return
                        nerror = 0
                    break
                except IOError, e:
                    last_exception = e
                    last_host = host
                    logger.debug("fail to read chunk %d of %s from %s, \
                                    exception: %s",
                                 chunk.id, self.path, host, e)
                    nerror += 1

        raise Exception("unexpected error[%s], \
                        start_offset=%d, chunk=%d, \
                        curr_offset_in_chunk=%d < length=%d: \
                        last exception on %s: %s" %
                        (self.path,
                         roff, index,
                         offset, length,
                         last_host, last_exception))

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
                if not ll:
                    continue

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

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

_mfs = {}

MFS_PREFIX = {
    }

_local_aware = True
if 'CONTAINER_INFO' in os.environ:
    _local_aware = False


def get_mfs(master, mountpoint=''):
    if master in _mfs:
        return _mfs[master]
    _mfs[master] = MooseFS(master, mountpoint=mountpoint)
    return _mfs[master]


def close_mfs():
    for master in _mfs:
        logger.debug('close the fs:%s at dir:%s' % (master,
                                                    _mfs[master].mountpoint))
        _mfs[master].close()
    _mfs.clear()


def mfsopen(path, master='mfsmaster'):
    return get_mfs(master).open(path)


def listdir(path, master='mfsmaster'):
    return get_mfs(master).listdir(path)


def get_mfs_by_path(path):
    for prefix, master in MFS_PREFIX.iteritems():
        if path.startswith(prefix):
            return get_mfs(master, prefix)


def add_prefix(gen, prefix):
    for root, dirs, names in gen:
        yield prefix + root, dirs, names
        for d in dirs:
            if d.startswith('/'):
                for root, dd, ns in walk(d, True):
                    yield root, dd, ns


def walk(path, followlinks=False):
    path = os.path.realpath(path)
    mfs = get_mfs_by_path(path)
    if mfs:
        rs = mfs.walk(path[len(mfs.mountpoint):], followlinks)
        return add_prefix(rs, mfs.mountpoint)
    else:
        return os.walk(path, followlinks=followlinks)


def open_file(path):
    mfs = get_mfs_by_path(path)
    if mfs:
        try:
            return mfs.open(path[len(mfs.mountpoint):])
        except CrossSystemSymlink, e:
            return open_file(e.dst)


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

    # print listdir('/')
    for root, dirs, names in walk('/'):
        print root, dirs, names
        for n in names:
            print n, mfsopen(os.path.join(root, n)).locs()

if __name__ == '__main__':
    _test()

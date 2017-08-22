from __future__ import absolute_import
import os
import stat
import errno
import socket
import threading
from .utils import FileInfo, read_chunk
from .consts import *
from .mfs_proxy import ProxyConn
from dpark.util import get_logger
import six
from six.moves import range
try:
    from cStringIO import StringIO
except ImportError:
    from six import BytesIO as StringIO

logger = get_logger(__name__)


class FileSystem(object):

    def readlink(self, path):
        raise NotImplementedError

    def open_file(self, path):
        raise NotImplementedError

    def listdir(self, path):
        raise NotImplementedError

    def walk(self, path, followlinks=True):
        raise NotImplementedError

    def check_ok(self, path):
        raise NotImplementedError


class PosixFS(object):

    def readlink(self, path):
        return os.readlink(path)

    def open_file(self, path):
        return PosixFile(path)

    def listdir(self, path):
        return os.listdir(path)

    def walk(self, path, followlinks):
        return os.walk(path, followlinks=followlinks)

    def check_ok(self, path):
        return True


class MooseFS(PosixFS):

    def __init__(self):
        self._local = threading.local()
        self._local.proxy_map = {}
        self.proxy_map = self._local.proxy_map

    def _find_proxy(self, path):
        for mountpoint in self.proxy_map:
            if mountpoint in path:
                return self.proxy_map[mountpoint]
        dir_path = path if os.path.isdir(path) else os.path.dirname(path)
        mount = ''
        while os.path.exists(os.path.join(dir_path, '.masterinfo')):
            mount = dir_path
            dir_path = os.path.dirname(dir_path)
        if mount:
            host, port, version = ProxyConn.get_masterinfo(os.path.join(mount, '.masterinfo'))
            self.proxy_map[mount] = ProxyConn(host, port, version)
            return self.proxy_map[mount]

    def _get_indeed_root(self, root):
        root = os.path.realpath(root)
        proxy = self._find_proxy(root)
        try:
            st = os.lstat(root)
            return root, proxy, st.st_ino
        except OSError as e:
            if e.errno == errno.ENOENT:
                return None, proxy, None
            raise e

    def walk(self, path, followlinks=False):
        ds = [path]
        while ds:
            root = ds.pop()
            real_root, proxy, inode = self._get_indeed_root(root)
            if not real_root:
                logger.warning('path not exists: %s', root)
                continue
            if not proxy and followlinks:
                logger.warning('the path to walk is symlink to local: %s', root)
                for sub_root, dirs, names in os.walk(real_root, followlinks=followlinks):
                    rel_path = os.path.relpath(sub_root, real_root)
                    rel_dir = rel_path if not rel_path.startswith('.') else ''
                    yield os.path.join(root, rel_dir), dirs, names
                continue
            cs = proxy.getdirplus(inode)
            dirs, files = [], []
            for name, info in six.iteritems(cs):
                if name in '..':
                    continue
                if info.ftype == TYPE_DIRECTORY:
                    dirs.append(name)
                elif info.ftype == TYPE_FILE:
                    files.append(name)
                else:
                    if os.path.isdir(os.path.join(root, name)):
                        dirs.append(name)
                    elif os.path.exists(os.path.join(path, name)):
                        files.append(name)

            yield root, dirs, files
            for d in sorted(dirs, reverse=True):
                if not d.startswith('/'):
                    if not followlinks and os.path.islink(os.path.join(root, d)):
                        continue
                    ds.append(os.path.join(root, d))

    def check_ok(self, path):
        if os.path.isdir(path):
            return os.path.exists(os.path.join(path, '.masterinfo'))
        else:
            return os.path.exists(os.path.join(os.path.dirname(path),
                                               '.masterinfo'))

    def open_file(self, path):
        return MooseFile(path, self)


class ReadableFile(object):
    def __init__(self, path):
        self.path = path
        self.inode = None
        self.info = None
        self.length = 0

    def locs(self, i=None):
        raise NotImplementedError

    def seek(self, offset, whence=0):
        raise NotImplementedError

    def tell(self):
        raise NotImplementedError

    def read(self, n):
        raise NotImplementedError

    def __iter__(self):
        line = b''
        while True:
            data = self.read(-1)
            if not data:
                break
            generator = StringIO(data)
            assert b'\n' not in line, line
            line += next(generator)
            if line.endswith(b'\n'):
                yield line
                line = b''
                ll = list(generator)
                if not ll:
                    continue

                for line in ll[:-1]:
                    yield line
                line = ll[-1]
                if line.endswith(b'\n'):
                    yield line
                    line = b''
        if line:
            yield line

    def close(self):
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class PosixFile(ReadableFile):
    def __init__(self, path):
        ReadableFile.__init__(self, path)
        st = os.lstat(path)
        self.inode = st.st_ino
        self.length = st.st_size
        self.info = FileInfo(st.st_ino, path, self._get_type(st.st_mode), st.st_mode,
                             st.st_uid, st.st_gid, st.st_atime, st.st_mtime,
                             st.st_ctime, st.st_nlink, st.st_size)
        self.fp = open(self.path, 'rb', 4096 * 1024)

    def _get_type(self, st_mode):
        if stat.S_ISDIR(st_mode):
            return 2
        elif stat.S_ISLNK(st_mode):
            return 3
        elif stat.S_ISREG(st_mode):
            return 1

    def locs(self, i=None):
        return []

    def seek(self, offset, whence=0):
        self.fp.seek(offset, whence)

    def tell(self):
        return self.fp.tell()

    def read(self, n):
        return self.fp.read(n)

    def close(self):
        self.info = None
        self.fp.close()


class MooseFile(ReadableFile):
    def __init__(self, path, fs):
        ReadableFile.__init__(self, path)
        self.fs = fs
        self._load(path)

    def _load(self, path):
        proxy = self.fs._find_proxy(path)
        st = os.lstat(path)
        self.inode = st.st_ino
        self.info = proxy.getattr(self.inode)
        self.length = self.info.length
        self.cscache = {}
        self.roff = 0
        self.rbuf = b''
        self.reader = None
        self.generator = None

    def get_chunk(self, i):
        chunk = self.cscache.get(i)
        if not chunk:
            proxy = self.fs._find_proxy(self.path)
            chunk = proxy.readchunk(self.inode, i)
            self.cscache[i] = chunk
        return chunk

    def __getstate__(self):
        return self.path

    def __setstate__(self, state):
        from . import file_manager
        self.path = path = state
        self.fs = file_manager._get_fs(path)
        self._load(path)

    def locs(self, i=None):
        if i is None:
            n = (self.length - 1) // CHUNKSIZE + 1
            return [[host for host, _ in self.get_chunk(i).addrs]
                    for i in range(n)]
        return [host for host, _ in self.get_chunk(i).addrs]

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
            self.rbuf = b''
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
            self.rbuf = b''
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
                self.rbuf = b''
                self.roff += nbuf

            self.fill_buffer()
            if not self.rbuf:
                break
        return b''.join(buf)

    def fill_buffer(self):
        if self.reader is None:
            if self.roff < self.length:
                self.reader = self.chunk_reader(self.roff)
            else:
                return
        try:
            self.rbuf = next(self.reader)
        except StopIteration:
            self.reader = None
            self.fill_buffer()

    def chunk_reader(self, roff):
        index = roff // CHUNKSIZE
        offset = roff % CHUNKSIZE
        chunk = self.get_chunk(index)
        length = min(self.length - index * CHUNKSIZE, CHUNKSIZE)
        if offset > length:
            return

        local_ip = socket.gethostbyname(socket.gethostname())
        for i in range(len(chunk.addrs)):
            ip, port = chunk.addrs[i]
            if ip == local_ip:
                if i != 0:
                    chunk.addrs[0], chunk.addrs[i] = chunk.addrs[i], chunk.addrs[0]
                break

        last_exception = None
        last_host = None
        for host, port in chunk.addrs:
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
                except IOError as e:
                    last_exception = e
                    last_host = host
                    logger.debug("fail to read chunk %d of %s from %s, \
                                    exception: %s",
                                 chunk.id, self.path, host, e)
                    nerror += 1

        raise Exception("unexpected path=%s, addrs=%s, "
                        "start_offset=%d, chunk=%d, "
                        "curr_offset_in_chunk=%d < length = %d, "
                        "last exception on host %s: %s" %
                        (self.path,
                         chunk.addrs,
                         roff, index,
                         offset, length,
                         last_host, last_exception))

    def close(self):
        self.roff = 0
        self.rbuf = b''
        self.reader = None
        self.generator = None

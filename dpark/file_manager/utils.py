from __future__ import absolute_import
import socket
import struct

from .consts import *
import six


def uint8(n):
    return struct.pack("B", n)


def uint64(n):
    return struct.pack("!Q", n)


class Error(Exception):
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return strerror(self.code)


def pack(cmd, *args):
    msg = []
    for a in args:
        if isinstance(a, six.integer_types):
            msg.append(struct.pack("!I", a))
        elif isinstance(a, bytes):
            msg.append(a)
        else:
            raise TypeError(str(type(a))+str(a))
    header = struct.pack("!II", cmd, sum(len(i) for i in msg))
    return header + b''.join(msg)


def unpack(fmt, buf):
    if not fmt.startswith("!"):
        fmt = "!" + fmt
    return struct.unpack(fmt, buf[:struct.calcsize(fmt)])


class FileInfo:
    def __init__(self, inode, name, ftype, mode, uid, gid,
                 atime, mtime, ctime, nlink, length):
        self.inode = inode
        self.name = name
        self.ftype = self._get_ftype(ftype)
        if ftype == TYPE_DIRECTORY:
            mode |= S_IFDIR
        elif ftype == TYPE_SYMLINK:
            mode |= S_IFLNK
        elif ftype == TYPE_FILE:
            mode |= S_IFREG
        self.mode = mode
        self.uid = uid
        self.gid = gid
        self.atime = atime
        self.mtime = mtime
        self.ctime = ctime
        self.nlink = nlink
        self.length = length
        self.blocks = (length + 511) / 512

    def __repr__(self):
        return ("FileInfo(%s, inode=%d, type=%s, length=%d)" %
                (self.name, self.inode, self.ftype, self.length))

    def is_symlink(self):
        return self.ftype == TYPE_SYMLINK

    def _get_ftype(self, ftype):
        ftype_str_map = {1: TYPE_FILE, 2: TYPE_DIRECTORY, 3: TYPE_SYMLINK,
                         4: TYPE_FIFO, 5: TYPE_BLOCKDEV, 6: TYPE_CHARDEV,
                         7: TYPE_SOCKET, 8: TYPE_TRASH, 9: TYPE_SUSTAINED}
        if ftype in ftype_str_map:
            return ftype_str_map[ftype]
        return '?'


def attrToFileInfo(inode, attrs, name='', version=(0, 0, 0)):
    if len(attrs) != 35:
        raise Exception('bad length')
    if version < (1, 7, 32):
        return FileInfo(inode, name, *struct.unpack("!BHIIIIIIQ", attrs))
    else:
        return _unpack_to_file_info(inode, attrs, name)


def _unpack_to_file_info(inode, attrs, name):
    tup = struct.unpack("!BHIIIIIIQ", attrs)
    type_mode = tup[1]
    ftype = (type_mode & 0xf000) >> 12
    mode = type_mode & 0x0fff
    return FileInfo(inode, name, ftype, mode, *(tup[2:]))


def read_chunk(host, port, chunkid, version, size, offset=0):
    if offset + size > CHUNKSIZE:
        raise ValueError("size too large %s > %s" %
                         (size, CHUNKSIZE-offset))

    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.settimeout(10)
    conn.connect((host, port))

    msg = pack(CLTOCS_READ, uint64(chunkid), version, offset, size)
    n = conn.send(msg)
    while n < len(msg):
        if not n:
            raise IOError("write failed")
        msg = msg[n:]
        n = conn.send(msg)

    def recv(n):
        d = conn.recv(n)
        while len(d) < n:
            nd = conn.recv(n-len(d))
            if not nd:
                raise IOError("not enough data")
            d += nd
        return d

    while size > 0:
        cmd, l = unpack("II", recv(8))

        if cmd == CSTOCL_READ_STATUS:
            if l != 9:
                raise Exception("readblock: READ_STATUS incorrect message size")
            cid, code = unpack("QB", recv(l))
            if cid != chunkid:
                raise Exception("readblock; READ_STATUS incorrect chunkid")
            conn.close()
            return

        elif cmd == CSTOCL_READ_DATA:
            if l < 20 :
                raise Exception("readblock; READ_DATA incorrect message size")
            cid, bid, boff, bsize, crc = unpack("QHHII", recv(20))
            if cid != chunkid:
                raise Exception("readblock; READ_STATUS incorrect chunkid")
            if l != 20 + bsize:
                raise Exception("readblock; READ_DATA incorrect message size ")
            if bsize == 0 : # FIXME
                raise Exception("readblock; empty block")
            if bid != offset >> 16:
                raise Exception("readblock; READ_DATA incorrect block number")
            if boff != offset & 0xFFFF:
                raise Exception("readblock; READ_DATA incorrect block offset")
            breq = 65536 - boff
            if size < breq:
                breq = size
            if bsize != breq:
                raise Exception("readblock; READ_DATA incorrect block size")

            while breq > 0:
                data = conn.recv(breq)
                if not data:
                    raise IOError("unexpected ending: need %d" % breq)
                yield data
                breq -= len(data)

            offset += bsize
            size -= bsize
        else:
            raise Exception("readblock; unknown message: %s" % cmd)
    conn.close()

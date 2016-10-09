import struct

from consts import *


def uint8(n):
    return struct.pack("B", n)


def uint64(n):
    return struct.pack("!Q", n)


class Error(Exception):
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return mfs_strerror(self.code)


def pack(cmd, *args):
    msg = []
    for a in args:
        if isinstance(a, (int, long)):
            msg.append(struct.pack("!I", a))
        elif isinstance(a, str):
            msg.append(a)
        else:
            raise TypeError(str(type(a))+str(a))
    header = struct.pack("!II", cmd, sum(len(i) for i in msg))
    return header + ''.join(msg)


def unpack(fmt, buf):
    if not fmt.startswith("!"):
        fmt = "!" + fmt
    return struct.unpack(fmt, buf[:struct.calcsize(fmt)])


class FileInfo:
    def __init__(self, inode, name, type, mode, uid, gid,
                 atime, mtime, ctime, nlink, length):
        self.inode = inode
        self.name = name
        self.type = chr(type)
        if type == TYPE_DIRECTORY:
            mode |= S_IFDIR
        elif type == TYPE_SYMLINK:
            mode |= S_IFLNK
        elif type == TYPE_FILE:
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
                (self.name, self.inode, self.type, self.length))

    def is_symlink(self):
        return self.type == TYPE_SYMLINK


def attrToFileInfo(inode, attrs, name=''):
    if len(attrs) != 35:
        raise Exception("invalid length of attrs")
    return FileInfo(inode, name, *struct.unpack("!BHIIIIIIQ", attrs))


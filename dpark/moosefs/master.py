import os
import socket
import threading
import Queue
import time
import struct
import logging

from consts import *
from utils import *

logger = logging.getLogger(__name__)

# mfsmaster need to been patched with dcache
ENABLE_DCACHE = False

class StatInfo:
    def __init__(self, totalspace, availspace, trashspace,
                reservedspace, inodes):
        self.totalspace    = totalspace
        self.availspace    = availspace
        self.trashspace    = trashspace
        self.reservedspace = reservedspace
        self.inodes        = inodes

class Chunk:
    def __init__(self, id, length, version, csdata):
        self.id = id
        self.length = length
        self.version = version
        self.addrs = self._parse(csdata)

    def _parse(self, csdata):
        return [(socket.inet_ntoa(csdata[i:i+4]),
                    unpack("H", csdata[i+4:i+6])[0])
                for i in range(len(csdata))[::6]]

    def __repr__(self):
        return "<Chunk(%d, %d, %d)>" % (self.id, self.version, self.length)

def try_again(f):
    def _(self, *a, **kw):
        for i in range(3):
            try:
                return f(self, *a, **kw)
            except IOError, e:
                self.close()
                logger.warning("mfs master connection: %s", e)
                time.sleep(2**i*0.1)
        else:
            raise
    return _

def spawn(target, *args, **kw):
    t = threading.Thread(target=target, name=target.__name__, args=args, kwargs=kw)
    t.daemon = True
    t.start()
    return t

class MasterConn:
    def __init__(self, host='mfsmaster', port=9421):
        self.host = host
        self.port = port
        self.uid = os.getuid()
        self.gid = os.getgid()
        self.sessionid = 0
        self.conn = None
        self.packetid = 0
        self.fail_count = 0
        self.dcache = {}
        self.dstat = {}

        self.lock = threading.RLock()
        self.reply = Queue.Queue()
        self.is_ready = False
        spawn(self.heartbeat)
        spawn(self.recv_thread)

    def heartbeat(self):
        while True:
            try:
                self.nop()
            except Exception, e:
                self.close()
            time.sleep(2)

    def connect(self):
        if self.conn is not None:
            return

        for _ in range(10):
            try:
                self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.conn.connect((self.host, self.port))
                break
            except socket.error, e:
                self.conn = None
                #self.next_try = time.time() + 1.5 ** self.fail_count
                self.fail_count += 1
                time.sleep(1.5 ** self.fail_count)

        if not self.conn:
            raise IOError("mfsmaster not availbale")

        regbuf = pack(CUTOMA_FUSE_REGISTER, FUSE_REGISTER_BLOB_NOACL,
                      self.sessionid, VERSION)
        self.send(regbuf)
        recv = self.recv(8)
        cmd, i = unpack("II", recv)
        if cmd != MATOCU_FUSE_REGISTER:
            raise Exception("got incorrect answer from mfsmaster %s" % cmd)

        if i not in (1, 4):
            raise Exception("got incorrect size from mfsmaster")

        data = self.recv(i)
        if i == 1:
            code, = unpack("B", data)
            if code != 0:
                raise Exception("mfsmaster register error: "
                        + mfs_strerror(code))
        if self.sessionid == 0:
            self.sessionid, = unpack("I", data)

        self.is_ready = True

    def close(self):
        with self.lock:
            if self.conn:
                self.conn.close()
                self.conn = None
                self.dcache.clear()
                self.is_ready = False

    def send(self, buf):
        with self.lock:
            conn = self.conn
        if not conn:
            raise IOError("not connected")
        n = conn.send(buf)
        while n < len(buf):
            sent = conn.send(buf[n:])
            if not sent:
                self.close()
                raise IOError("write to master failed")
            n += sent

    def nop(self):
        with self.lock:
            self.connect()
            msg = pack(ANTOAN_NOP, 0)
            self.send(msg)

    def recv(self, n):
        with self.lock:
            conn = self.conn
        if not conn:
            raise IOError("not connected")
        r = conn.recv(n)
        while len(r) < n:
            rr = conn.recv(n - len(r))
            if not rr:
                self.close()
                raise IOError("unexpected error: need %d" % (n-len(r)))
            r += rr
        return r

    def recv_cmd(self):
        d = self.recv(12)
        cmd, size = unpack("II", d)
        data = self.recv(size-4) if size > 4 else ''
        while cmd in (ANTOAN_NOP, MATOCU_FUSE_NOTIFY_ATTR, MATOCU_FUSE_NOTIFY_DIR):
            if cmd == ANTOAN_NOP:
                pass
            elif cmd == MATOCU_FUSE_NOTIFY_ATTR:
                while len(data) >= 43:
                    parent, inode = unpack("II", data)
                    attr = data[8:43]
                    if parent in self.dcache:
                        cache = self.dcache[parent]
                        for name in cache:
                            if cache[name].inode == inode:
                                cache[name] = attrToFileInfo(inode, attr)
                                break
                    data = data[43:]
            elif cmd == MATOCU_FUSE_NOTIFY_DIR:
                while len(data) >= 4:
                    inode, = unpack("I", data)
                    if inode in self.dcache:
                        del self.dcache[inode]
                        with self.lock:
                            self.send(pack(CUTOMA_FUSE_DIR_REMOVED, 0, inode))
                    data = data[4:]
            d = self.recv(12)
            cmd, size = unpack("II", d)
            data = self.recv(size-4) if size > 4 else ''
        return d, data

    def recv_thread(self):
        while True:
            with self.lock:
                if not self.is_ready:
                    time.sleep(0.01)
                    continue
            try:
                r = self.recv_cmd()
                self.reply.put(r)
            except IOError, e:
                self.reply.put(e)

    @try_again
    def sendAndReceive(self, cmd, *args):
        #print 'sendAndReceive', cmd, args
        self.packetid += 1
        msg = pack(cmd, self.packetid, *args)
        with self.lock:
            self.connect()
            while not self.reply.empty():
                self.reply.get_nowait()
            self.send(msg)
        r = self.reply.get()
        if isinstance(r, Exception):
            raise r
        h, d = r
        rcmd, size, pid = unpack("III", h)
        if rcmd != cmd+1 or pid != self.packetid or size <= 4:
            self.close()
            raise Exception("incorrect answer (%s!=%s, %s!=%s, %d<=4",
                rcmd, cmd+1, pid, self.packetid, size)
        if len(d) == 1 and ord(d[0]) != 0:
            raise Error(ord(d[0]))
        return d

    def statfs(self):
        ans = self.sendAndReceive(CUTOMA_FUSE_STATFS)
        return StatInfo(*unpack("QQQQI", ans))

#    def access(self, inode, modemask):
#        return self.sendAndReceive(CUTOMA_FUSE_ACCESS, inode,
#            self.uid, self.gid, uint8(modemask))
#
    def lookup(self, parent, name):
        if ENABLE_DCACHE:
            cache = self.dcache.get(parent)
            if cache is None and self.dstat.get(parent, 0) > 1:
                cache = self.getdirplus(parent)
            if cache is not None:
                return cache.get(name), None

            self.dstat[parent] = self.dstat.get(parent, 0) + 1

        ans = self.sendAndReceive(CUTOMA_FUSE_LOOKUP, parent,
                uint8(len(name)), name, 0, 0)
        if len(ans) == 1:
            return None, ""
        if len(ans) != 39:
            raise Exception("bad length")
        inode, = unpack("I", ans)
        return attrToFileInfo(inode, ans[4:]), None

    def getattr(self, inode):
        ans = self.sendAndReceive(CUTOMA_FUSE_GETATTR, inode,
                self.uid, self.gid)
        return attrToFileInfo(inode, ans)

    def readlink(self, inode):
        ans = self.sendAndReceive(CUTOMA_FUSE_READLINK, inode)
        length, = unpack("I", ans)
        if length+4 != len(ans):
            raise Exception("invalid length")
        return ans[4:-1]

    def getdir(self, inode):
        "return: {name: (inode,type)}"
        ans = self.sendAndReceive(CUTOMA_FUSE_GETDIR, inode,
                self.uid, self.gid)
        p = 0
        names = {}
        while p < len(ans):
            length, = unpack("B", ans[p:p+1])
            p += 1
            if length + p + 5 > len(ans):
                break
            name = ans[p:p+length]
            p += length
            inode, type = unpack("IB", ans)
            names[name] = (inode, type)
            p += 5
        return names

    def getdirplus(self, inode):
        "return {name: FileInfo()}"
        if ENABLE_DCACHE:
            infos = self.dcache.get(inode)
            if infos is not None:
                return infos

        flag = GETDIR_FLAG_WITHATTR
        if ENABLE_DCACHE:
            flag |= GETDIR_FLAG_DIRCACHE
        ans = self.sendAndReceive(CUTOMA_FUSE_GETDIR, inode,
                self.uid, self.gid, uint8(flag))
        p = 0
        infos = {}
        while p < len(ans):
            length, = unpack("B", ans[p:p+1])
            p += 1
            name = ans[p:p+length]
            p += length
            i, = unpack("I", ans[p:p+4])
            attr = ans[p+4:p+39]
            infos[name] = attrToFileInfo(i, attr, name)
            p += 39
        if ENABLE_DCACHE:
            self.dcache[inode] = infos
        return infos

    def opencheck(self, inode, flag=1):
        ans = self.sendAndReceive(CUTOMA_FUSE_OPEN, inode,
                self.uid, self.gid, uint8(flag))
        return ans

    def readchunk(self, inode, index):
        ans = self.sendAndReceive(CUTOMA_FUSE_READ_CHUNK, inode, index)
        n = len(ans)
        if n < 20 or (n-20)%6 != 0:
            raise Exception("read chunk: invalid length: %s" % n)
        length, id, version = unpack("QQI", ans)
        return Chunk(id, length, version, ans[20:])


def test():
    m = MasterConn("mfsmaster")
    m.connect()
    m.close()
    #print m.get_attr(1)
    while True:
        print m.getdir(1)
        print m.getdirplus(1)
        time.sleep(60)
    info, err = m.lookup(1, "test.csv")
    print info, err
    #print m.opencheck(info.inode)
    chunks = m.readchunk(info.inode, 0)
    print chunks, chunks.addrs

    for i in range(1000):
        info, err = m.lookup(1, "test.csv")
        chunks = m.readchunk(info.inode, 0)
        print i,err, chunks
        time.sleep(10)

    m.close()

if __name__ == '__main__':
    test()

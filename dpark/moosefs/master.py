import os
import socket
import threading
import Queue
import time

from dpark.util import get_logger

from consts import *
from utils import *

logger = get_logger(__name__)


class StatInfo:

    def __init__(self, totalspace, availspace, trashspace,
                 sustainedspace, inodes):
        self.totalspace = totalspace
        self.availspace = availspace
        self.trashspace = trashspace
        self.sustainedspace = sustainedspace
        self.inodes = inodes


class Chunk:

    def __init__(self, id, length, version, csdata):
        self.id = id
        self.length = length
        self.version = version
        self.addrs = self._parse(csdata)

    def _parse(self, csdata):
        return [(socket.inet_ntoa(csdata[i:i + 4]),
                 unpack("H", csdata[i + 4:i + 6])[0])
                for i in range(len(csdata))[::6]]

    def __repr__(self):
        return "<Chunk(%d, %d, %d)>" % (self.id, self.version, self.length)


def try_again(f):
    def _(self, *a, **kw):
        for i in range(3):
            try:
                return f(self, *a, **kw)
            except IOError as e:
                self.close()
                logger.warning("mfs master connection: %s", e)
                time.sleep(2**i * 0.1)
        else:
            raise
    return _


def spawn(target, *args, **kw):
    t = threading.Thread(
        target=target,
        name=target.__name__,
        args=args,
        kwargs=kw)
    t.daemon = True
    t.start()
    return t


class MasterConn:

    def __init__(self, host='mfsmaster', port=9421, mountpoint="/", subfolder="/"):
        self.host = host
        self.port = port
        self.uid = os.getuid()
        self.gid = os.getgid()
        self.sessionid = 0
        self.sesflags = 0
        self.masterversion = 0
        self.conn = None
        self.packetid = 0
        self.fail_count = 0
        self.dstat = {}
        self.mountpoint = mountpoint + '\0'
        self.subfolder = subfolder + '\0'

        self.lock = threading.RLock()
        self.reply = Queue.Queue()
        self.is_ready = False
        # notify the daemon thread to exit
        self.term = False
        self.thread_heartbeat = spawn(self.heartbeat)
        self.thread_recv = spawn(self.recv_thread)

    def heartbeat(self):
        while True:
            try:
                self.nop()
            except Exception as e:
                logger.debug('close in heart beat except:%s', e)
                self.close()
            time.sleep(2)
            with self.lock:
                if self.term:
                    break

    def connect(self):
        if self.conn is not None:
            return
        for _ in range(10):
            try:
                self.conn = socket.socket(socket.AF_INET,
                                          socket.SOCK_STREAM)
                self.conn.connect((self.host, self.port))
                logger.debug('the host:%s, the port:%d, local:%s',
                             self.host, self.port,
                             self.conn.getsockname())
                break
            except socket.error:
                self.conn = None
                self.fail_count += 1
                time.sleep(1.5 ** self.fail_count)
        if not self.conn:
            raise IOError("mfsmaster not availbale")
        regbuf = pack(CLTOMA_FUSE_REGISTER, FUSE_REGISTER_BLOB_ACL,
                      REGISTER_NEWSESSION, VERSION, len(self.mountpoint),
                      self.mountpoint, len(self.subfolder), self.subfolder)
        self.send(regbuf)
        recv = self.recv(8)
        cmd, i = unpack("II", recv)
        if cmd != MATOCL_FUSE_REGISTER:
            raise Exception("got incorrect answer from mfsmaster %s" % cmd)

        if i not in (1, 4, 13, 21, 25, 35):
            raise Exception("got incorrect size from mfsmaster")

        data = self.recv(i)
        if len(data) != i:
            raise Exception("error receiving data from mfsmaster")

        if i == 1:
            code, = unpack("B", data)
            if code != 0:
                raise Exception("mfsmaster register error: "
                                + mfs_strerror(code))
        elif i == 4:
            self.host = socket.inet_ntoa(unpack("I", data))
            logger.warning('mfsmaster redirected to %s', self.host)
            return self.connect()

        if i in [25, 35]:
            self.masterversion, self.sessionid, self.sesflags = unpack("IIB", data)
        else:
            self.sessionid, self.sesflags = unpack("I", data)
        logger.debug('start connection with session id:%s ' % (self.sessionid))
        self.is_ready = True

    def close(self):
        with self.lock:
            if self.conn:
                self.conn.close()
                self.conn = None
                self.is_ready = False
        logger.debug('close the socket at session:%s', self.sessionid)

    def _close_session(self):
        with self.lock:
            if self.conn:
                # close session
                regbuf = pack(CLTOMA_FUSE_REGISTER,
                              FUSE_REGISTER_BLOB_ACL,
                              REGISTER_CLOSESESSION,
                              self.sessionid)
                try:
                    self.send(regbuf)
                    recv_cnt = 0
                    bound = 2
                    self.conn.settimeout(2)
                    while recv_cnt < bound:
                        try:
                            r = self.recv(8)
                            cmd, size = unpack("II", r)
                            logger.debug('clean recv: cmd:%d, size:%d',
                                         cmd, size)
                            r = self.recv(size)
                        except Exception as e:
                            logger.debug('end of exception:%s' % e)
                            break
                        recv_cnt += 1
                except:
                    logger.debug('send exception ignore')

    def terminate(self):
        with self.lock:
            self.term = True
        self.thread_recv.join(2)
        self.thread_heartbeat.join(3)
        self._close_session()
        self.close()

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
                raise IOError("unexpected error: need %d" % (n - len(r)))
            r += rr
        return r

    def recv_cmd(self):
        while True:
            d = self.recv(12)
            cmd, size = unpack("II", d)
            data = self.recv(size - 4) if size > 4 else ''
            with self.lock:
                if cmd != ANTOAN_NOP or self.term:
                    return d, data

    def recv_thread(self):
        while True:
            with self.lock:
                if self.term:
                    break
                if not self.is_ready:
                    time.sleep(0.01)
                    continue
            try:
                r = self.recv_cmd()
                self.reply.put(r)
            except IOError as e:
                self.reply.put(e)

    @try_again
    def sendAndReceive(self, cmd, *args):
        # print 'sendAndReceive', cmd, args
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
        if rcmd != cmd + 1 or pid != self.packetid or size <= 4:
            self.close()
            raise Exception("incorrect answer (%s!=%s, %s!=%s, %d<=4",
                            rcmd, cmd + 1, pid, self.packetid, size)
        if len(d) == 1 and ord(d[0]) != 0:
            raise Error(ord(d[0]))
        return d

    def statfs(self):
        ans = self.sendAndReceive(CLTOMA_FUSE_STATFS)
        return StatInfo(*unpack("QQQQI", ans))

#    def access(self, inode, modemask):
#        return self.sendAndReceive(CLTOMA_FUSE_ACCESS, inode,
#            self.uid, self.gid, uint8(modemask))
#
    def lookup(self, parent, name):
        ans = self.sendAndReceive(CLTOMA_FUSE_LOOKUP, parent,
                                  uint8(len(name)), name, 0, 0)
        if len(ans) == 1:
            return None, ""
        if len(ans) != 39:
            raise Exception("bad length")
        inode, = unpack("I", ans)
        return attrToFileInfo(inode, ans[4:]), None

    def getattr(self, inode):
        ans = self.sendAndReceive(CLTOMA_FUSE_GETATTR, inode,
                                  self.uid, self.gid)
        return attrToFileInfo(inode, ans)

    def readlink(self, inode):
        ans = self.sendAndReceive(CLTOMA_FUSE_READLINK, inode)
        length, = unpack("I", ans)
        if length + 4 != len(ans):
            raise Exception("invalid length")
        return ans[4:-1]

    def getdir(self, inode):
        "return: {name: (inode,type)}"
        ans = self.sendAndReceive(CLTOMA_FUSE_GETDIR, inode,
                                  self.uid, self.gid)
        p = 0
        names = {}
        while p < len(ans):
            length, = unpack("B", ans[p:p + 1])
            p += 1
            if length + p + 5 > len(ans):
                break
            name = ans[p:p + length]
            p += length
            inode, type = unpack("IB", ans)
            names[name] = (inode, type)
            p += 5
        return names

    def getdirplus(self, inode):
        "return {name: FileInfo()}"
        flag = GETDIR_FLAG_WITHATTR
        ans = self.sendAndReceive(CLTOMA_FUSE_GETDIR, inode,
                                  self.uid, self.gid, uint8(flag))
        p = 0
        infos = {}
        while p < len(ans):
            length, = unpack("B", ans[p:p + 1])
            p += 1
            name = ans[p:p + length]
            p += length
            i, = unpack("I", ans[p:p + 4])
            attr = ans[p + 4:p + 39]
            infos[name] = attrToFileInfo(i, attr, name)
            p += 39
        return infos

    def opencheck(self, inode, flag=1):
        ans = self.sendAndReceive(CLTOMA_FUSE_OPEN, inode,
                                  self.uid, self.gid, uint8(flag))
        return ans

    def readchunk(self, inode, index):
        ans = self.sendAndReceive(CLTOMA_FUSE_READ_CHUNK, inode, index)
        n = len(ans)
        if n < 20 or (n - 20) % 6 != 0:
            raise Exception("read chunk: invalid length: %s" % n)
        length, id, version = unpack("QQI", ans)
        return Chunk(id, length, version, ans[20:])

    def append(self, inode_dst, inode_src):
        self.sendAndReceive(CLTOMA_FUSE_APPEND, inode_dst, inode_src,
                            self.uid, self.gid)


def test():
    m = MasterConn("mfsmaster")
    m.connect()
    m.close()
    # print m.get_attr(1)
    while True:
        print m.getdir(1)
        print m.getdirplus(1)
        time.sleep(60)
    info, err = m.lookup(1, "test.csv")
    print info, err
    # print m.opencheck(info.inode)
    chunks = m.readchunk(info.inode, 0)
    print chunks, chunks.addrs

    for i in range(1000):
        info, err = m.lookup(1, "test.csv")
        chunks = m.readchunk(info.inode, 0)
        print i, err, chunks
        time.sleep(10)

    m.close()

if __name__ == '__main__':
    test()

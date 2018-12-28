from __future__ import absolute_import
import os
import grp
import time
import socket
import getpass
from .utils import unpack, pack, uint8, attrToFileInfo, uint64
from .consts import *
import six
from six.moves import range

from dpark.utils.log import get_logger

logger = get_logger(__name__)


class ProtocolError(Exception):
    pass


class Chunk:

    def __init__(self, index, id_, file_length, version, csdata, ele_width=6):
        self.index = index
        self.id = id_
        self.file_length = file_length
        self.length = min(file_length - index * CHUNKSIZE, CHUNKSIZE)
        self.version = version
        self.ele_width = ele_width
        self.addrs = self._parse(csdata)

    def _parse(self, csdata):
        return [(socket.inet_ntoa(csdata[i:i + 4]),
                 unpack("H", csdata[i + 4:i + 6])[0])
                for i in range(0, len(csdata), self.ele_width)]

    def __repr__(self):
        return "<Chunk(%d, %d, %d)>" % (self.id, self.version, self.length)


class ProxyConn(object):
    def __init__(self, host, port, version=(0, 0, 0)):
        self.host = host
        self.port = port
        self.version = version
        self.uid = os.getuid()
        self.gids = [g.gr_gid for g in grp.getgrall() if getpass.getuser() in g.gr_mem]
        self.gids.insert(0, os.getgid())
        self.conn = None
        self.msgid = 0

    @classmethod
    def get_masterinfo(cls, path):
        while path != os.path.sep:
            mp = os.path.join(path, ".masterinfo")
            try:
                stb = os.lstat(mp)
            except OSError:
                pass
            else:
                if stb.st_ino in (0x7FFFFFFF, 0x7FFFFFFE) and \
                        stb.st_nlink == 1 and \
                        stb.st_uid == 0 and \
                        stb.st_gid == 0 and \
                        stb.st_size in (10, 14):
                    sz = stb.st_size
                    with open(mp, 'rb', 0) as f:
                        proxyinfo = f.read(sz)
                        if len(proxyinfo) != sz:
                            raise Exception('fail to read master info from %s' % mp)
                        ip = socket.inet_ntoa(proxyinfo[:4])
                        port, = unpack("H", proxyinfo[4:])
                        if stb.st_size > 10:
                            major_version, mid_version, minor_version = unpack('HBB', proxyinfo[10:])
                            if major_version > 1:
                                minor_version /= 2
                            return ip, port, (major_version, mid_version, minor_version)
                        else:
                            return ip, port, (0, 0, 0)
            path = os.path.dirname(path)

    def recv_full(self, n):
        r = b""
        while len(r) < n:
            rr = self.conn.recv(n - len(r))
            if not rr:
                raise IOError('need %d bytes, got %d', n, len(r))
            r += rr
        return r

    def send_full(self, buf):
        n = self.conn.send(buf)
        while n < len(buf):
            sent = self.conn.send(buf[n:])
            if not sent:
                raise IOError('Write failed')
            n += sent

    def _connect(self):
        if self.conn is not None:
            return
        N = 8
        for i in range(N):
            try:
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.connect((self.host, self.port))
                self.conn = conn
                return
            except socket.error as e:
                if i == N - 1:
                    raise Exception("Fail to connect to mfs proxy %s:%s, %s", self.host, self.port, e)
                time.sleep(1.5 ** i)  # 1.5**8 = 25.6

    def _recv_cmd(self, cmd):
        d = self.recv_full(12)
        rcmd, size, msgid = unpack("III", d)
        data = self.recv_full(size - 4)
        if rcmd != cmd + 1:
            raise ProtocolError("get incorrect cmd (%s \!= %s)" % (rcmd, cmd + 1))

        if msgid != self.msgid:
            raise ProtocolError('get incorrect msgid(%s != %s)' % (msgid, self.msgid))
        return data

    def sendAndReceive(self, cmd, *args):
        self.msgid += 1
        msg = pack(cmd, self.msgid, *args)
        num_retry = 3
        for i in range(num_retry):
            self._connect()
            try:
                self.send_full(msg)
                data = self._recv_cmd(cmd)
                return data
            except IOError:
                self.terminate()
                if i == num_retry - 1:
                    raise
                else:
                    time.sleep(2 ** i * 0.1)

    def terminate(self):
        self.conn.close()
        self.conn = None

    def getdirplus(self, inode, max_entries=0xFFFFFFFF, nedgeid=0):
        flag = GETDIR_FLAG_WITHATTR
        if self.version < (2, 0, 0):
            ans = self.sendAndReceive(CLTOMA_FUSE_READDIR, inode, self.uid, self.gids[0], uint8(flag))
        else:
            gidsize = len(self.gids)
            gids = [gid for gid in self.gids]
            gids.append(uint8(flag))
            gids.append(max_entries)
            gids.append(uint64(nedgeid))
            ans = self.sendAndReceive(CLTOMA_FUSE_READDIR, inode,
                                      self.uid, gidsize, *gids)
        p = 0
        infos = {}
        # rnedgeid, = unpack('Q', ans[p: p + 8])
        p += 8
        while p < len(ans):
            length, = unpack('B', ans[p: p + 1])
            p += 1
            name = ans[p: p + length]
            if not six.PY2:
                name = name.decode('utf-8')

            p += length
            i, = unpack("I", ans[p: p + 4])
            p += 4
            attr = ans[p: p + 35]
            infos[name] = attrToFileInfo(i, attr, name, self.version)
            p += 35
        return infos

    def getattr(self, inode, opened=0):
        if self.version < (1, 6, 28):
            ans = self.sendAndReceive(CLTOMA_FUSE_GETATTR, inode,
                                      self.uid, self.gids[0])
        else:
            ans = self.sendAndReceive(CLTOMA_FUSE_GETATTR, inode,
                                      uint8(opened), self.uid, self.gids[0])
        return attrToFileInfo(inode, ans[:35], version=self.version)

    def readlink(self, inode):
        ans = self.sendAndReceive(CLTOMA_FUSE_READLINK, inode)
        length, = unpack('I', ans)
        if length + 4 != len(ans):
            raise Exception('invalid length')
        return ans[4: -1]

    def readchunk(self, inode, index, chunkopflags=0):
        """
        // msgid:32 length:64 chunkid:64 version:32 N*[ ip:32 port:16 ]
        // msgid:32 protocolid:8 length:64 chunkid:64 version:32 N*[ ip:32 port:16 cs_ver:32 ]
                (master and client both versions >= 1.7.32 - protocolid==1)
        // msgid:32 protocolid:8 length:64 chunkid:64 version:32 N*[ ip:32 port:16 cs_ver:32 labelmask:32 ]
                (master and client both versions >= 3.0.10 - protocolid==2)
        """

        cnt = 0
        while True:
            cnt += 1
            if self.version < (3, 0, 4):
                ans = self.sendAndReceive(CLTOMA_FUSE_READ_CHUNK, inode, index)
            else:
                ans = self.sendAndReceive(CLTOMA_FUSE_READ_CHUNK, inode,
                                          index, uint8(chunkopflags))
            n = len(ans)
            if n == 1:
                from .utils import Error
                err = ord(ans)
                if err == ERROR_LOCKED:
                    if cnt < 100:
                        time.sleep(0.1)
                        continue

                    logger.warning('Waited too long for locked chunk %s:%s', inode, index)

                raise Error(ord(ans))

            if n < 20:
                raise Exception('read chunk invalid length: %s(expected 20 above)' % n)

            # self.version is master`s version, not mfsmount`s
            if self.version >= (1, 7, 32) and ((n - 21) % 14 == 0 or (n - 21) % 10 == 0):
                protocolid, flength, id_, version = unpack('BQQI', ans)
                if protocolid == 2:
                    assert (n - 21) % 14 == 0, n
                    return Chunk(index, id_, flength, version, ans[21:], ele_width=14)
                elif protocolid == 1:
                    assert (n - 21) % 10 == 0, n
                    return Chunk(index, id_, flength, version, ans[21:], ele_width=10)

            assert (n - 20) % 6 == 0, n
            flength, id_, version = unpack("QQI", ans)
            return Chunk(index, id_, flength, version, ans[20:])

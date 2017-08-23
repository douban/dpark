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


class ProtocolError(Exception):
    pass


class Chunk:

    def __init__(self, id, length, version, csdata, ele_width=6):
        self.id = id
        self.length = length
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
        for i in range(10):
            try:
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.connect((self.host, self.port))
                self.conn = conn
                return
            except socket.error:
                time.sleep(1.5 ** i)

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
            l = [gid for gid in self.gids]
            l.append(uint8(flag))
            l.append(max_entries)
            l.append(uint64(nedgeid))
            ans = self.sendAndReceive(CLTOMA_FUSE_READDIR, inode,
                                      self.uid, gidsize, *l)
        p = 0
        infos = {}
        rnedgeid, = unpack('Q', ans[p: p + 8])
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
        if self.version < (3, 0, 4):
            ans = self.sendAndReceive(CLTOMA_FUSE_READ_CHUNK, inode, index)
        else:
            ans = self.sendAndReceive(CLTOMA_FUSE_READ_CHUNK, inode,
                                      index, uint8(chunkopflags))
        n = len(ans)
        if n == 1:
            from .utils import Error
            raise Error(ord(ans))
        if n % 2 == 0:
            if n < 20:
                raise Exception('read chunk invalid length: %s(expected 20 above)' % n)
            if (n - 20) % 6 == 0:
                length, id, version = unpack("QQI", ans)
                return Chunk(id, length, version, ans[20:])
        else:
            if n < 21:
                raise Exception('read chunk invalid length: %s(expected 21 above)' % n)
            if (n - 21) % 10 == 0:
                protocolid, length, id, version = unpack('BQQI', ans)
                return Chunk(id, length, version, ans[21:], ele_width=10)
            elif (n - 21) % 14 == 0:
                protocolid, length, id, version = unpack('BQQI', ans)
                return Chunk(id, length, version, ans[21:], ele_width=14)

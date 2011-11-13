import os
import socket
import threading
import time
import struct

from consts import *
from utils import *

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

class MasterConn:
    def __init__(self, host='mfsmaster', port=9421):
        self.host = host
        self.port = port
        self.uid = os.getuid()
        self.gid = os.getgid()
        self.sessionid = 0
        self.conn = None
        self.fail_count = 0

    def connect(self):
        if self.conn is not None:
            return
        try:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #self.conn.settimeout(1)
            self.conn.connect((self.host, self.port))
        except socket.error, e:
            self.conn = None
            self.next_try = time.time() + 1.5 ** self.fail_count
            self.fail_count += 1
            raise
        
        if self.sessionid == 0:
            regbuf = pack(CUTOMA_FUSE_REGISTER, FUSE_REGISTER_BLOB_ACL,
                    uint8(REGISTER_NEWSESSION), VERSION, 2, "/\000", 2, "/\000")
        else:
            regbuf = pack(CUTOMA_FUSE_REGISTER, FUSE_REGISTER_BLOB_ACL,
                    uint8(REGISTER_RECONNECT), self.sessionid, VERSION)
        self.send(regbuf)
        recv = self.recv(8)
        cmd, i = unpack("II", recv)
        if cmd != MATOCU_FUSE_REGISTER:
            raise Exception("got incorrect answer from mfsmaster %s" % cmd)

        if i not in (1, 13, 21):
            raise Exception("got incorrect size from mfsmaster")

        data = self.recv(i)
        if i == 1:
            code, = unpack("B", data)
            if code != 0:
                raise Exception("mfsmaster register error: " 
                        + mfs_strerror(code))
        if self.sessionid == 0:
            self.sessionid, = unpack("I", data)


    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def send(self, buf):
        #print 'send', len(buf), " ".join(str(ord(c)) for c in buf)
        self.conn.send(buf)

    def nop(self):
        msg = pack(uint8(ANTOAN_NOP), 0)
        self.conn.send(msg)

    def recv(self, n):
        d = self.conn.recv(n)
        if len(d)>=8:
            cmd, size = unpack("II", d)
            while cmd == ANTOAN_NOP:
                d = d[12:] + self.conn.recv(12)
                cmd, size = unpack("II", d)
        assert len(d) == n, 'unexpected end: %s != %s' % (len(d), n)
        return d

    def sendAndReceive(self, cmd, *args):
        #print 'sendAndReceive', cmd, args
        packetid = 1
        msg = pack(cmd, packetid, *args)
        # TODO lock
        self.connect()
        self.send(msg)
        r = self.recv(12)
        rcmd, size, id = unpack("III", r)
        if rcmd != cmd+1 or id != packetid or size <= 4:
            self.close()
            raise Exception("incorrect answer (%s!=%s, %s!=%s, %d<=4", 
                rcmd, cmd+1, id, packetid, size)
        d = self.conn.recv(size-4)
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
        ans = self.sendAndReceive(CUTOMA_FUSE_LOOKUP, parent, 
                uint8(len(name)), name, 0, 0)
        if len(ans) == 1:
            return None, ""
        if len(ans) != 39:
            return None, "bad length"
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
        return ans[4:]

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
        ans = self.sendAndReceive(CUTOMA_FUSE_GETDIR, inode, 
                self.uid, self.gid, uint8(GETDIR_FLAG_WITHATTR))
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
    m = MasterConn("localhost")
    m.connect()
    m.close()
    m.connect()
    #print m.get_attr(1)
    print m.getdir(1)
    print m.getdirplus(1)
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

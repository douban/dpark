import os
import socket
import struct

from utils import *

mfsdirs = []
for conf in ('/etc/mfshdd.cfg', '/usr/local/etc/mfshdd.cfg'):
    if not os.path.exists(conf):
        continue
    f = open(conf)
    for line in f:
        path = line.strip('#* \n')
        if os.path.exists(path):
            mfsdirs.append(path)
    f.close()

CHUNKHDRSIZE = 1024 * 5
CHUNKSIZE = 1 << 26

def read_chunk(host, port, chunkid, version, size, offset=0):
    if offset + size > CHUNKSIZE:
        raise ValueError("size too large %s > %s" % 
            (size, CHUNKSIZE-offset))
    
    from dpark.accumulator import ReadBytes, LocalReadBytes

    name = '%02X/chunk_%016X_%08X.mfs' % (chunkid & 0xFF, chunkid, version)
    for d in mfsdirs:
        p = os.path.join(d, name)
        if os.path.exists(p):
            if os.path.getsize(p) < CHUNKHDRSIZE + offset + size:
                raise ValueError("size too large")
            f = open(p)
            f.seek(CHUNKHDRSIZE + offset)
            while size > 0:
                to_read = min(size, 1024*1024*4)
                data = f.read(to_read)
                yield data
                LocalReadBytes.add(len(data))
                size -= len(data)
                if len(data) < to_read:
                    break
            f.close()
            return

    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.settimeout(10)
    conn.connect((host, port))

    msg = pack(CUTOCS_READ, uint64(chunkid), version, offset, size) 
    n = conn.send(msg)
    while n < len(msg):
        msg = msg[n:]
   
    def recv(n):
        d = conn.recv(n)
        while len(d) < n:
            d += conn.recv(n-len(d))
        return d

    while size > 0:
        cmd, l = unpack("II", recv(8))

        if cmd == CSTOCU_READ_STATUS:
            if l != 9:
                raise Exception("readblock: READ_STATUS incorrect message size")
            cid, code = unpack("QB", recv(l))
            if cid != chunkid:
                raise Exception("readblock; READ_STATUS incorrect chunkid")
            conn.close()
            return

        elif cmd == CSTOCU_READ_DATA:
            if l < 20 :
                raise Exception("readblock; READ_DATA incorrect message size")
            cid, bid, boff, bsize, crc = unpack("QHHII", recv(20))
            if cid != chunkid:
                raise Exception("readblock; READ_STATUS incorrect chunkid")
            if l != 20 + bsize:
                raise Exception("readblock; READ_DATA incorrect message size ")
            if bsize == 0 : # FIXME
                raise Exception("readblock; empty block")
                #yield ""
                continue
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
                yield data
                ReadBytes.add(len(data))
                breq -= len(data)
            offset += bsize
            size -= bsize
        else:
            raise Exception("readblock; unknown message: %s" % cmd)
    conn.close()


def test():
    d = list(read_block('192.168.11.3', 9422, 6544760, 1, 6, 0))
    print len(d), sum(len(s) for s in d)
    d = list(read_block('192.168.11.3', 9422, 6544936, 1, 46039893, 0))
    print len(d), sum(len(s) for s in d)

if __name__ == '__main__':
    test()

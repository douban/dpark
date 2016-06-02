import os
import socket
import commands
from dpark.util import get_logger

from consts import CHUNKSIZE, CLTOCS_READ, CSTOCL_READ_DATA, CSTOCL_READ_STATUS
from utils import uint64, pack, unpack


logger = get_logger(__name__)

mfsdirs = []
def _scan():
    cmd = """ps -eo cmd| grep mfschunkserver | grep -v grep |
    head -1 | cut -d ' ' -f1 | xargs dirname | sed 's#sbin##g'"""
    mfs_prefix = commands.getoutput(cmd)
    mfs_cfg = '%s/etc/mfshdd.cfg' % mfs_prefix
    mfs_cfg_list = (mfs_cfg, '/etc/mfs/mfshdd.cfg',
    '/etc/mfshdd.cfg', '/usr/local/etc/mfshdd.cfg')
    for conf in mfs_cfg_list:
        if not os.path.exists(conf):
            continue
        f = open(conf)
        for line in f:
            if line.startswith('#') or line.startswith('*'):
                continue
            fields = line.split()
            if fields:
                path = fields[0]
                if os.path.exists(path):
                    mfsdirs.append(path)
        f.close()
_scan()

CHUNKHDRSIZE = 1024 * 5

def read_chunk_from_local(chunkid, version, size, offset=0):
    if offset + size > CHUNKSIZE:
        raise ValueError("size too large %s > %s" %
            (size, CHUNKSIZE-offset))

    from dpark.accumulator import LocalReadBytes
    name = '%02X/chunk_%016X_%08X.mfs' % (chunkid & 0xFF, chunkid, version)
    for d in mfsdirs:
        p = os.path.join(d, name)
        if os.path.exists(p):
            if os.path.getsize(p) < CHUNKHDRSIZE + offset + size:
                logger.error('%s is not completed: %d < %d', name,
                        os.path.getsize(p), CHUNKHDRSIZE + offset + size)
                return
                #raise ValueError("size too large")
            f = open(p)
            f.seek(CHUNKHDRSIZE + offset)
            while size > 0:
                to_read = min(size, 640*1024)
                data = f.read(to_read)
                if not data:
                    return
                LocalReadBytes.add(len(data))
                yield data
                size -= len(data)
            f.close()
            return
    else:
        logger.warning("%s was not found", name)


def read_chunk(host, port, chunkid, version, size, offset=0):
    if offset + size > CHUNKSIZE:
        raise ValueError("size too large %s > %s" %
            (size, CHUNKSIZE-offset))

    from dpark.accumulator import RemoteReadBytes

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
                #yield ""
                #continue
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
                    #print chunkid, version, offset, size, bsize, breq
                    raise IOError("unexpected ending: need %d" % breq)
                RemoteReadBytes.add(len(data))
                yield data
                breq -= len(data)

            offset += bsize
            size -= bsize
        else:
            raise Exception("readblock; unknown message: %s" % cmd)
    conn.close()


def test():
    d = list(read_chunk('192.168.11.3', 9422, 6544760, 1, 6, 0))
    print len(d), sum(len(s) for s in d)
    d = list(read_chunk('192.168.11.3', 9422, 6544936, 1, 46039893, 0))
    print len(d), sum(len(s) for s in d)

if __name__ == '__main__':
    test()

import os
import zmq
import time
import uuid
import binascii
import random
import socket
import struct
import cPickle
import marshal

from dpark.util import compress, decompress, spawn, get_logger
from dpark.cache import Cache
from dpark.serialize import marshalable
from dpark.env import env

logger = get_logger(__name__)

MARSHAL_TYPE, PICKLE_TYPE = range(2)
BLOCK_SHIFT = 20
BLOCK_SIZE = 1 << BLOCK_SHIFT

class BroadcastManager:
    header_fmt = '>BI'
    header_len = struct.calcsize(header_fmt)

    def start(self, is_master):
        raise NotImplementedError

    def shutdown(self):
        raise NotImplementedError

    def register(self, uuid, value):
        raise NotImplementedError

    def clear(self, uuid):
        raise NotImplementedError

    def fetch(self, uuid, block_num):
        raise NotImplementedError

    def to_blocks(self, uuid, obj):
        try:
            if marshalable(obj):
                buf = marshal.dumps((uuid, obj))
                type = MARSHAL_TYPE
            else:
                buf = cPickle.dumps((uuid, obj), -1)
                type = PICKLE_TYPE

        except Exception:
            buf = cPickle.dumps((uuid, obj), -1)
            type = PICKLE_TYPE

        checksum = binascii.crc32(buf) & 0xFFFF
        stream = struct.pack(self.header_fmt, type, checksum) + buf
        blockNum = (len(stream) + (BLOCK_SIZE - 1)) >> BLOCK_SHIFT
        blocks = [compress(stream[i*BLOCK_SIZE:(i+1)*BLOCK_SIZE]) for i in range(blockNum)]
        return blocks

    def from_blocks(self, uuid, blocks):
        stream = ''.join(map(decompress,  blocks))
        type, checksum = struct.unpack(self.header_fmt, stream[:self.header_len])
        buf = stream[self.header_len:]
        _checksum = binascii.crc32(buf) & 0xFFFF
        if _checksum != checksum:
            raise RuntimeError('Wrong blocks: checksum: %s, expected: %s' % (
                _checksum, checksum))

        if type == MARSHAL_TYPE:
            _uuid, value = marshal.loads(buf)
        elif type == PICKLE_TYPE:
            _uuid, value = cPickle.loads(buf)
        else:
            raise RuntimeError('Unknown serialization type: %s' % type)

        if uuid != _uuid:
            raise RuntimeError('Wrong blocks: uuid: %s, expected: %s' % (_uuid, uuid))

        return value

GUIDE_STOP, GUIDE_INFO, GUIDE_SOURCES, GUIDE_REPORT_BAD = range(4)
SERVER_STOP, SERVER_FETCH, SERVER_FETCH_FAIL, SERVER_FETCH_OK = range(4)

class P2PBroadcastManager(BroadcastManager):
    def __init__(self):
        self.published = {}
        self.cache = Cache()
        self.host = socket.gethostname()
        self.server_thread = None
        random.seed(os.getpid() + int(time.time()*1000)%1000)

    def start(self, is_master):
        if is_master:
            self.guides = {}
            self.guide_addr, self.guide_thread = self.start_guide()
            env.register('BroadcastGuideAddr', self.guide_addr)
        else:
            self.guide_addr = env.get('BroadcastGuideAddr')

        logger.debug("broadcast started: %s", self.guide_addr)

    def shutdown(self):
        sock = env.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(self.guide_addr)
        sock.send_pyobj((GUIDE_STOP, None))
        sock.recv_pyobj()
        sock.close()

    def register(self, uuid, value):
        if uuid in self.published:
            raise RuntimeError('broadcast %s has already registered' % uuid)

        if not self.server_thread:
            self.server_addr, self.server_thread = self.start_server()

        blocks = self.to_blocks(uuid, value)
        self.published[uuid] = blocks
        self.guides[uuid] = {self.server_addr: [1] * len(blocks)}
        self.cache.put(uuid, value)
        return len(blocks)

    def fetch(self, uuid, block_num):
        if not self.server_thread:
            self.server_addr, self.server_thread = self.start_server()

        value = self.cache.get(uuid)
        if value is not None:
            return value

        blocks = self.fetch_blocks(uuid, block_num)
        value = self.from_blocks(uuid, blocks)
        return value

    def clear(self, uuid):
        self.cache.put(uuid, None)
        del self.published[uuid]

    def fetch_blocks(self, uuid, block_num):
        guide_sock = env.ctx.socket(zmq.REQ)
        guide_sock.connect(self.guide_addr)
        logger.debug("connect to guide %s", self.guide_addr)

        blocks = [None] * block_num
        bitmap = [0] * block_num
        self.published[uuid] = blocks
        def _report_bad(addr):
            guide_sock.send_pyobj((GUIDE_REPORT_BAD, (uuid, addr)))
            guide_sock.recv_pyobj()

        def _fetch(addr, indices):
            sock = env.ctx.socket(zmq.REQ)
            try:
                sock.setsockopt(zmq.LINGER, 0)
                sock.connect(addr)
                for i in indices:
                    sock.send_pyobj((SERVER_FETCH, (uuid, i)))
                    avail = sock.poll(5 * 1000, zmq.POLLIN)
                    if not avail:
                        logger.debug("%s recv broadcast %d from %s timeout",
                                       self.server_addr, i, addr)
                        _report_bad(addr)
                        return
                    result, msg = sock.recv_pyobj()

                    if result == SERVER_FETCH_FAIL:
                        _report_bad(addr)
                        return
                    if result == SERVER_FETCH_OK:
                        id, block = msg
                        if i == id and block is not None:
                            blocks[id] = block
                            bitmap[id] = 1
                    else:
                        raise RuntimeError('Unknown server response: %s %s' % (result, msg))

            finally:
                sock.close()

        while not all(bitmap):
            guide_sock.send_pyobj((GUIDE_SOURCES, (uuid, self.server_addr, bitmap)))
            sources = guide_sock.recv_pyobj()
            logger.debug("received SourceInfo from master: %s", sources.keys())
            local = []
            remote = []
            for addr, _bitmap in sources.iteritems():
                if addr.startswith('tcp://%s:' % self.host):
                    local.append((addr, _bitmap))
                else:
                    remote.append((addr, _bitmap))

            for addr, _bitmap in local:
                indices = [i for i in xrange(block_num) if not bitmap[i] and _bitmap[i]]
                if indices:
                    _fetch(addr, indices)

            random.shuffle(remote)
            for addr, _bitmap in remote:
                indices = [i for i in xrange(block_num) if not bitmap[i] and _bitmap[i]]
                if indices:
                    _fetch(addr, [random.choice(indices)])

        guide_sock.close()
        return blocks

    def start_guide(self):
        sock = env.ctx.socket(zmq.REP)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        guide_addr = "tcp://%s:%d" % (self.host, port)

        def run():
            logger.debug("guide start at %s", guide_addr)

            while True:
                type, msg = sock.recv_pyobj()
                if type == GUIDE_STOP:
                    sock.send_pyobj(0)
                    break
                elif type == GUIDE_SOURCES:
                    uuid, addr, bitmap = msg
                    sources = self.guides[uuid]
                    sock.send_pyobj(sources)
                    if any(bitmap):
                        sources[addr] = bitmap
                elif type == GUIDE_REPORT_BAD:
                    uuid, addr = msg
                    sock.send_pyobj(0)
                    sources = self.guides[uuid]
                    if addr in sources:
                        del sources[addr]
                else:
                    logger.error('Unknown guide message: %s %s', type, msg)

            sock.close()
            logger.debug("Sending stop notification to all servers ...")
            for uuid, sources in self.guides.iteritems():
                for addr in sources:
                    self.stop_server(addr)

        return guide_addr, spawn(run)

    def start_server(self):
        sock = env.ctx.socket(zmq.REP)
        sock.setsockopt(zmq.LINGER, 0)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        server_addr = 'tcp://%s:%d' % (self.host,port)

        def run():
            logger.debug("server started at %s", server_addr)

            while True:
                type, msg = sock.recv_pyobj()
                logger.debug('server recv: %s %s', type, msg)
                if type == SERVER_STOP:
                    sock.send_pyobj(None)
                    break
                elif type == SERVER_FETCH:
                    uuid, id = msg
                    if uuid not in self.published:
                        sock.send_pyobj((SERVER_FETCH_FAIL, None))
                    else:
                        blocks = self.published[uuid]
                        if id >= len(blocks):
                            sock.send_pyobj((SERVER_FETCH_FAIL, None))
                        else:
                            sock.send_pyobj((SERVER_FETCH_OK, (id, blocks[id])))
                else:
                    logger.error('Unknown server message: %s %s', type, msg)

            sock.close()
            logger.debug("stop Broadcast server %s", server_addr)
            for uuid in self.published.keys():
                self.clear(uuid)

        return server_addr, spawn(run)

    def stop_server(self, addr):
        req = env.ctx.socket(zmq.REQ)
        req.setsockopt(zmq.LINGER, 0)
        req.connect(addr)
        req.send_pyobj((SERVER_STOP, None))
        avail = req.poll(1 * 100, zmq.POLLIN)
        if avail:
            req.recv_pyobj()
        req.close()
        self.server_thread = None


_manager = P2PBroadcastManager()

def start_manager(is_master):
    _manager.start(is_master)

def stop_manager():
    _manager.shutdown()

class Broadcast:
    def __init__(self, value):
        assert value is not None, 'broadcast object should not been None'
        self.uuid = str(uuid.uuid4())
        self.value = value
        self.block_num = _manager.register(self.uuid, self.value)
        self.bytes = self.block_num * BLOCK_SIZE
        logger.info("broadcast %s in %d blocks", self.uuid, self.block_num)

    def clear(self):
        _manager.clear(self.uuid)

    def __getstate__(self):
        return (self.uuid, self.block_num)

    def __setstate__(self, v):
        self.uuid, self.block_num = v

    def __getattr__(self, name):
        if name != 'value':
            return getattr(self.value, name)

        value = _manager.fetch(self.uuid, self.block_num)
        if value is None:
            raise RuntimeError("fetch broadcast failed")
        self.value = value
        return value


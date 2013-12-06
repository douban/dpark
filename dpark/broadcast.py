import os, time
import uuid
import socket
import marshal
import cPickle
import logging
import gc
import random

import zmq

from dpark.util import compress, decompress, spawn
from dpark.cache import Cache
from dpark.serialize import marshalable
from dpark.env import env

logger = logging.getLogger("broadcast")

class SourceInfo:
    Stop = -2

    def __init__(self, addr):
        self.addr = addr
        self.parents = []
        self.leechers = 0
        self.failed = False

    def __cmp__(self, other):
        return self.leechers - other.leechers

    def __repr__(self):
        return "<source %s (%d)>" % (self.addr, self.leechers)

    def is_child_of(self, addr):
        for p in self.parents:
            if p.addr == addr or p.is_child_of(addr):
                return True
        return False

class Block:
    def __init__(self, id, data):
        self.id = id
        self.data = data

class Broadcast:
    initialized = False
    is_master = False
    cache = Cache() 
    broadcastFactory = None
    BlockSize = 1024 * 1024

    def __init__(self, value, is_local):
        assert value is not None, 'broadcast object should not been None'
        self.uuid = str(uuid.uuid4())
        self.value = value
        self.is_local = is_local
        self.bytes = 0
        self.stopped = False
        if is_local:
            if self.cache.put(self.uuid, value) is None:
                raise Exception('object %s is too big to cache', repr(value))
        else:
            self.send()

    def clear(self):
        self.stopped = True
        self.cache.put(self.uuid, None)
        if hasattr(self, 'value'):
            delattr(self, 'value')

    def __getstate__(self):
        return (self.uuid, self.bytes, self.value if self.bytes < self.BlockSize/20 else None)

    def __setstate__(self, v):
        self.stopped = False
        self.uuid, self.bytes, value = v
        if value is not None:
            self.value = value

    def __getattr__(self, name):
        if name != 'value':
            return getattr(self.value, name)

        if self.stopped:
            raise SystemExit("broadcast has been cleared")

        # in the executor process, Broadcast is not initialized
        if not self.initialized:
            raise AttributeError(name)

        uuid = self.uuid
        value = self.cache.get(uuid)
        if value is not None:
            self.value = value
            return value

        value = self.recv()
        if value is None:
            raise Exception("recv broadcast failed")
        self.value = value
        self.cache.put(uuid, value)

        return value 

    def send(self):
        raise NotImplementedError

    def recv(self):
        raise NotImplementedError

    def blockifyObject(self, obj):
        try:
            if marshalable(obj):
                buf = '0'+marshal.dumps(obj)
            else:
                buf = '1'+cPickle.dumps(obj, -1)

        except Exception:
            buf = '1'+cPickle.dumps(obj, -1)

        N = self.BlockSize
        blockNum = len(buf) / N + 1
        val = [Block(i, compress(buf[i*N:i*N+N])) 
                    for i in range(blockNum)]
        return val, len(buf)

    def unBlockifyObject(self, blocks):
        s = ''.join(decompress(b.data) for b in blocks)
        if s[0] == '0':
            return marshal.loads(s[1:])
        else:
            return cPickle.loads(s[1:])
   
    @classmethod
    def initialize(cls, is_master):
        if cls.initialized:
            return

        cls.initialized = True
        cls.is_master = is_master
        cls.host = socket.gethostname()

        logger.debug("Broadcast initialized")

    @classmethod
    def shutdown(cls):
        pass


class TreeBroadcast(Broadcast):
    guides = {}
    MaxDegree = 3
    tracker_addr = None
    tracker_thread = None

    def __init__(self, value, is_local):
        self.initializeSlaveVariables()
        Broadcast.__init__(self, value, is_local)

    def initializeSlaveVariables(self):
        self.blocks = []
        self.server_addr = None
        self.guide_addr = None

    def clear(self):
        if not self.is_local:
            self.stopServer(self.guide_addr)
            self.guide_thread.join()
            self.server_thread.join()
        else:
            Broadcast.clear(self)

    def send(self):
        logger.debug("start send %s", self.uuid)
        self.blocks, self.bytes = self.blockifyObject(self.value)
        logger.info("broadcast %s: %d bytes in %d blocks", self.uuid, 
                self.bytes, len(self.blocks))

        self.startServer()
        self.startGuide()

    def startGuide(self):
        sock = env.ctx.socket(zmq.REP)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        self.guide_addr = "tcp://%s:%d" % (self.host, port)

        self.guide_thread = spawn(self.guide, sock)
        self.guides[self.uuid] = self

    def guide(self, sock):
        logger.debug("guide start at %s", self.guide_addr)
   
        sources = {}
        listOfSources = [SourceInfo(self.server_addr)]
        while True:
            msg = sock.recv_pyobj()
            if msg == SourceInfo.Stop:
                sock.send_pyobj(0)
                break
            addr = msg
            # use the first one to recover
            if addr in sources:
                ssi = listOfSources[0]
                sock.send_pyobj(ssi)
                continue

            while True:
                ssi = self.selectSuitableSource(listOfSources, addr)
                if not ssi:
                    self.MaxDegree += 1
                    ssi = self.selectSuitableSource(listOfSources, addr)
                if ssi:
                    if not self.check_activity(ssi.addr):
                        ssi.failed = True
                    else:
                        break

            logger.debug("sending selected sourceinfo %s", ssi.addr)
            sock.send_pyobj(ssi)

            o = SourceInfo(addr)
            logger.debug("Adding possible new source to listOfSource: %s",
                o)
            sources[addr] = o
            listOfSources.append(o)
            sources[addr].parents.append(ssi)

        sock.close()
        logger.debug("Sending stop notification to %d servers ...", len(listOfSources))
        for source_info in listOfSources:
            self.stopServer(source_info.addr)
        self.guides.pop(self.uuid, None)

    def selectSuitableSource(self, listOfSources, skip):
        def parse_host(addr):
            return addr.split(':')[1][2:]
        host = parse_host(skip)
        samehost = [s for s in listOfSources
                if parse_host(s.addr) == host]
        selected = self._selectSource(samehost, skip)
        if not selected:
            selected = self._selectSource(listOfSources, skip)

        if selected:
            selected.leechers += 1
        return selected

    def _selectSource(self, sources, skip):
        for s in sources:
            if (not s.failed and s.addr != skip and not s.is_child_of(skip) and s.leechers < self.MaxDegree):
                return s

    def check_activity(self, addr):
        try:
            host, port = addr.split('://')[1].split(':')
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect((host, int(port)))
            c.close()
            return True
        except IOError, e:
            return False

    def startServer(self):
        sock = env.ctx.socket(zmq.REP)
        sock.setsockopt(zmq.LINGER, 0)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        self.server_addr = 'tcp://%s:%d' % (self.host,port)

        def run():
            logger.debug("server started at %s", self.server_addr)

            while True:
                id = sock.recv_pyobj()
                if id == SourceInfo.Stop:
                    sock.send_pyobj(0)
                    break
                sock.send_pyobj(id < len(self.blocks) and self.blocks[id] or None)

            sock.close()
            logger.debug("stop TreeBroadcast server %s", self.server_addr)

            Broadcast.clear(self) # release obj

        self.server_thread = spawn(run)

    def stopServer(self, addr):
        req = env.ctx.socket(zmq.REQ)
        req.setsockopt(zmq.LINGER, 0)
        req.connect(addr)
        req.send_pyobj(SourceInfo.Stop)
        avail = req.poll(1 * 100, zmq.POLLIN)
        if avail:
            req.recv_pyobj()
        req.close()

    def recv(self):
        self.initializeSlaveVariables()
        self.startServer()

        start = time.time()
        self.receive(self.uuid)
        value = self.unBlockifyObject(self.blocks)
        used = time.time() - start
        logger.debug("Reading Broadcasted variable %s took %ss", self.uuid, used)
        return value

    def receive(self, uuid):
        guide_addr, total_blocks = self.get_guide_addr(uuid)
        guide_sock = env.ctx.socket(zmq.REQ)
        guide_sock.connect(guide_addr)
        logger.debug("connect to guide %s", guide_addr)

        self.blocks = []
        start = time.time()
        while True:
            guide_sock.send_pyobj(self.server_addr)
            source_info = guide_sock.recv_pyobj()
            logger.debug("received SourceInfo from master: %s", 
                source_info)
            self.receiveSingleTransmission(source_info, total_blocks)
            if len(self.blocks) == total_blocks:
                break

        logger.debug("%s got broadcast in %.1fs from %s", self.server_addr, time.time() - start, source_info.addr)
        guide_sock.close()

    def receiveSingleTransmission(self, source_info, total_blocks):
        sock = env.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(source_info.addr)
        try:
            for i in range(len(self.blocks), total_blocks):
                while True:
                    sock.send_pyobj(i)
                    avail = sock.poll(10 * 1000, zmq.POLLIN) # unmarshal object will block server thread
                    if not avail:
                        logger.warning("%s recv broadcast %d from %s timeout", self.server_addr, i, source_info.addr)
                        return False
                    block = sock.recv_pyobj()
                    if block is not None:
                        break
                    # not available
                    time.sleep(0.1)

                if not isinstance(block, Block) or i != block.id:
                    logger.error("%s recv bad block %d %s", self.server_addr, i, block)
                    return False
                logger.debug("Received block: %s from %s",
                    block.id, source_info.addr)
                self.blocks.append(block)
        finally:
            sock.close()

    @classmethod
    def initialize(cls, is_master):
        Broadcast.initialize(is_master)
        sock = env.ctx.socket(zmq.REP)
        sock.setsockopt(zmq.LINGER, 0)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        cls.tracker_addr = 'tcp://%s:%d' % (cls.host, port)

        def run():
            logger.debug("TreeBroadcast tracker started at %s", 
                    cls.tracker_addr)
            while True:
                uuid = sock.recv_pyobj()
                obj = cls.guides.get(uuid)
                sock.send_pyobj((obj.guide_addr, len(obj.blocks)) if obj is not None else '')
                if not uuid:
                    break
            sock.close()
            logger.debug("TreeBroadcast tracker stopped")

        if is_master:
            cls.tracker_thread = spawn(run)
            env.register('TreeBroadcastTrackerAddr', cls.tracker_addr)
        else:
            cls.tracker_addr = env.get('TreeBroadcastTrackerAddr')

        logger.debug("TreeBroadcast initialized")

    @classmethod
    def get_guide_addr(cls, uuid):
        sock = env.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(cls.tracker_addr)
        sock.send_pyobj(uuid)
        guide_addr = sock.recv_pyobj()
        sock.close()
        return guide_addr

    @classmethod
    def shutdown(cls):
        for uuid, obj in cls.guides.items():
            obj.clear()
        if cls.tracker_thread:
            cls.get_guide_addr('')
            cls.tracker_thread.join()
        Broadcast.shutdown()

class P2PBroadcast(TreeBroadcast):
    def guide(self, sock):
        sources = {self.server_addr: ([1] * len(self.blocks))}
        bad_servers = []
        last_check = 0
        while True:
            msg = sock.recv_pyobj()
            if msg == SourceInfo.Stop:
                sock.send_pyobj(0)
                break

            now = time.time()
            if last_check + 10 < now:
                for addr in sources.keys():
                    if addr != self.server_addr and not self.check_activity(addr):
                        del sources[addr]
                        bad_servers.append(addr)
                last_check = now

            sock.send_pyobj(sources)
            addr, bitmap = msg
            if not addr or not isinstance(addr, str) or not addr.startswith('tcp://'):
                logger.error('invalid server addr: %s', addr)
                continue
            if any(bitmap):
                sources[addr] = bitmap

        sock.close()
        logger.debug("Sending stop notification to %d servers ...", len(sources))
        for addr in sources:
            self.stopServer(addr)
        self.sources = {}
        for addr in bad_servers:
            self.stopServer(addr)
        self.guides.pop(self.uuid, None)

    def receive(self, uuid):
        r = self.get_guide_addr(uuid)
        assert r, 'broadcast guide has shutdown'
        guide_addr, total_blocks = r
        guide_sock = env.ctx.socket(zmq.REQ)
        guide_sock.connect(guide_addr)
        logger.debug("connect to guide %s", guide_addr)

        self.blocks = [None] * total_blocks
        self.bitmap = ([0] * total_blocks)
        start = time.time()
        hostname = socket.gethostname()
        random.seed(os.getpid() + int(start*1000)%1000)

        while not self.stopped:
            guide_sock.send_pyobj((self.server_addr, self.bitmap))
            source_infos = guide_sock.recv_pyobj()
            if all(self.bitmap):
                break
            logger.debug("received SourceInfo from master: %s", source_infos.keys()) 
            self.receive_from_local(dict((k,v) for k,v in source_infos.iteritems() if hostname in k))
            self.receive_one(source_infos)
 
        if self.stopped:
            os._exit(0)

        logger.debug("%s got broadcast in %.1fs", self.server_addr, time.time() - start)
        guide_sock.close()

    def receive_from_local(self, sources):
        poller = zmq.Poller()
        socks = []
        addrs = {}
        for addr in sources:
            i = self.peek(sources[addr])
            if i is None: continue
            self.bitmap[i] = 1
            sock = env.ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(addr)
            sock.send_pyobj(i)
            poller.register(sock, zmq.POLLIN)
            socks.append(sock)
            addrs[sock] = addr

        while socks:
            t = time.time()
            avail = poller.poll(5 * 1000) # unmarshal object will block server thread
            if not avail:
                break
            for sock, _ in avail:
                block = sock.recv_pyobj()
                if block is not None and isinstance(block, Block):
                    self.blocks[block.id] = block
                    logger.debug("Received block: %s from %s", block.id, addrs[sock])
                i = self.peek(sources[addrs[sock]]) 
                if i is not None:
                    self.bitmap[i] = 1
                    sock.send_pyobj(i)
                else:
                    poller.unregister(sock)
                    socks.remove(sock)
                    sock.close()

        # rebuild bitmap 
        self.bitmap = ([bool(b) for b in self.blocks])
        timeout_servers = [addrs.get(sock) for sock in socks]
        if timeout_servers:
            logger.debug("recv from %s timeout", timeout_servers)


    def receive_one(self, sources):
        host = socket.gethostname()
        sources = sources.items() #sorted(sources.items(), key=lambda (k,b):len(b))
        random.shuffle(sources)

        poller = zmq.Poller()
        socks = []
        addrs = {}
        hosts = set()
        for addr, bitmap in sources:
            i = self.peek(bitmap)
            if i is None: continue
            self.bitmap[i] = 1

            host = addr.split(':')[1][5:]
            if host in hosts: continue
            hosts.add(host)

            sock = env.ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(addr)
            addrs[sock] = addr
            sock.send_pyobj(i)
            poller.register(sock, zmq.POLLIN)
            socks.append(sock)

        while socks:
            t = time.time()
            avail = poller.poll(5 * 1000) # unmarshal object will block server thread
            if not avail:
                break
            for sock, _ in avail:
                block = sock.recv_pyobj()
                if block is not None and isinstance(block, Block):
                    self.blocks[block.id] = block
                    logger.debug("Received block: %s from %s", block.id, addrs.get(sock))
                poller.unregister(sock)
                socks.remove(sock)
                sock.close()

        # rebuild bitmap 
        self.bitmap = [bool(b) for b in self.blocks]
        timeout_servers = [addrs.get(sock) for sock in socks]
        if timeout_servers:
            logger.debug("recv from %s timeout", timeout_servers)

    def peek(self, bitmap):
        avails = [i for i in range(len(bitmap)) if not self.bitmap[i] and bitmap[i]]
        if avails:
            return random.choice(avails)

TheBroadcast = P2PBroadcast

def _test_init():
    TheBroadcast.initialize(False)

def _test_in_process(v):
    assert v.value[0] == 0
    assert len(v.value) == 1000*1000

if __name__ == '__main__':
    import logging
    logging.basicConfig(
        format="%(process)d:%(threadName)s:%(levelname)s %(message)s",
        level=logging.DEBUG)
    TheBroadcast.initialize(True)
    import multiprocessing
    from dpark.env import env
    pool = multiprocessing.Pool(4, _test_init)

    v = range(1000*1000)
    b = TreeBroadcast(v, False)
    b = cPickle.loads(cPickle.dumps(b, -1))
    assert len(b.value) == len(v), b.value

    for i in range(10):
        pool.apply_async(_test_in_process, [b])
    time.sleep(3)

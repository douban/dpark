import os, time
import uuid
import socket
import marshal
import cPickle
import logging
import gc

import zmq

from util import compress, decompress, getproctitle, setproctitle, spawn
import cache
from serialize import marshalable
from env import env

logger = logging.getLogger("broadcast")

class SourceInfo:
    Stop = -2

    def __init__(self, addr, total_blocks=0, total_bytes=0, block_size=0):
        self.addr = addr
        self.total_blocks = total_blocks
        self.total_bytes = total_bytes
        self.block_size = block_size

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

class VariableInfo:
    def __init__(self, blocks, total_blocks, total_bytes):
        self.blocks = blocks
        self.total_blocks = total_blocks
        self.total_bytes = total_bytes
        self.has_blocks = 0

class Broadcast(object):
    initialized = False
    is_master = False
    cache = cache.Cache() 
    broadcastFactory = None
    BlockSize = 1024 * 1024
    MaxRetryCount = 2
    MinKnockInterval = 500
    MaxKnockInterval = 999
        
    def __init__(self, value, is_local):
        assert value is not None, 'broadcast object should not been None'
        self.uuid = str(uuid.uuid4())
        self.value = value
        self.is_local = is_local
        if is_local:
            if not self.cache.put(self.uuid, value):
                raise Exception('object %s is too big to cache', repr(value))
        else:
            self.send()

    def clear(self):
        self.cache.put(self.uuid, None)
        self.value = None

    def __getstate__(self):
        return (self.uuid, self.bytes, self.value if self.bytes < self.BlockSize/2 else None)

    def __setstate__(self, v):
        self.uuid, self.bytes, value = v
        if value is not None:
            self.value = value
    
    def __getattr__(self, name):
        if name != 'value':
            raise AttributeError(name)

        # in the executor process, Broadcast is not initialized
        if not self.initialized:
            raise AttributeError(name)

        uuid = self.uuid
        self.value = self.cache.get(uuid)
        if self.value is not None:
            return self.value    
        
        oldtitle = getproctitle()
        setproctitle('dpark worker: broadcasting ' + uuid)

        self.recv()
        if self.value is None:
            raise Exception("recv broadcast failed")
        self.cache.put(uuid, self.value)

        setproctitle(oldtitle)
        return self.value                
                
    def send(self):
        raise NotImplementedError

    def recv(self):
        raise NotImplementedError

    def blockifyObject(self, obj):
        try:
            buf = marshal.dumps(obj)
        except Exception:
            buf = cPickle.dumps(obj, -1)

        N = self.BlockSize
        blockNum = len(buf) / N + 1
        val = [Block(i, compress(buf[i*N:i*N+N])) 
                    for i in range(blockNum)]
        vi = VariableInfo(val, blockNum, len(buf))
        vi.has_blocks = blockNum
        return vi

    def unBlockifyObject(self, blocks):
        s = ''.join(decompress(b.data) for b in blocks)
        try:
            return marshal.loads(s)
        except Exception :
            return cPickle.loads(s)
   
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


class FileBroadcast(Broadcast):
    workdir = None
    @property
    def path(self):
        return os.path.join(self.workdir, self.uuid)

    def send(self):
        try:
            d = marshal.dumps(self.value)
        except Exception:
            d = cPickle.dumps(self.value, -1)
        d = compress(d)
        self.bytes = len(d)
        f = open(self.path, 'wb')
        f.write(d)
        f.close()
        logger.debug("dump to %s", self.path)

    def recv(self):
        d = decompress(open(self.path, 'rb').read())
        try:
            self.value = marshal.loads(d)
        except Exception:
            self.value = cPickle.loads(d)
        logger.debug("load from %s", self.path)

    def clear(self):
        Broadcast.clear(self)
        if os.path.exists(self.path):
            os.remove(self.path)

    @classmethod
    def initialize(cls, is_master):
        Broadcast.initialize(is_master)
        cls.workdir = env.get('WORKDIR')[0]
        logger.debug("FileBroadcast initialized")


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
        self.total_bytes = -1
        self.total_blocks = -1
        self.block_size = self.BlockSize

        self.listOfSources = []
        self.server_addr = None
        self.guide_addr = None

    def clear(self):
        Broadcast.clear(self)
        if not self.is_local:
            self.stopServer(self.guide_addr)
            self.server_thread.join()
            self.guide_thread.join()

    def send(self):
        logger.debug("start send %s", self.uuid)
        variableInfo = self.blockifyObject(self.value)
        self.blocks = variableInfo.blocks
        self.total_bytes = variableInfo.total_bytes
        self.total_blocks = variableInfo.total_blocks
        logger.info("broadcast %s: %d bytes in %d blocks", self.uuid, 
                self.total_bytes, self.total_blocks)
        self.bytes = self.total_bytes

        self.startGuide()
        self.startServer()
    
    def startGuide(self):
        sock = env.ctx.socket(zmq.REP)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        self.guide_addr = "tcp://%s:%d" % (self.host, port)
        
        def run():
            logger.debug("guide start at %s", self.guide_addr)
           
            sources = {}
            while True:
                msg = sock.recv_pyobj()
                if msg == SourceInfo.Stop:
                    sock.send_pyobj(0)
                    break
                addr = msg
                # use the first one to recover
                if addr in sources:
                    ssi = self.listOfSources[0]
                    sock.send_pyobj(ssi)
                    continue

                while True:
                    ssi = self.selectSuitableSource(addr)
                    if not ssi:
                        self.MaxDegree += 1
                        ssi = self.selectSuitableSource(addr)
                    if ssi:
                        if not self.check_activity(ssi):
                            ssi.failed = True
                        else:
                            break

                logger.debug("sending selected sourceinfo %s", ssi.addr)
                sock.send_pyobj(ssi)
                
                o = SourceInfo(addr, self.total_blocks,
                    self.total_bytes, self.block_size)
                logger.debug("Adding possible new source to listOfSource: %s",
                    o)
                sources[addr] = o
                self.listOfSources.append(o)
                sources[addr].parents.append(ssi)                

            sock.close()
            logger.debug("Sending stop notification to %d servers ...", len(self.listOfSources))
            for source_info in self.listOfSources:
                self.stopServer(source_info.addr)
            self.listOfSources = []
            self.unregisterValue(self.uuid)

        self.guide_thread = spawn(run)
        self.registerValue(self.uuid, self)
        logger.debug("guide started...")

    def selectSuitableSource(self, skip):
        def parse_host(addr):
            return addr.split(':')[1][2:]
        host = parse_host(skip)
        samehost = [s for s in self.listOfSources
                if parse_host(s.addr) == host]
        selected = self._selectSource(samehost, skip)
        if not selected:
            selected = self._selectSource(self.listOfSources, skip)

        if selected:
            selected.leechers += 1
        return selected
    
    def _selectSource(self, sources, skip):
        for s in sources:
            if (not s.failed and s.addr != skip and not s.is_child_of(skip) and s.leechers < self.MaxDegree):
                return s

    def check_activity(self, source_info):
        try:
            host, port = source_info.addr.split('://')[1].split(':')
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
                msg = sock.recv_pyobj()
                if msg == SourceInfo.Stop:
                    sock.send_pyobj(0)
                    break
                id = msg
                sock.send_pyobj(id < len(self.blocks) and self.blocks[id] or None)

            sock.close()
            logger.debug("stop TreeBroadcast server %s", self.server_addr)

            # clear cache
            self.cache.put(self.uuid, None)
            self.blocks = []
            self.value = None

        self.server_thread = spawn(run)
        self.listOfSources = [SourceInfo(self.server_addr, 
            self.total_blocks, self.total_bytes,
            self.block_size)]

    def stopServer(self, addr):
        req = env.ctx.socket(zmq.REQ)
        req.setsockopt(zmq.LINGER, 0)
        req.connect(addr)
        req.send_pyobj(SourceInfo.Stop)
        poller = zmq.Poller()
        poller.register(req, zmq.POLLIN)
        avail = dict(poller.poll(1 * 1000))
        if avail and avail.get(req) == zmq.POLLIN:
            req.recv_pyobj()
        poller.unregister(req)
        req.close()

    def recv(self):
        self.initializeSlaveVariables()
                
        self.startServer()

        start = time.time()
        self.receive(self.uuid)
        self.value = self.unBlockifyObject(self.blocks)
        used = time.time() - start
        logger.debug("Reading Broadcasted variable %s took %ss", self.uuid, used)

    def receive(self, uuid):
        guide_addr = self.get_guide_addr(uuid)
        assert guide_addr, 'guide addr is not available'
        guide_sock = env.ctx.socket(zmq.REQ)
        guide_sock.connect(guide_addr)
        logger.debug("connect to guide %s", guide_addr)

        self.blocks = []
        start = time.time()
        for i in range(10):
            guide_sock.send_pyobj(self.server_addr)
            source_info = guide_sock.recv_pyobj()

            self.total_blocks = source_info.total_blocks
            self.total_bytes = source_info.total_bytes
            logger.debug("received SourceInfo from master: %s", 
                source_info)
            if self.receiveSingleTransmission(source_info):
                break
        else:
            raise Exception("receiveSingleTransmission failed")
        
        logger.debug("%s got broadcast in %.1fs from %s", self.server_addr, time.time() - start, source_info.addr)

#        guide_sock.send_pyobj(source_info)
#        guide_sock.recv_pyobj()
        guide_sock.close()

        return len(self.blocks) == self.total_blocks

    def receiveSingleTransmission(self, source_info):
        logger.debug("Inside receiveSingleTransmission")
        logger.debug("total_blocks: %s has %s", self.total_blocks,
                len(self.blocks))
        sock = env.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(source_info.addr)
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        try:
            for i in range(len(self.blocks), source_info.total_blocks):
                while True:
                    sock.send_pyobj(i)
                    avail = dict(poller.poll(3 * 1000))
                    if not avail or avail.get(sock) != zmq.POLLIN:
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
            poller.unregister(sock)
            sock.close()

        return len(self.blocks) == source_info.total_blocks

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
                obj = cls.guides.get(uuid, '')
                sock.send_pyobj(obj and obj.guide_addr)
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

    @classmethod
    def registerValue(cls, uuid, obj):
        cls.guides[uuid] = obj
        logger.debug("New value registered with the Tracker %s, %s", uuid, obj.guide_addr) 

    @classmethod
    def unregisterValue(cls, uuid):
        obj = cls.guides.pop(uuid, None)
        if obj:
            logger.debug("value unregistered from Tracker %s, %s", uuid, obj.guide_addr) 

TheBroadcast = TreeBroadcast

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
    from env import env
    pool = multiprocessing.Pool(4, _test_init)

    v = range(1000*1000)
    b = TreeBroadcast(v, False)
    b = cPickle.loads(cPickle.dumps(b, -1))
    assert len(b.value) == len(v), b.value

    for i in range(10):
        pool.apply_async(_test_in_process, [b])
    time.sleep(3)

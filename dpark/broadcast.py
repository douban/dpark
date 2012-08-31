import os, time
import uuid
import socket
import marshal
import cPickle
import threading
import logging
import zlib
import gzip
from multiprocessing import Lock
try:
    from setproctitle import getproctitle, setproctitle
except ImportError:
    def getproctitle():
        return ''
    def setproctitle(x):
        pass

import zmq

import cache
from serialize import marshalable
from env import env

logger = logging.getLogger("broadcast")

class SourceInfo:
    StopBroadcast = -2

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

class BroadcastBlock:
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
    ever_used = False
    is_master = False
    cache = cache.Cache() 
    broadcastFactory = None
    BlockSize = 1024 * 1024
    MaxRetryCount = 2
    MinKnockInterval = 500
    MaxKnockInterval = 999
        
    def __init__(self, value, is_local):
        assert value is not None, 'broadcast object should not been None'
        Broadcast.ever_used = True
        self.uuid = str(uuid.uuid4())
        self.value = value
        self.is_local = is_local
        if is_local:
            if not self.cache.put(self.uuid, value):
                raise Exception('object %s is too big to cache', repr(value))
        else:
            self.sendBroadcast()

    def clear(self):
        self.cache.put(self.uuid, None)

    def __getstate__(self):
        return (self.uuid, self.bytes, self.value if self.bytes < self.BlockSize/2 else None)

    def __setstate__(self, v):
        self.uuid, self.bytes, value = v
        if value is not None:
            self.value = value
        Broadcast.ever_used = True
    
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

        self.recvBroadcast()
        if self.value is None:
            raise Exception("recv broadcast failed")
        self.cache.put(uuid, self.value)

        setproctitle(oldtitle)
        return self.value                
                
    def sendBroadcast(self):
        raise NotImplementedError

    def recvBroadcast(self):
        raise NotImplementedError

    def blockifyObject(self, obj):
        if marshalable(obj):
            buf = marshal.dumps(obj)
        else:
            buf = cPickle.dumps(obj, -1)
        buf = zlib.compress(buf, 1)
        N = self.BlockSize
        blockNum = len(buf) / N
        if len(buf) % N != 0:
            blockNum += 1
        val = [BroadcastBlock(i/N, buf[i:i+N]) 
                    for i in range(0, len(buf), N)]
        vi = VariableInfo(val, blockNum, len(buf))
        vi.has_blocks = blockNum
        return vi

    def unBlockifyObject(self, blocks):
        z = zlib.decompressobj()
        s = ''.join([z.decompress(b.data) for b in blocks] + [z.flush()])
        try:
            return marshal.loads(s)
        except ValueError:
            return cPickle.loads(s)
   
    @classmethod
    def initialize(cls, is_master):
        if cls.initialized:
            return

        cls.is_master = is_master
        cls.host = socket.gethostname()

#        cls.broadcastFactory = FileBroadcastFactory()
        cls.broadcastFactory = TreeBroadcastFactory()
        cls.broadcastFactory.initialize(is_master)
        cls.initialized = True
        logger.debug("Broadcast initialized")

    @classmethod
    def getBroadcastFactory(cls):
        return cls.broadcastFactory

    @classmethod
    def newBroadcast(cls, value, is_local):
        return cls.broadcastFactory.newBroadcast(value, is_local)

class BroadcastFactory(object):
    def initialize(self, is_master):
        raise NotImplementedError
    def newBroadcast(self, value, is_local):
        raise NotImplementedError

class FileBroadcast(Broadcast):
    workdir = None
    @property
    def path(self):
        return os.path.join(self.workdir, self.uuid)

    def sendBroadcast(self):
        f = gzip.open(self.path, 'wb')
        if marshalable(self.value):
            marshal.dump(self.value, f)
        else:
            cPickle.dump(self.value, f, -1)
        f.flush()
        self.bytes = f.tell()
        f.close()
        logger.debug("dump to %s", self.path)

    def recvBroadcast(self):
        try:
            self.value = marshal.load(gzip.open(self.path, 'rb'))
        except ValueError:
            self.value = cPickle.load(gzip.open(self.path, 'rb'))
        logger.debug("load from %s", self.path)

    def clear(self):
        Broadcast.clear(self)
        os.remove(self.path())

    @classmethod
    def initialize(cls, is_master):
        cls.workdir = env.get('WORKDIR')
        logger.debug("FileBroadcast initialized")

class FileBroadcastFactory(BroadcastFactory):
    def initialize(self, is_master):
        return FileBroadcast.initialize(is_master)
    def newBroadcast(self, value, is_local):
        return FileBroadcast(value, is_local)

class TreeBroadcast(FileBroadcast):
    guides = {}
    MaxDegree = 3
    master_addr = None

    def __init__(self, value, is_local):
        self.initializeSlaveVariables()
        Broadcast.__init__(self, value, is_local)

    def initializeSlaveVariables(self):    
        self.blocks = []
        self.total_bytes = -1
        self.total_blocks = -1
        self.block_size = self.BlockSize

        self.listOfSources = []
        self.serverAddr = None
        self.guide_addr = None

    def clear(self):
        Broadcast.clear(self)
        if not self.is_local:
            self.stopServer(self.guide_addr)
            self.stopServer(self.serverAddr)
            self.listOfSources = []
            self.blocks = []

    def sendBroadcast(self):
        logger.debug("start sendBroadcast %s", self.uuid)
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
        def run():
            sock = env.ctx.socket(zmq.REP)
            port = sock.bind_to_random_port("tcp://0.0.0.0")
            self.guide_addr = "tcp://%s:%d" % (self.host, port)
            logger.debug("guide start at %s", self.guide_addr)
           
            sources = {}
            while True:
                addr = sock.recv_pyobj()
                if addr == SourceInfo.StopBroadcast:
                    break
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
            logger.debug("Sending stop notification ...")

            for source_info in self.listOfSources:
                self.stopServer(source_info.addr)
            self.unregisterValue(self.uuid)

        t = threading.Thread(target=run)
        t.daemon = True
        t.start()
        # wait for guide to start
        while self.guide_addr is None:
            time.sleep(0.01)
        self.registerValue(self.uuid, self.guide_addr)
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
        def run():
            sock = env.ctx.socket(zmq.REP)
            sock.setsockopt(zmq.LINGER, 0)
            port = sock.bind_to_random_port("tcp://0.0.0.0")
            self.serverAddr = 'tcp://%s:%d' % (self.host,port)
            logger.debug("server started at %s", self.serverAddr)

            while True:
                id = sock.recv_pyobj()
                if id == SourceInfo.StopBroadcast:
                    break
                sock.send_pyobj(id < len(self.blocks) and self.blocks[id] or None)

            sock.close()
            logger.debug("stop TreeBroadcast server %s", self.serverAddr)

            # clear cache
            self.cache.put(self.uuid, None)

        t = threading.Thread(target=run)
        t.daemon = True
        t.start()
        while self.serverAddr is None:
            time.sleep(0.01)
        #logger.debug("server started...")
        self.listOfSources = [SourceInfo(self.serverAddr, 
            self.total_blocks, self.total_bytes,
            self.block_size)]

    def stopServer(self, addr):
        req = env.ctx.socket(zmq.REQ)
        req.setsockopt(zmq.LINGER, 0)
        req.connect(addr)
        req.send_pyobj(SourceInfo.StopBroadcast)
        #req.recv_pyobj()
        req.close()

    def recvBroadcast(self):
        self.initializeSlaveVariables()
                
        self.startServer()

        start = time.time()
        self.receiveBroadcast(self.uuid)
        self.value = self.unBlockifyObject(self.blocks)
        used = time.time() - start
        logger.debug("Reading Broadcasted variable %s took %ss", self.uuid, used)

    def receiveBroadcast(self, uuid):
        master_addr = self.getMasterAddr(uuid)
        guide_sock = env.ctx.socket(zmq.REQ)
        guide_sock.connect(master_addr)
        logger.debug("connect to guide %s", master_addr)

        self.blocks = []
        start = time.time()
        for i in range(10):
            guide_sock.send_pyobj(self.serverAddr)
            source_info = guide_sock.recv_pyobj()

            self.total_blocks = source_info.total_blocks
            self.total_bytes = source_info.total_bytes
            logger.debug("received SourceInfo from master: %s", 
                source_info)
            if self.receiveSingleTransmission(source_info):
                break
        else:
            raise Exception("receiveSingleTransmission failed")
        
        logger.debug("%s got broadcast in %.1fs from %s", self.serverAddr, time.time() - start, source_info.addr)

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
        for i in range(len(self.blocks), source_info.total_blocks):
            while True:
                sock.send_pyobj(i)
                avail = dict(poller.poll(3 * 1000))
                if not avail or avail.get(sock) != zmq.POLLIN:
                    logger.warning("%s recv broadcast %d from %s timeout", self.serverAddr, i, source_info.addr)
                    poller.unregister(sock)
                    sock.close()    
                    return False
                block = sock.recv_pyobj()
                if block is not None:
                    break
                # not available
                time.sleep(0.1)

            if not isinstance(block, BroadcastBlock) or i != block.id:
                logger.error("%s recv bad block %d %s", self.serverAddr, i, block)
                poller.unregister(sock)
                sock.close()    
                return False
            logger.debug("Received block: %s from %s", 
                block.id, source_info.addr)
            self.blocks.append(block)
        poller.unregister(sock)
        sock.close()    
        return len(self.blocks) == source_info.total_blocks

    def getMasterAddr(self, uuid):
        sock = env.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(self.master_addr)
        sock.send_pyobj(uuid)
        guide_addr = sock.recv_pyobj()
        sock.close()
        return guide_addr

    @classmethod
    def initialize(cls, is_master):

        FileBroadcast.initialize(is_master)

        def run():
            sock = env.ctx.socket(zmq.REP)
            sock.setsockopt(zmq.LINGER, 0)
            port = sock.bind_to_random_port("tcp://0.0.0.0")
            cls.master_addr = 'tcp://%s:%d' % (cls.host, port)
            logger.debug("TreeBroadcast tracker started at %s", 
                    cls.master_addr)
            while True:
                uuid = sock.recv_pyobj()
                guide = cls.guides.get(uuid, '')
                if not guide:
                    logger.warning("broadcast %s is not registered", uuid)
                sock.send_pyobj(guide)
            sock.close()
            logger.debug("TreeBroadcast tracker stopped")

        if is_master:
            t = threading.Thread(target=run)
            t.daemon = True
            t.start()
            while cls.master_addr is None:
                time.sleep(0.01)
            env.register('TreeBroadcastTrackerAddr', cls.master_addr)
        else:
            cls.master_addr = env.get('TreeBroadcastTrackerAddr')
            
        logger.debug("TreeBroadcast initialized")

    @classmethod
    def registerValue(cls, uuid, guide_addr):
        cls.guides[uuid] = guide_addr
        logger.debug("New value registered with the Tracker %s, %s", uuid, guide_addr) 

    @classmethod
    def unregisterValue(cls, uuid):
        guide_addr = cls.guides.pop(uuid, None)
        logger.debug("value unregistered from Tracker %s, %s", uuid, guide_addr) 

class TreeBroadcastFactory(BroadcastFactory):
    def initialize(self, is_master):
        return TreeBroadcast.initialize(is_master)
    def newBroadcast(self, value, is_local):
        return TreeBroadcast(value, is_local)

def _test_init():
    Broadcast.initialize(False)

def _test_in_process(v):
    assert v.value[0] == 0
    assert len(v.value) == 1000*1000

if __name__ == '__main__':
    import logging
    logging.basicConfig(
        format="%(process)d:%(threadName)s:%(levelname)s %(message)s",
        level=logging.DEBUG)
    Broadcast.initialize(True)
    import multiprocessing
    from env import env
    pool = multiprocessing.Pool(4, _test_init)

    v = range(1000*1000)
    b = Broadcast.newBroadcast(v, False)
    b = cPickle.loads(cPickle.dumps(b, -1))
    assert len(b.value) == len(v), b.value

    for i in range(10):
        pool.apply_async(_test_in_process, [b])
    time.sleep(3)

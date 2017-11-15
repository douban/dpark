from __future__ import absolute_import
import os
import zmq
import uuid
import binascii
import random
import socket
import struct
import six.moves.cPickle
import marshal
import mmap
from multiprocessing import Manager, Condition
from mmap import ACCESS_WRITE, ACCESS_READ

from dpark.util import compress, decompress, spawn, get_logger
from dpark.cache import Cache
from dpark.serialize import marshalable
from dpark.env import env
import six
from six.moves import range, map
try:
    from itertools import izip
except ImportError:
    izip = zip

logger = get_logger(__name__)

MARSHAL_TYPE, PICKLE_TYPE = list(range(2))
BLOCK_SHIFT = 20
BLOCK_SIZE = 1 << BLOCK_SHIFT
GUIDE_ADDR = 'NewBroadcastGuideAddr'
DOWNLOAD_ADDR = 'NewDownloadAddr'
BATCHED_BLOCKS = 3
GUIDE_STOP, GUIDE_GET_SOURCES, GUIDE_SET_SOURCES, GUIDE_REPORT_BAD = list(range(4))
SERVER_STOP, SERVER_FETCH, SERVER_FETCH_FAIL, SERVER_FETCH_OK, \
    DATA_GET, DATA_GET_OK, DATA_GET_FAIL, DATA_DOWNLOADING, SERVER_CLEAR_ITEM, \
    REGISTER_BLOCKS, REGISTER_BLOCKS_OK, REGISTER_BLOCKS_FAILED = list(range(12))


class GuideManager:
    def __init__(self):
        self.guides = {}
        self.host = socket.gethostname()
        self.guide_addr = None
        self.guide_thread = None
        self.register_addr = {}
        self.ctx = None

    def start(self):
        self.ctx = zmq.Context()
        self.guide_addr, self.guide_thread = self.start_guide()
        env.register(GUIDE_ADDR, self.guide_addr)

    def start_guide(self):
        sock = self.ctx.socket(zmq.REP)
        port = sock.bind_to_random_port('tcp://0.0.0.0')
        guide_addr = 'tcp://%s:%d' % (self.host, port)

        def run():
            logger.debug("guide start at %s", guide_addr)

            while True:
                type, msg = sock.recv_pyobj()
                if type == GUIDE_STOP:
                    sock.send_pyobj(0)
                    break
                elif type == GUIDE_GET_SOURCES:
                    uuid = msg
                    sources = None
                    if uuid in self.guides:
                        sources = self.guides[uuid]
                    else:
                        logger.warning('uuid %s NOT REGISTERED in guide server', uuid)
                    sock.send_pyobj(sources)
                elif type == GUIDE_SET_SOURCES:
                    uuid, addr, bitmap = msg
                    if any(bitmap):
                        sources = None
                        if uuid in self.guides:
                            sources = self.guides[uuid]
                        if sources:
                            sources[addr] = bitmap
                        else:
                            self.guides[uuid] = {addr: bitmap}
                            self.register_addr[uuid] = addr
                    sock.send_pyobj(None)
                elif type == GUIDE_REPORT_BAD:
                    uuid, addr = msg
                    sources = self.guides[uuid]
                    if addr in sources:
                        if addr != self.register_addr[uuid]:
                            del sources[addr]
                        else:
                            logger.warning('The addr %s to delete is the register Quit!!!', addr)
                    sock.send_pyobj(None)
                else:
                    logger.error('Unknown guide message: %s %s', type, msg)
            sock.close()

        return guide_addr, spawn(run)

    def shutdown(self):
        if self.guide_thread and self.guide_addr.\
                startswith('tcp://%s:' % socket.gethostname()):
            sock = self.ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(self.guide_addr)
            sock.send_pyobj((GUIDE_STOP, None))
            sock.recv_pyobj()
            sock.close()
            self.guide_thread.join()

download_cond = None
shared_uuid_fn_dict = None
shared_uuid_map_dict = None


def check_memory(location):
    try:
        import psutil
        pid = os.getpid()
        p = psutil.Process(pid)
        rss = p.memory_info().rss >> 20
        logger.info('memory rss %d MB in host %s at ',
                    rss, socket.gethostname(), location)
    except ImportError:
        logger.warning('import psutil failed')


def init_dict():
    global download_cond, shared_uuid_fn_dict, shared_uuid_map_dict
    manager = Manager()
    shared_uuid_fn_dict = manager.dict()
    shared_uuid_map_dict = manager.dict()
    download_cond = Condition()


def decide_dir(work_dirs):
    return work_dirs[-1]


def gen_broadcast_path(work_dirs, uuid):
    work_dir = decide_dir(work_dirs)
    broadcast_dir = os.path.join(work_dir, 'broadcast')
    if not os.path.exists(broadcast_dir):
        os.makedirs(broadcast_dir)
    uuid_path = '%s_%d' % (uuid, os.getpid())
    broadcast_path = os.path.join(broadcast_dir, uuid_path)
    return broadcast_path


class DownloadManager:
    def __init__(self):
        self.server_thread = None
        self.download_threads = {}
        self.uuid_state_dict = None
        self.shared_uuid_fn_dict = None
        self.shared_uuid_map_dict = None
        self.uuid_map_dict = None
        self.guide_addr = None
        self.server_addr = None
        self.host = None
        self.ctx = None
        self.random_inst = None
        self.work_dirs = []

    def start(self):
        global shared_uuid_fn_dict, shared_uuid_map_dict
        self.ctx = zmq.Context()
        self.host = socket.gethostname()
        self.guide_addr = env.get(GUIDE_ADDR)
        self.random_inst = random.SystemRandom()
        self.server_addr, self.server_thread = self.start_server()
        self.uuid_state_dict = {}
        self.shared_uuid_fn_dict = shared_uuid_fn_dict
        self.shared_uuid_map_dict = shared_uuid_map_dict
        self.uuid_map_dict = {}
        self.work_dirs = env.get('WORKDIR')
        env.register(DOWNLOAD_ADDR, self.server_addr)

    def start_server(self):
        sock = self.ctx.socket(zmq.REP)
        sock.setsockopt(zmq.LINGER, 0)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        server_addr = 'tcp://%s:%d' % (self.host, port)
        guide_sock = self.ctx.socket(zmq.REQ)
        guide_sock.setsockopt(zmq.LINGER, 0)
        guide_sock.connect(self.guide_addr)

        def run():
            logger.debug("server started at %s", server_addr)

            while True:
                type, msg = sock.recv_pyobj()
                logger.debug('server recv: %s %s', type, msg)
                if type == SERVER_STOP:
                    sock.send_pyobj(None)
                    break
                elif type == SERVER_FETCH:
                    uuid, indices, client_addr = msg
                    if uuid in self.uuid_state_dict:
                        fd = os.open(self.uuid_state_dict[uuid][0], os.O_RDONLY)
                        mmfp = mmap.mmap(fd, 0, access=ACCESS_READ)
                        os.close(fd)
                        bitmap = self.uuid_map_dict[uuid]
                        block_num = len(bitmap)
                        bls = []
                        for index in indices:
                            if index >= block_num:
                                logger.warning('input index too big %s for '
                                               'len of blocks  %d from host %s',
                                               str(indices), block_num, client_addr)
                                sock.send_pyobj((SERVER_FETCH_FAIL, None))
                            else:
                                mmfp.seek(bitmap[index][0])
                                block = mmfp.read(bitmap[index][1])
                                bls.append(block)
                        mmfp.close()
                        sock.send_pyobj((SERVER_FETCH_OK, (indices, bls)))
                    else:
                        logger.warning('server fetch failed for uuid %s '
                                       'not exists in server %s from host %s',
                                       uuid, socket.gethostname(), client_addr)
                        sock.send_pyobj((SERVER_FETCH_FAIL, None))
                elif type == DATA_GET:
                    uuid, compressed_size = msg
                    if uuid not in self.uuid_state_dict or not self.uuid_state_dict[uuid][1]:
                        if uuid not in self.download_threads:
                            sources = self._get_sources(uuid, guide_sock)
                            if not sources:
                                logger.warning('get sources from guide server failed in host %s',
                                               socket.gethostname())
                                sock.send_pyobj(DATA_GET_FAIL)
                                continue
                            self.download_threads[uuid] = spawn(self._download_blocks,
                                                                *[sources, uuid, compressed_size,
                                                                  guide_sock])
                            sock.send_pyobj(DATA_DOWNLOADING)
                        else:
                            sock.send_pyobj(DATA_DOWNLOADING)
                    else:
                        sock.send_pyobj(DATA_GET_OK)
                elif type == SERVER_CLEAR_ITEM:
                    uuid = msg
                    self.clear(uuid)
                    sock.send_pyobj(None)
                elif type == REGISTER_BLOCKS:
                    uuid, broadcast_path, block_map = msg
                    self.uuid_state_dict[uuid] = broadcast_path, True
                    self.shared_uuid_map_dict[uuid] = block_map
                    self.shared_uuid_fn_dict[uuid] = broadcast_path
                    self.uuid_map_dict[uuid] = block_map
                    sock.send_pyobj(REGISTER_BLOCKS_OK)
                else:
                    logger.error('Unknown server message: %s %s', type, msg)

            sock.close()
            logger.debug("stop Broadcast server %s", server_addr)
            for uuid in list(self.uuid_state_dict.keys()):
                self.clear(uuid)

        return server_addr, spawn(run)

    def _get_sources(self, uuid, guide_sock):
        try:
            guide_sock.send_pyobj((GUIDE_GET_SOURCES,
                                   uuid))
            sources = guide_sock.recv_pyobj()
        except:
            logger.warning('GET sources failed for addr %s with ZMQ ERR',
                           self.server_addr)
            sources = {}
        return sources

    def _update_sources(self, uuid, bitmap, guide_sock):
        try:
            guide_sock.send_pyobj((GUIDE_SET_SOURCES,
                                   (uuid, self.server_addr, bitmap)))
            guide_sock.recv_pyobj()
        except:
            pass

    def _download_blocks(self, sources, uuid, compressed_size, guide_sock):
        block_num = 0
        bitmap = [0]
        write_mmap_handler = None

        def _report_bad(addr):
            logger.debug('fetch blocks failed from server %s', addr)
            guide_sock.send_pyobj((GUIDE_REPORT_BAD, (uuid, addr)))
            guide_sock.recv_pyobj()

        def _fetch(addr, indices, bit_map):
            sock = self.ctx.socket(zmq.REQ)
            try:
                sock.setsockopt(zmq.LINGER, 0)
                sock.connect(addr)
                sock.send_pyobj((SERVER_FETCH, (uuid, indices, self.server_addr)))
                avail = sock.poll(1 * 1000, zmq.POLLIN)
                if not avail:
                    try:
                        check_sock = socket.socket()
                        addr_list = addr[len('tcp://'):].split(':')
                        addr_list[1] = int(addr_list[1])
                        check_sock.connect(tuple(addr_list))
                    except Exception as e:
                        logger.warning('connect the addr %s failed with exception %s',
                                       addr, e.message)
                        _report_bad(addr)
                    else:
                        logger.debug("%s recv broadcast %s from %s timeout",
                                     self.server_addr, str(indices), addr)
                    finally:
                        check_sock.close()
                    return
                result, msg = sock.recv_pyobj()
                if result == SERVER_FETCH_FAIL:
                    _report_bad(addr)
                    return
                if result == SERVER_FETCH_OK:
                    indices, blocks = msg
                    for rank, index in enumerate(indices):
                        if blocks[rank] is not None:
                            write_mmap_handler.seek(bit_map[index][0])
                            write_mmap_handler.write(blocks[rank])
                            bitmap[index] = bit_map[index]
                else:
                    raise RuntimeError('Unknown server response: %s %s' % (result, msg))
            finally:
                sock.close()

        final_path = gen_broadcast_path(self.work_dirs, uuid)
        self.uuid_state_dict[uuid] = final_path, False
        fp = open(final_path, 'wb')
        fp.truncate(compressed_size)
        fp.close()
        fd = os.open(final_path, os.O_RDWR)
        write_mmap_handler = mmap.mmap(fd, 0,
                                       access=ACCESS_WRITE)
        os.close(fd)
        while not all(bitmap):
            remote = []
            for addr, _bitmap in six.iteritems(sources):
                if block_num == 0:
                    block_num = len(_bitmap)
                    bitmap = [0] * block_num
                    self.uuid_map_dict[uuid] = bitmap
                if not addr.startswith('tcp://%s:' % self.host):
                    remote.append((addr, _bitmap))
            self.random_inst.shuffle(remote)
            for addr, _bitmap in remote:
                indices = [i for i in range(block_num) if not bitmap[i] and _bitmap[i]]
                if indices:
                    self.random_inst.shuffle(indices)
                    _fetch(addr, indices[:BATCHED_BLOCKS], _bitmap)
                    self._update_sources(uuid, bitmap, guide_sock)
            sources = self._get_sources(uuid, guide_sock)
        write_mmap_handler.flush()
        write_mmap_handler.close()
        self.shared_uuid_map_dict[uuid] = bitmap
        self.shared_uuid_fn_dict[uuid] = self.uuid_state_dict[uuid][0]
        self.uuid_state_dict[uuid] = self.uuid_state_dict[uuid][0], True
        with download_cond:
            download_cond.notify_all()

    def clear(self, uuid):
        if uuid in self.uuid_state_dict:
            del self.uuid_state_dict[uuid]
        if uuid in self.shared_uuid_fn_dict:
            del self.shared_uuid_fn_dict[uuid]
            del self.shared_uuid_map_dict[uuid]

    def shutdown(self):
        if self.server_thread and self.server_addr.\
                startswith('tcp://%s:' % socket.gethostname()):
            req = self.ctx.socket(zmq.REQ)
            req.setsockopt(zmq.LINGER, 0)
            req.connect(self.server_addr)
            req.send_pyobj((SERVER_STOP, None))
            avail = req.poll(1 * 100, zmq.POLLIN)
            if avail:
                req.recv_pyobj()
            req.close()
            for _, th in six.iteritems(self.download_threads):
                th.join()
            self.server_thread.join()


def accumulate_list(l):
    acc = 0
    acc_l = []
    for item in l:
        acc_l.append(acc)
        acc += item
    acc_l.append(acc)
    return acc_l


class BroadcastManager:
    header_fmt = '>BI'
    header_len = struct.calcsize(header_fmt)

    def __init__(self):
        self.guide_addr = None
        self.download_addr = None
        self.cache = None
        self.shared_uuid_fn_dict = None
        self.shared_uuid_map_dict = None
        self.ctx = None
        self.work_dirs = None

    def start(self):
        global shared_uuid_fn_dict, shared_uuid_map_dict
        self.guide_addr = env.get(GUIDE_ADDR)
        self.download_addr = env.get(DOWNLOAD_ADDR)
        self.cache = Cache()
        self.ctx = zmq.Context()
        self.work_dirs = env.get('WORKDIR')
        self.shared_uuid_fn_dict = shared_uuid_fn_dict
        self.shared_uuid_map_dict = shared_uuid_map_dict

    def register(self, uuid, value):
        if uuid in self.shared_uuid_fn_dict:
            raise RuntimeError('broadcast %s has already registered' % uuid)
        blocks, size, block_map = self.to_blocks(uuid, value)
        self._dump_blocks_to_file(blocks, uuid, block_map)
        self._update_sources(uuid, block_map)
        self.cache.put(uuid, value)
        return size

    def _dump_blocks_to_file(self, blocks, uuid, block_map):
        broadcast_path = gen_broadcast_path(self.work_dirs, uuid)
        fp = open(broadcast_path, 'wb')
        for block in blocks:
            fp.write(block)
        fp.close()
        download_sock = self.ctx.socket(zmq.REQ)
        try:
            download_sock.setsockopt(zmq.LINGER, 0)
            download_sock.connect(self.download_addr)
            download_sock.send_pyobj((REGISTER_BLOCKS, (uuid, broadcast_path, block_map)))
            result = download_sock.recv_pyobj()
        finally:
            download_sock.close()
        if result == REGISTER_BLOCKS_FAILED:
            raise RuntimeError('Register the broadcast failed')
        return broadcast_path

    def _update_sources(self, uuid, bitmap):
        guide_sock = self.ctx.socket(zmq.REQ)
        try:
            guide_sock.setsockopt(zmq.LINGER, 0)
            guide_sock.connect(self.guide_addr)
            guide_sock.send_pyobj((GUIDE_SET_SOURCES,
                                   (uuid, self.download_addr, bitmap)))
            guide_sock.recv_pyobj()
        finally:
            guide_sock.close()

    def clear(self, uuid):
        self.cache.put(uuid, None)
        sock = self.ctx.socket(zmq.REQ)
        sock.connect(self.download_addr)
        sock.send_pyobj((SERVER_CLEAR_ITEM, uuid))
        sock.recv_pyobj()
        sock.close()

    def fetch(self, uuid, compressed_size):
        value = self.cache.get(uuid)
        if value is not None:
            return value

        blocks = self.fetch_blocks(uuid, compressed_size)
        value = self.from_blocks(uuid, blocks)
        return value

    def _get_blocks_by_filename(self, file_name, block_map):
        fp = open(file_name, 'rb')
        buf = fp.read()
        blocks = [buf[offset: offset + size] for offset, size in block_map]
        fp.close()
        return blocks

    def fetch_blocks(self, uuid, compressed_size):
        if uuid in self.shared_uuid_fn_dict:
            return self._get_blocks_by_filename(self.shared_uuid_fn_dict[uuid],
                                                self.shared_uuid_map_dict[uuid])
        download_sock = self.ctx.socket(zmq.REQ)
        download_sock.connect(self.download_addr)
        download_sock.send_pyobj((DATA_GET,
                                  (uuid, compressed_size)))
        res = download_sock.recv_pyobj()
        if res == DATA_GET_OK:
            return self._get_blocks_by_filename(self.shared_uuid_fn_dict[uuid],
                                                self.shared_uuid_map_dict[uuid])
        if res == DATA_GET_FAIL:
            raise RuntimeError('Data GET failed for uuid:%s' % uuid)
        while uuid not in self.shared_uuid_fn_dict:
            with download_cond:
                download_cond.wait()
        if uuid in self.shared_uuid_fn_dict:
            return self._get_blocks_by_filename(self.shared_uuid_fn_dict[uuid],
                                                self.shared_uuid_map_dict[uuid])
        else:
            raise RuntimeError('get blocks failed')

    def to_blocks(self, uuid, obj):
        try:
            if marshalable(obj):
                buf = marshal.dumps((uuid, obj))
                type = MARSHAL_TYPE
            else:
                buf = six.moves.cPickle.dumps((uuid, obj), -1)
                type = PICKLE_TYPE

        except Exception:
            buf = six.moves.cPickle.dumps((uuid, obj), -1)
            type = PICKLE_TYPE

        checksum = binascii.crc32(buf) & 0xFFFF
        stream = struct.pack(self.header_fmt, type, checksum) + buf
        blockNum = (len(stream) + (BLOCK_SIZE - 1)) >> BLOCK_SHIFT
        blocks = [compress(stream[i*BLOCK_SIZE:(i+1)*BLOCK_SIZE]) for i in range(blockNum)]
        sizes = [len(block) for block in blocks]
        size_l = accumulate_list(sizes)
        block_map = list(izip(size_l[:-1], sizes))
        return blocks, size_l[-1], block_map

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
            _uuid, value = six.moves.cPickle.loads(buf)
        else:
            raise RuntimeError('Unknown serialization type: %s' % type)

        if uuid != _uuid:
            raise RuntimeError('Wrong blocks: uuid: %s, expected: %s' % (_uuid, uuid))

        return value

_manager = BroadcastManager()
_download_manager = DownloadManager()
_guide_manager = GuideManager()


def start_manager(has_guide=False, has_download=False, in_task=False):
    if has_guide:
        _guide_manager.start()
    if has_download:
        init_dict()
        _download_manager.start()
    if in_task:
        _manager.start()


def stop_manager():
    _guide_manager.shutdown()
    _download_manager.shutdown()


class Broadcast:
    def __init__(self, value):
        assert value is not None, 'broadcast object should not been None'
        self.uuid = str(uuid.uuid4())
        self.value = value
        self.compressed_size = _manager.register(self.uuid, self.value)
        block_num = (self.compressed_size + BLOCK_SIZE - 1) >> BLOCK_SHIFT
        self.bytes = block_num * BLOCK_SIZE
        logger.info("broadcast %s in %d blocks", self.uuid, block_num)

    def clear(self):
        _manager.clear(self.uuid)

    def __getstate__(self):
        return self.uuid, self.compressed_size

    def __setstate__(self, v):
        self.uuid, self.compressed_size = v

    def __getattr__(self, name):
        if name != 'value':
            return getattr(self.value, name)

        value = _manager.fetch(self.uuid, self.compressed_size)
        if value is None:
            raise RuntimeError("fetch broadcast failed")
        self.value = value
        return value

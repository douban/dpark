from __future__ import absolute_import
import os
import zmq
import uuid as uuid_pkg
import time
import binascii
import random
import socket
import struct
import marshal
import mmap
from multiprocessing import Manager, Condition
from mmap import ACCESS_WRITE, ACCESS_READ

from dpark.utils.log import get_logger
from dpark.utils import compress, decompress, spawn, mkdir_p
from dpark.cache import Cache
from dpark.serialize import marshalable
from dpark.env import env
import six
from six.moves import range, map, cPickle

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
DATA_GET, DATA_GET_OK, DATA_GET_FAIL, DATA_DOWNLOADING, SERVER_CLEAR_ITEM = list(range(9))


class GuideManager(object):
    def __init__(self):
        self._started = False
        self.guides = {}
        self.host = socket.gethostname()
        self.guide_thread = None
        self.guide_addr = None
        self.register_addr = {}
        self.ctx = zmq.Context()

    def start(self):
        if self._started:
            return

        self._started = True
        self.guide_thread = self.start_guide()
        env.register(GUIDE_ADDR, self.guide_addr)

    def start_guide(self):
        sock = self.ctx.socket(zmq.REP)
        port = sock.bind_to_random_port('tcp://0.0.0.0')
        self.guide_addr = 'tcp://%s:%d' % (self.host, port)

        def run():
            logger.debug("guide start at %s", self.guide_addr)

            while self._started:
                if not sock.poll(1000, zmq.POLLIN):
                    continue

                type_, msg = sock.recv_pyobj()
                if type_ == GUIDE_STOP:
                    sock.send_pyobj(0)
                    break
                elif type_ == GUIDE_GET_SOURCES:
                    uuid = msg
                    sources = None
                    if uuid in self.guides:
                        sources = self.guides[uuid]
                    else:
                        logger.warning('uuid %s NOT REGISTERED in guide server', uuid)
                    sock.send_pyobj(sources)
                elif type_ == GUIDE_SET_SOURCES:
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
                elif type_ == GUIDE_REPORT_BAD:
                    uuid, addr = msg
                    sources = self.guides[uuid]
                    if addr in sources:
                        if addr != self.register_addr[uuid]:
                            del sources[addr]
                        else:
                            logger.warning('The addr %s to delete is the register Quit!!!', addr)
                    sock.send_pyobj(None)
                else:
                    logger.error('Unknown guide message: %s %s', type_, msg)
                    sock.send_pyobj(None)

        return spawn(run)

    def shutdown(self):
        if not self._started:
            return

        self._started = False
        if self.guide_thread and self.guide_addr. \
                startswith('tcp://%s:' % socket.gethostname()):
            self.guide_thread.join(timeout=1)
            if self.guide_thread.is_alive():
                logger.warning("guide_thread not stopped.")
            self.guide_addr = None


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


def decide_dir(work_dirs):
    return work_dirs[-1]


def gen_broadcast_path(work_dirs, uuid):
    work_dir = decide_dir(work_dirs)
    broadcast_dir = os.path.join(work_dir, 'broadcast')
    mkdir_p(broadcast_dir)
    uuid_path = '%s_%d' % (uuid, os.getpid())
    broadcast_path = os.path.join(broadcast_dir, uuid_path)
    return broadcast_path


class DownloadManager(object):
    def __init__(self):
        self._started = False
        self.server_thread = None
        self.download_threads = {}
        self.uuid_state_dict = None
        self.uuid_map_dict = None
        self.guide_addr = None
        self.server_addr = None
        self.host = None
        self.ctx = None
        self.random_inst = None
        self.work_dirs = []
        self.master_broadcast_blocks = {}

    def start(self):
        if self._started:
            return

        self.manager = manager = Manager()
        self.shared_uuid_fn_dict = manager.dict()
        self.shared_uuid_map_dict = manager.dict()
        self.shared_master_blocks = manager.dict()
        self.download_cond = Condition()

        self._started = True
        self.ctx = zmq.Context()
        self.host = socket.gethostname()
        if GUIDE_ADDR not in env.environ:
            start_guide_manager()

        self.guide_addr = env.get(GUIDE_ADDR)
        self.random_inst = random.SystemRandom()
        self.server_addr, self.server_thread = self.start_server()
        self.uuid_state_dict = {}
        self.uuid_map_dict = {}
        self.work_dirs = env.get('WORKDIR')
        self.master_broadcast_blocks = {}
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

            while self._started:
                if not sock.poll(1000, zmq.POLLIN):
                    continue
                type_, msg = sock.recv_pyobj()
                logger.debug('server recv: %s %s', type_, msg)
                if type_ == SERVER_STOP:
                    sock.send_pyobj(None)
                    break
                elif type_ == SERVER_FETCH:
                    uuid, indices, client_addr = msg
                    if uuid in self.master_broadcast_blocks:
                        block_num = len(self.master_broadcast_blocks[uuid])
                        bls = []
                        for index in indices:
                            if index >= block_num:
                                logger.warning('input index too big %s for '
                                               'len of blocks  %d from host %s',
                                               str(indices), block_num, client_addr)
                                sock.send_pyobj((SERVER_FETCH_FAIL, None))
                            else:
                                bls.append(self.master_broadcast_blocks[uuid][index])
                        sock.send_pyobj((SERVER_FETCH_OK, (indices, bls)))
                    elif uuid in self.uuid_state_dict:
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
                elif type_ == DATA_GET:
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
                                                                *[sources, uuid, compressed_size])
                            sock.send_pyobj(DATA_DOWNLOADING)
                        else:
                            sock.send_pyobj(DATA_DOWNLOADING)
                    else:
                        sock.send_pyobj(DATA_GET_OK)
                elif type_ == SERVER_CLEAR_ITEM:
                    uuid = msg
                    self.clear(uuid)
                    sock.send_pyobj(None)
                else:
                    logger.error('Unknown server message: %s %s', type_, msg)
                    sock.send_pyobj(None)

            sock.close()
            logger.debug("stop Broadcast server %s", server_addr)
            for uuid in list(self.uuid_state_dict.keys()):
                self.clear(uuid)

        return server_addr, spawn(run)

    def get_blocks(self, uuid):
        if uuid in self.master_broadcast_blocks:
            return self.master_broadcast_blocks[uuid]
        if uuid in self.shared_master_blocks:
            return self.shared_master_blocks[uuid]

    def register_blocks(self, uuid, blocks):
        if uuid in self.master_broadcast_blocks:
            logger.warning('the block uuid %s exists in dict', uuid)
            return
        self.master_broadcast_blocks[uuid] = blocks
        self.shared_master_blocks[uuid] = blocks

    def _get_sources(self, uuid, source_sock):
        try:
            source_sock.send_pyobj((GUIDE_GET_SOURCES,
                                    uuid))
            sources = source_sock.recv_pyobj()
        except:
            logger.warning('GET sources failed for addr %s with ZMQ ERR',
                           self.server_addr)
            sources = {}
        return sources

    def _update_sources(self, uuid, bitmap, source_sock):
        try:
            source_sock.send_pyobj((GUIDE_SET_SOURCES,
                                    (uuid, self.server_addr, bitmap)))
            source_sock.recv_pyobj()
        except:
            pass

    def _download_blocks(self, sources, uuid, compressed_size):
        block_num = 0
        bitmap = [0]
        write_mmap_handler = None
        download_guide_sock = self.ctx.socket(zmq.REQ)
        download_guide_sock.setsockopt(zmq.LINGER, 0)
        download_guide_sock.connect(self.guide_addr)

        def _report_bad(addr):
            logger.debug('fetch blocks failed from server %s', addr)
            download_guide_sock.send_pyobj((GUIDE_REPORT_BAD, (uuid, addr)))
            download_guide_sock.recv_pyobj()

        def _fetch(addr, indices, bit_map):
            sock = self.ctx.socket(zmq.REQ)
            try:
                sock.setsockopt(zmq.LINGER, 0)
                sock.connect(addr)
                sock.send_pyobj((SERVER_FETCH, (uuid, indices, self.server_addr)))
                avail = sock.poll(1 * 1000, zmq.POLLIN)
                check_sock = None
                if not avail:
                    try:
                        check_sock = socket.socket()
                        addr_list = addr[len('tcp://'):].split(':')
                        addr_list[1] = int(addr_list[1])
                        check_sock.connect(tuple(addr_list))
                    except Exception as e:
                        logger.warning('connect the addr %s failed with exception %s',
                                       addr, e)
                        _report_bad(addr)
                    else:
                        logger.debug("%s recv broadcast %s from %s timeout",
                                     self.server_addr, str(indices), addr)
                    finally:
                        if check_sock:
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
            for _addr, _bitmap in six.iteritems(sources):
                if block_num == 0:
                    block_num = len(_bitmap)
                    bitmap = [0] * block_num
                    self.uuid_map_dict[uuid] = bitmap
                if not _addr.startswith('tcp://%s:' % self.host):
                    remote.append((_addr, _bitmap))
            self.random_inst.shuffle(remote)
            for _addr, _bitmap in remote:
                _indices = [i for i in range(block_num) if not bitmap[i] and _bitmap[i]]
                if _indices:
                    self.random_inst.shuffle(_indices)
                    _fetch(_addr, _indices[:BATCHED_BLOCKS], _bitmap)
                    self._update_sources(uuid, bitmap, download_guide_sock)
            sources = self._get_sources(uuid, download_guide_sock)
        write_mmap_handler.flush()
        write_mmap_handler.close()
        self.shared_uuid_map_dict[uuid] = bitmap
        self.shared_uuid_fn_dict[uuid] = self.uuid_state_dict[uuid][0]
        self.uuid_state_dict[uuid] = self.uuid_state_dict[uuid][0], True
        download_guide_sock.close()
        with self.download_cond:
            self.download_cond.notify_all()

    def clear(self, uuid):
        if uuid in self.master_broadcast_blocks:
            del self.master_broadcast_blocks[uuid]
            del self.shared_master_blocks[uuid]
        if uuid in self.uuid_state_dict:
            del self.uuid_state_dict[uuid]
        if uuid in self.shared_uuid_fn_dict:
            del self.shared_uuid_fn_dict[uuid]
            del self.shared_uuid_map_dict[uuid]

    def shutdown(self):
        if not self._started:
            return

        self._started = False
        if self.server_thread and self.server_addr. \
                startswith('tcp://%s:' % socket.gethostname()):
            for _, th in six.iteritems(self.download_threads):
                th.join(timeout=0.1)  # only in executor, not needed
            self.server_thread.join(timeout=1)
            if self.server_thread.is_alive():
                logger.warning("Download mananger server_thread not stopped.")

        self.manager.shutdown()  # shutdown will try join and terminate server process


def accumulate_list(l):
    acc = 0
    acc_l = []
    for item in l:
        acc_l.append(acc)
        acc += item
    acc_l.append(acc)
    return acc_l


class BroadcastManager(object):
    header_fmt = '>BI'
    header_len = struct.calcsize(header_fmt)

    def __init__(self):
        self._started = False
        self.guide_addr = None
        self.download_addr = None
        self.cache = None
        self.shared_uuid_fn_dict = None
        self.shared_uuid_map_dict = None
        self.download_cond = None
        self.ctx = None

    def start(self):
        if self._started:
            return

        self._started = True
        start_download_manager()
        self.guide_addr = env.get(GUIDE_ADDR)
        self.download_addr = env.get(DOWNLOAD_ADDR)
        self.cache = Cache()
        self.ctx = zmq.Context()
        self.shared_uuid_fn_dict = _download_manager.shared_uuid_fn_dict
        self.shared_uuid_map_dict = _download_manager.shared_uuid_map_dict
        self.download_cond = _download_manager.download_cond

    def register(self, uuid, value):
        self.start()

        if uuid in self.shared_uuid_fn_dict:
            raise RuntimeError('broadcast %s has already registered' % uuid)
        blocks, size, block_map = self.to_blocks(uuid, value)
        _download_manager.register_blocks(uuid, blocks)
        self._update_sources(uuid, block_map)
        self.cache.put(uuid, value)
        return size

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
        assert self._started
        self.cache.put(uuid, None)
        sock = self.ctx.socket(zmq.REQ)
        sock.connect(self.download_addr)
        sock.send_pyobj((SERVER_CLEAR_ITEM, uuid))
        sock.recv_pyobj()
        sock.close()

    def fetch(self, uuid, compressed_size):
        start_download_manager()
        self.start()
        value = self.cache.get(uuid)
        if value is not None:
            return value
        blocks = _download_manager.get_blocks(uuid)
        if blocks is None:
            blocks = self.fetch_blocks(uuid, compressed_size)
        value = self.from_blocks(uuid, blocks)
        return value

    @staticmethod
    def _get_blocks_by_filename(file_name, block_map):
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
        while True:
            with self.download_cond:
                if uuid not in self.shared_uuid_fn_dict:
                    self.download_cond.wait()
                else:
                    break
        if uuid in self.shared_uuid_fn_dict:
            return self._get_blocks_by_filename(self.shared_uuid_fn_dict[uuid],
                                                self.shared_uuid_map_dict[uuid])
        else:
            raise RuntimeError('get blocks failed')

    def to_blocks(self, uuid, obj):
        try:
            if marshalable(obj):
                buf = marshal.dumps((uuid, obj))
                type_ = MARSHAL_TYPE
            else:
                buf = cPickle.dumps((uuid, obj), -1)
                type_ = PICKLE_TYPE

        except Exception:
            buf = cPickle.dumps((uuid, obj), -1)
            type_ = PICKLE_TYPE

        checksum = binascii.crc32(buf) & 0xFFFF
        stream = struct.pack(self.header_fmt, type_, checksum) + buf
        blockNum = (len(stream) + (BLOCK_SIZE - 1)) >> BLOCK_SHIFT
        blocks = [compress(stream[i * BLOCK_SIZE:(i + 1) * BLOCK_SIZE]) for i in range(blockNum)]
        sizes = [len(block) for block in blocks]
        size_l = accumulate_list(sizes)
        block_map = list(izip(size_l[:-1], sizes))
        return blocks, size_l[-1], block_map

    def from_blocks(self, uuid, blocks):
        stream = b''.join(map(decompress, blocks))
        type_, checksum = struct.unpack(self.header_fmt, stream[:self.header_len])
        buf = stream[self.header_len:]
        _checksum = binascii.crc32(buf) & 0xFFFF
        if _checksum != checksum:
            raise RuntimeError('Wrong blocks: checksum: %s, expected: %s' % (
                _checksum, checksum))

        if type_ == MARSHAL_TYPE:
            _uuid, value = marshal.loads(buf)
        elif type_ == PICKLE_TYPE:
            _uuid, value = cPickle.loads(buf)
        else:
            raise RuntimeError('Unknown serialization type: %s' % type_)

        if uuid != _uuid:
            raise RuntimeError('Wrong blocks: uuid: %s, expected: %s' % (_uuid, uuid))

        return value

    def shutdown(self):
        if not self._started:
            return

        self._started = False


_manager = BroadcastManager()
_download_manager = DownloadManager()
_guide_manager = GuideManager()


def start_guide_manager():
    _guide_manager.start()


def start_download_manager():
    _download_manager.start()


def stop_manager():
    _manager.shutdown()
    _download_manager.shutdown()
    _guide_manager.shutdown()
    env.environ.pop(GUIDE_ADDR, None)
    env.environ.pop(DOWNLOAD_ADDR, None)


class Broadcast(object):
    def __init__(self, value):
        assert value is not None, 'broadcast object should not been None'
        self.uuid = str(uuid_pkg.uuid4())
        self.value = value
        self.compressed_size = _manager.register(self.uuid, self.value)
        block_num = (self.compressed_size + BLOCK_SIZE - 1) >> BLOCK_SHIFT
        self.bytes = block_num * BLOCK_SIZE
        logger.info("broadcast %s in %d blocks, %d bytes", self.uuid, block_num, self.compressed_size)

    def clear(self):
        _manager.clear(self.uuid)

    def __getstate__(self):
        return self.uuid, self.compressed_size

    def __setstate__(self, v):
        self.uuid, self.compressed_size = v

    def __getattr__(self, name):
        if name != 'value':
            return getattr(self.value, name)

        t = time.time()
        value = _manager.fetch(self.uuid, self.compressed_size)
        if value is None:
            raise RuntimeError("fetch broadcast failed")
        env.task_stats.secs_broadcast += time.time() - t
        self.value = value
        return value

    def __len__(self):
        return len(self.value)

    def __iter__(self):
        return self.value.__iter__()

    def __getitem__(self, key):
        return self.value.__getitem__(key)

    def __contains__(self, item):
        return self.value.__contains__(item)

    def __missing__(self, key):
        return self.value.__missing__(key)

    def __reversed__(self):
        return self.value.__reversed__()

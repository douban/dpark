import socket
import zmq
import time

from dpark.env import env
from dpark.util import spawn, get_logger

logger = get_logger(__name__)

class TrackerMessage(object):
    pass

class StopTrackerMessage(TrackerMessage):
    pass

class SetValueMessage(TrackerMessage):
    def __init__(self, key, value):
        self.key = key
        self.value = value

class AddItemMessage(TrackerMessage):
    def __init__(self, key, item):
        self.key = key
        self.item = item

class RemoveItemMessage(TrackerMessage):
    def __init__(self, key, item):
        self.key = key
        self.item = item

class GetValueMessage(TrackerMessage):
    def __init__(self, key):
        self.key = key

class TrackerServer(object):
    locs = {}
    def __init__(self):
        self.addr = None
        self.thread = None

    def start(self):
        self.thread = spawn(self.run)
        while self.addr is None:
            time.sleep(0.01)

    def stop(self):
        sock = env.ctx.socket(zmq.REQ)
        sock.connect(self.addr)
        sock.send_pyobj(StopTrackerMessage())
        confirm_msg = sock.recv_pyobj()
        sock.close()
        self.thread.join()
        return confirm_msg

    def get(self, key):
        return self.locs.get(key, [])

    def set(self, key, value):
        if not isinstance(value, list):
            value = [value]

        self.locs[key] = value

    def add(self, key, item):
        if key not in self.locs:
            self.locs[key] = []

        self.locs[key].append(item)

    def remove(self, key, item):
        if item in self.locs[key]:
            self.locs[key].remove(item)

    def run(self):
        locs = self.locs
        sock = env.ctx.socket(zmq.REP)
        port = sock.bind_to_random_port("tcp://0.0.0.0")
        self.addr = "tcp://%s:%d" % (socket.gethostname(), port)
        logger.debug("TrackerServer started at %s", self.addr)
        def reply(msg):
            sock.send_pyobj(msg)
        while True:
            msg = sock.recv_pyobj()
            if isinstance(msg, SetValueMessage):
                self.set(msg.key, msg.value)
                reply('OK')
            elif isinstance(msg, AddItemMessage):
                self.add(msg.key, msg.item)
                reply('OK')
            elif isinstance(msg, RemoveItemMessage):
                self.remove(msg.key, msg.item)
                reply('OK')
            elif isinstance(msg, GetValueMessage):
                reply(self.get(msg.key))
            elif isinstance(msg, StopTrackerMessage):
                reply('OK')
                break
            else:
                logger.error("unexpected msg %s %s", msg, type(msg))
                reply('ERROR')
        sock.close()
        logger.debug("stop TrackerServer %s", self.addr)

class TrackerClient(object):
    def __init__(self, addr):
        self.addr = addr

    def call(self, msg):
        try:
            sock = env.ctx.socket(zmq.REQ)
            sock.connect(self.addr)
            sock.send_pyobj(msg)
            return sock.recv_pyobj()
        finally:
            sock.close()


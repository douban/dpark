import sys, os
import socket
import time
import threading
import logging

from mesos_pb2 import *
from messages_pb2 import *

logger = logging.getLogger(__name__)

def spawn(target, *args, **kw):
    t = threading.Thread(target=target, name=target.__name__, args=args, kwargs=kw)
    t.daemon = True
    t.start()
    return t

class UPID(object):
    def __init__(self, name, addr=None):
        if addr is None and name and '@' in name:
            name, addr = name.split('@')
        self.name = name
        self.addr = addr
    
    def __str__(self):
        return "%s@%s" % (self.name, self.addr)


class Process(UPID):
    def __init__(self, name, port=0):
        UPID.__init__(self, name)
        self.port = port
        self.conn_pool = {}
        self.delayed_jobs = []
        self.linked = {}
        self.sender = None
        self.aborted = False

    def delay(self, delay, func, *args, **kw):
        self.delayed_jobs.append((time.time() + delay, func, args, kw))

    def run_delayed_jobs(self):
        while not self.aborted:
            now = time.time()
            todo = [t for t in self.delayed_jobs if t[0] < now]
            self.delayed_jobs = [t for t in self.delayed_jobs if t[0] >= now]
            for _, func, args, kw in todo:
                func(*args, **kw)
            else:
                time.sleep(1)

    def link(self, upid, callback):
        self._get_conn(upid.addr)
        self.linked[upid.addr] = callback

    def _get_conn(self, addr):
        if addr not in self.conn_pool:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host, port = addr.split(':')
            try:
                s.connect((host, int(port)))
            except IOError, e:
                logger.warning("fail to connect to %s, retry after 3 seconds", addr) 
                time.sleep(3)
                s.connect((host, int(port)))
            self.conn_pool[addr] = s
        return self.conn_pool[addr]

    def _encode(self, upid, msg):        
        if isinstance(msg, str):
            body = ''
            uri = '/%s/%s' % (upid.name, msg)
        else:
            body = msg.SerializeToString()
            uri = '/%s/mesos.internal.%s' % (upid.name, msg.__class__.__name__)
        agent = 'libprocess/%s@%s' % (self.name, self.addr)
        msg = ['POST %s HTTP/1.0' % str(uri),
               'User-Agent: %s' % agent,
               'Connection: Keep-Alive',]
        if body:
            msg += [
               'Transfer-Encoding: chunked',
               '',
               '%x' % len(body), body,
               '0', '', '',]
        return '\r\n'.join(msg)

    def send(self, upid, msg):
        data = self._encode(upid, msg)
        try:
            conn = self._get_conn(upid.addr)
            conn.send(data)
        except IOError:
            logger.warning("failed to send data to %s, retry again", upid)
            self.conn_pool.pop(upid.addr, None)
            if upid.addr in self.linked: # broken link
                callback = self.linked.pop(upid.addr)
                callback()
            try:
                conn = self._get_conn(upid.addr)
                conn.send(data)
            except IOError:
                logger.error("failed to send data to %s, give up", upid)
                self.conn_pool.pop(upid.addr, None)

    def reply(self, msg):
        return self.send(self.sender, msg)

    def onPing(self):
        self.reply('PONG')

    def handle(self, msg):
        if self.aborted:
            return
        name = msg.__class__.__name__
        f = getattr(self, 'on' + name, None)
        assert f, 'should have on%s()' % name
        args = [v for (_,v) in msg.ListFields()]
        f(*args)

    def abort(self):
        self._get_conn(self.addr)
        self.aborted = True

    def stop(self):
        self.abort()

    def join(self):
        self.delay_t.join()
        return self.accept_t.join()

    def run(self):
        self.start()
        return self.join()

    def communicate(self, conn):
        rf = conn.makefile('r', 4096)
        while True:
            headers = []
            while True:
                try:
                    line = rf.readline()
                except IOError:
                    break
                if not line or line == '\r\n':
                    break
                headers.append(line)
            if not headers:
                break # EoF
            
            method, uri, _ = headers[0].split(' ')
            _, process, mname = uri.split('/')
            assert process == self.name, 'unexpected messages'
            agent = headers[1].split(' ')[1]
            logger.info("incoming request: %s from %s", uri, agent)
            
            sender_name, addr = agent.split('@')
            self.sender = UPID(sender_name.split('/')[1], addr)
            
            if mname == 'PING':
                self.onPing()
                continue
            
            size = int(rf.readline(), 16)
            body = rf.read(size+2)[:-2]
            rf.read(5)  # ending

            sname = mname.split('.')[2]
            if sname not in globals():
                logger.error("unknown messages: %s", sname)
                continue

            try:
                msg = globals()[sname].FromString(body)
                self.handle(msg)
            except Exception, e:
                logger.error("error while processing message %s: %s", sname, e)

        logger.info("conn from %s is closed", remote_addr)

        conn.close()

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ip = socket.gethostbyname(socket.gethostname())
        sock.bind((ip, self.port))
        if not self.port:
            port = sock.getsockname()[1]
            self.addr = '%s:%d' % (ip, port)
        self.accept_t = spawn(self.accept)
        self.delay_t = spawn(self.run_delayed_jobs)

    def accept(self, s): 
        s.listen(1)
        conns = []
        while True:
            conn, addr = s.accept()
            logger.debug("accepted conn from %s", addr)
            conns.append(conn)
            if self.aborted:
                break

            spawn(self.communicate, conn, addr)

        for c in conns:
            c.close()

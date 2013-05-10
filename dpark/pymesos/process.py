import sys, os
import socket
import time
import threading

from mesos_pb2 import *
from messages_pb2 import *

class UPID(object):
    def __init__(self, name, addr=None):
        if addr is None and name and '@' in name:
            name, addr = name.split('@')
        self.name = name
        self.addr = addr

    def __str__(self):
        return "%s@%s" % (self.name, self.addr)


class Process(UPID):
    def __init__(self, name):
        UPID.__init__(self, name, None)
        self.pool = {}
        self.aborted = False

    def _get_conn(self, addr):
        if addr not in self.pool:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host, port = addr.split(':')
            s.connect((host, int(port)))
            self.pool[addr] = s
        return self.pool[addr]

    def send(self, upid, msg):
        conn = self._get_conn(upid.addr)
        conn.send(self._encode(upid, msg))

    def _encode(self, upid, msg):
        body = msg.SerializeToString()
        uri = '/%s/mesos.internal.%s' % (upid.name, msg.__class__.__name__)
        agent = 'libprocess/%s@%s' % (self.name, self.addr)
        msg = ['POST %s HTTP/1.0' % str(uri),
               'User-Agent: %s' % agent,
               'Connection: Keep-Alive',
               'Transfer-Encoding: chunked',
               '',
               '%x' % len(body), body,
               '0', '', '',]
        return '\r\n'.join(msg)

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
        self.aborted = True

    def join(self):
        return self.accept_t.join()

    def run(self):
        self.start()
        return self.join()

    def communicate(self, conn):
        rf = conn.makefile('r', 4096)
        while True:
            headers = []
            while True:
                line = rf.readline()
                #if not line:
                #    break
                if line == '\r\n': break;
                headers.append(line)
            #if not line or self.aborted:
            #    break
            #print headers
            size = int(rf.readline(), 16)
            body = rf.read(size+2)[:-2]
            rf.read(5)  # ending
            method, uri, _ = headers[0].split(' ')
            _, process, mname = uri.split('/')
            agent = headers[1].split(' ')[1]
            #print uri, agent
            sname = mname.split('.')[2]
            sender_n, addr = agent.split('@')
            sender = UPID(sender_n.split('/')[1], addr)
            msg = globals()[sname].FromString(body)
            self.handle(msg)

        conn.close()

    def start(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ip = socket.gethostbyname(socket.gethostname())
        s.bind((ip, 0))
        port = s.getsockname()[1]
        self.addr = '%s:%d' % (ip, port)

        t = threading.Thread(target=self.accept, args=[s])
        t.daemon = True
        t.start()
        self.accept_t = t

    def accept(self, s):
        s.listen(1)
        conns = []
        while True:
            conn, addr = s.accept()
            #print 'accepted', addr, conn
            conns.append(conn)
            if self.aborted:
                break
            t = threading.Thread(target=self.communicate, args=[conn,])
            t.daemon = True
            t.start()

        for c in conns:
            c.close()

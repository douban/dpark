import sys, os
import socket
import time
import threading
import logging
import Queue
import select

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


def async(f):
    def func(self, *a, **kw):
        self.delay(0, f, self, *a, **kw)
    return func

class Process(UPID):
    def __init__(self, name, port=0):
        UPID.__init__(self, name)
        self.port = port
        self.conn_pool = {}
        self.jobs = Queue.PriorityQueue()
        self.linked = {}
        self.sender = None
        self.aborted = False

    def delay(self, delay, func, *args, **kw):
        self.jobs.put((time.time() + delay, 0, func, args, kw))

    def run_jobs(self):
        while True:
            try:
                job = self.jobs.get(timeout=1)
            except Queue.Empty:
                if self.aborted:
                    break
                continue

            #self.jobs.task_done()
            t, tried, func, args, kw = job
            now = time.time()
            if t > now:
                if self.aborted:
                    break
                self.jobs.put(job)
                time.sleep(min(t-now, 0.1))
                continue
            
            try:
                #logger.debug("run job %s", func.__name__)
                func(*args, **kw)
                #logger.debug("run job %s comeplete", func.__name__)
            except Exception, e:
                logging.error("error while call %s (tried %d times)", func, tried)
                import traceback; traceback.print_exc()
                if tried < 4:
                    self.jobs.put((t + 3 ** tried, tried + 1, func, args, kw))

    @async
    def link(self, upid, callback):
        self._get_conn(upid.addr)
        self.linked[upid.addr] = callback

    def _get_conn(self, addr):
        if addr not in self.conn_pool:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host, port = addr.split(':')
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
               '0']
        msg += ['', ''] # for last \r\n\r\n
        return '\r\n'.join(msg)

    #@async
    def send(self, upid, msg):
        logger.debug("send to %s %s", upid, msg.__class__.__name__)
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
            raise

    def reply(self, msg):
        return self.send(self.sender, msg)

    def onPing(self):
        self.reply('PONG')

    @async
    def handle(self, msg):
        if self.aborted:
            return
        name = msg.__class__.__name__
        f = getattr(self, 'on' + name, None)
        assert f, 'should have on%s()' % name
        args = [v for (_,v) in msg.ListFields()]
        f(*args)

    def abort(self):
        self.aborted = True
        self.listen_sock.close()

    def stop(self):
        self.abort()
        self.join()
        for addr in self.conn_pool:
            self.conn_pool[addr].close()
        self.conn_pool.clear()

    def join(self):
        self.delay_t.join()
        return self.accept_t.join()

    def run(self):
        self.start()
        return self.join()

    def start(self):
        self.listen_sock = sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('0.0.0.0', self.port))
        if not self.port:
            port = sock.getsockname()[1]
            self.addr = '%s:%d' % (socket.gethostname(), port)
        self.accept_t = spawn(self.ioloop, sock)
        self.delay_t = spawn(self.run_jobs)

    def process_message(self, rf):
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
            return False # EoF
       
        method, uri, _ = headers[0].split(' ')
        _, process, mname = uri.split('/')
        assert process == self.name, 'unexpected messages'
        agent = headers[1].split(' ')[1].strip()
        logger.debug("incoming request: %s from %s", uri, agent)
        
        sender_name, addr = agent.split('@')
        self.sender = UPID(sender_name.split('/')[1], addr)
        
        if mname == 'PING':
            self.onPing()
            return True

        size = rf.readline() 
        if size:
            size = int(size, 16)
            body = rf.read(size+2)[:-2]
            rf.read(5)  # ending
        else:
            body = ''

        sname = mname.split('.')[2]
        if sname not in globals():
            logger.error("unknown messages: %s", sname)
            return True

        try:
            msg = globals()[sname].FromString(body)
            self.handle(msg)
        except Exception, e:
            logger.error("error while processing message %s: %s", sname, e)
            import traceback; traceback.print_exc()
        return True

    def ioloop(self, sock): 
        sock.listen(1)
        sfd = sock.fileno()
        conns = {sfd: sock}
        while not self.aborted:
            #logging.debug("select %s", conns.keys())
            rlist = select.select(conns.keys(), [], [], 1)[0]
            for fd in rlist:
                #logging.debug("start process event %d", fd)
                if fd == sfd:
                    conn, addr = sock.accept()
                    logger.debug("accepted conn from %s", addr)
                    conns[conn.fileno()] = conn.makefile('r')
                elif fd in conns:
                    try:
                        f = conns[fd]
                        while True:
                            if not self.process_message(f):
                                conns.pop(fd).close()
                                break
                            # is there any data in read buffer ?
                            if not f._rbuf.tell(): 
                                break
                    except Exception, e:
                        import traceback; traceback.print_exc()
                        conns.pop(fd).close()
                #logging.debug("stop process event %d", fd)
        sock.close()
    """
    def communicate(self, conn):
        rf = conn.makefile('r', 4096)
        while not self.aborted:
            cont = self.process_message(rf)
            if not cont:
                break
        rf.close()

    def ioloop(self, s):
        s.listen(1)
        conns = []
        while True:
            conn, addr = s.accept()
            logger.debug("accepted conn from %s", addr)
            conns.append(conn)
            if self.aborted:
                break

            spawn(self.communicate, conn)

        for c in conns:
            c.close()        
    """

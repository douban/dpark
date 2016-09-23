#!/usr/bin/env python

# hook for virtualenv
# switch to the virtualenv where the executor belongs,
# replace all the path for modules
import sys
import os.path
P = 'site-packages'
apath = os.path.abspath(__file__)
if P in apath:
    virltualenv = apath[:apath.index(P)]
    sysp = [p[:-len(P)] for p in sys.path if p.endswith(P)][0]
    if sysp != virltualenv:
        sys.path = [p.replace(sysp, virltualenv) for p in sys.path]

import os
import zmq
import time
import socket
import pickle
import subprocess

from addict import Dict
from threading import Thread
from pymesos import Executor, MesosExecutorDriver, decode_data

ctx = zmq.Context()


def forword(fd, addr, prefix=''):
    f = os.fdopen(fd, 'r', 4096)
    out = ctx.socket(zmq.PUSH)
    out.connect(addr)
    while True:
        try:
            line = f.readline()
            if not line:
                break
            out.send(prefix + line)
        except IOError:
            break
    f.close()
    out.close()


def reply_status(driver, task_id, status):
    update = Dict()
    update.task_id = task_id
    update.state = status
    update.timestamp = time.time()
    driver.sendStatusUpdate(update)


def launch_task(self, driver, task):
    reply_status(driver, task.task_id, 'TASK_RUNNING')

    host = socket.gethostname()
    cwd, command, _env, shell, addr1, addr2, addr3 = pickle.loads(
        decode_data(task.data)
    )

    prefix = "[%s@%s] " % (str(task.task_id.value), host)
    outr, outw = os.pipe()
    errr, errw = os.pipe()
    t1 = Thread(target=forword, args=[outr, addr1, prefix])
    t1.daemon = True
    t1.start()
    t2 = Thread(target=forword, args=[errr, addr2, prefix])
    t2.daemon = True
    t2.start()
    wout = os.fdopen(outw, 'w', 0)
    werr = os.fdopen(errw, 'w', 0)

    if addr3:
        tid = int(task.task_id.value.split('-')[0])
        subscriber = ctx.socket(zmq.SUB)
        subscriber.connect(addr3)
        subscriber.setsockopt(zmq.SUBSCRIBE, '')
        poller = zmq.Poller()
        poller.register(subscriber, zmq.POLLIN)
        socks = dict(poller.poll(min(tid / 100.0 + 1, 5) * 60 * 1000))
        if socks and socks.get(subscriber) == zmq.POLLIN:
            hosts = pickle.loads(subscriber.recv(zmq.NOBLOCK))
            line = hosts.get(host)
            if line:
                command = line.split(' ')
            else:
                return reply_status(driver, task.task_id, 'TASK_FAILED')
        else:
            return reply_status(driver, task.task_id, 'TASK_FAILED')

    mem = 100
    for r in task.resources:
        if r.name == 'mem':
            mem = r.scalar.value
            break

    try:
        env = dict(os.environ)
        env.update(_env)
        if not os.path.exists(cwd):
            print >>werr, 'CWD %s is not exists, use /tmp instead' % cwd
            cwd = '/tmp'
        p = subprocess.Popen(command,
                             stdout=wout, stderr=werr,
                             cwd=cwd, env=env, shell=shell)
        tid = task.task_id.value
        self.ps[tid] = p
        code = None
        last_time = 0
        while True:
            time.sleep(0.1)
            code = p.poll()
            if code is not None:
                break

            now = time.time()
            if now < last_time + 2:
                continue

            last_time = now
            try:
                import psutil
                process = psutil.Process(p.pid)

                rss = sum((proc.memory_info().rss
                           for proc in process.get_children(recursive=True)),
                          process.memory_info().rss)
                rss = (rss >> 20)

                if rss > mem * 1.5:
                    print >>werr, "task %s used too much memory: %dMB > %dMB * 1.5, kill it. " \
                        "use -m argument to request more memory." % (
                            tid, rss, mem)
                    p.kill()

                elif rss > mem:
                    print >>werr, "task %s used too much memory: %dMB > %dMB, " \
                        "use -m to request for more memory" % (
                            tid, rss, mem)

            except Exception:
                pass

        if code == 0:
            status = 'TASK_FINISHED'
        else:
            print >>werr, ' '.join(command) + ' exit with %s' % code
            status = 'TASK_FAILED'
    except Exception:
        status = 'TASK_FAILED'
        import traceback
        print >>werr, 'exception while open ' + ' '.join(command)
        for line in traceback.format_exc():
            werr.write(line)

    reply_status(driver, task.task_id, status)

    wout.close()
    werr.close()
    t1.join()
    t2.join()

    self.ps.pop(tid, None)
    self.ts.pop(tid, None)


class MyExecutor(Executor):

    def __init__(self):
        self.ps = {}
        self.ts = {}

    def launchTask(self, driver, task):
        t = Thread(target=launch_task, args=(self, driver, task))
        t.daemon = True
        t.start()
        self.ts[task.task_id.value] = t

    def killTask(self, driver, task_id):
        try:
            if task_id.value in self.ps:
                self.ps[task_id.value].kill()
                reply_status(driver, task_id, 'TASK_KILLED')
        except:
            pass

    def shutdown(self, driver):
        for p in self.ps.values():
            try:
                p.kill()
            except:
                pass
        for t in self.ts.values():
            t.join()

if __name__ == "__main__":
    if os.getuid() == 0:
        gid = os.environ['GID']
        uid = os.environ['UID']
        os.setgid(int(gid))
        os.setuid(int(uid))
    executor = MyExecutor()
    MesosExecutorDriver(executor, use_addict=True).run()

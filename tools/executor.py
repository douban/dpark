#!/usr/bin/env python
import os
import pickle
import subprocess
import sys
import threading
import time
from threading import Thread
import socket

import zmq

import mesos
import mesos_pb2

ctx = zmq.Context()

def forword(fd, addr, prefix=''):
    f = os.fdopen(fd, 'r', 0)
    out = ctx.socket(zmq.PUSH)
    out.connect(addr)
    while True:
        try:
            line = f.readline()
            if not line: break
            out.send(prefix+line)
        except IOError:
            break
    out.close()
    f.close()

def reply_status(driver, task, status):
    update = mesos_pb2.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = status
    driver.sendStatusUpdate(update)
    

def launch_task(self, driver, task):
    reply_status(driver, task, mesos_pb2.TASK_RUNNING)
    
    host = socket.gethostname()
    cwd, command, _env, shell, addr1, addr2, addr3 = pickle.loads(task.data)
    stderr = ctx.socket(zmq.PUSH)
    stderr.connect(addr2)

    prefix = "[%s@%s] " % (str(task.task_id.value), host)
    outr, outw = os.pipe()
    errr, errw = os.pipe()
    t1 = Thread(target=forword, args=[outr, addr1, prefix])
    t1.daemon = True
    t1.start()
    t2 = Thread(target=forword, args=[errr, addr2, prefix])
    t2.daemon = True
    t2.start()
    wout = os.fdopen(outw,'w',0)
    werr = os.fdopen(errw,'w',0)


    if addr3:
        subscriber = ctx.socket(zmq.SUB)
        subscriber.connect(addr3)
        subscriber.setsockopt(zmq.SUBSCRIBE, '')
        poller = zmq.Poller()
        poller.register(subscriber, zmq.POLLIN)
        print >> werr, 'start polling at %s' % host
        socks = dict(poller.poll(60 * 1000))
        print >> werr, 'stop polling at %s' % host
        if socks and socks.get(subscriber) == zmq.POLLIN:
            hosts = pickle.loads(subscriber.recv(zmq.NOBLOCK))
            line = hosts.get(host)
            if line:
                command = line.split(' ')
            else:
                print >> werr, 'publisher canceled task'
                reply_status(driver, task, mesos_pb2.TASK_FAILED)
                return
        else:
            print >> werr, 'waiting publisher timeout'
            reply_status(driver, task, mesos_pb2.TASK_FAILED)
            return

    try:
        env = dict(os.environ)
        env.update(_env)
        if not os.path.exists(cwd):
            print >>werr, 'CWD %s is not exists, use /tmp instead' % cwd
            cwd = '/tmp'
        p = subprocess.Popen(command,
                stdout=wout, stderr=werr,
                cwd=cwd, env=env, shell=shell)
        self.ps[task.task_id.value] = p
        p.wait()
        code = p.returncode
        if code == 0 or code is None:
            status = mesos_pb2.TASK_FINISHED
        else:
            stderr.send(prefix+' '.join(command) 
                + (' exit with %s\n' % code))
            status = mesos_pb2.TASK_FAILED
    except Exception, e:
        status = mesos_pb2.TASK_FAILED
        import traceback
        stderr.send('exception while open '+
                ' '.join(command) + '\n')
        for line in traceback.format_exc():
            stderr.send(line)
    
    wout.close()
    werr.close()
    t1.join()
    t2.join()
    stderr.close()
    if task.task_id.value in self.ps:
        del self.ps[task.task_id.value]

    reply_status(driver, task, status)


class MyExecutor(mesos.Executor):
    def registered(self, driver, executorInfo,
                   frameworkInfo, slaveInfo):
        self.ps = {}

    def launchTask(self, driver, task):
        t = Thread(target=launch_task, 
            args=(self, driver, task))
        t.daemon = True
        t.start()
  
    def killTask(self, driver, task_id):
        if task_id.value in self.ps:
            self.ps[task_id.value].kill()
    
    def shutdown(self, driver):
        for p in self.ps.values():
            try: p.kill()
            except: pass

    def frameworkMessage(self, driver, message):
        pass

if __name__ == "__main__":
  executor = MyExecutor()
  mesos.MesosExecutorDriver(executor).run()

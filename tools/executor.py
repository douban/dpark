#!/usr/bin/env python
import os
import cPickle as pickle
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
    cwd, command, _env, shell, stdout_addr, stderr_addr = pickle.loads(task.data)

    prefix = "[%s@%s] " % (str(task.task_id.value), socket.gethostname())
    outr, outw = os.pipe()
    errr, errw = os.pipe()
    t1 = Thread(target=forword, args=[outr, stdout_addr, prefix])
    t1.daemon = True
    t1.start()
    t2 = Thread(target=forword, args=[errr, stderr_addr, prefix])
    t2.daemon = True
    t2.start()
    wout = os.fdopen(outw,'w',0)
    werr = os.fdopen(errw,'w',0)

    env = dict(os.environ)
    env.update(_env)
    if not os.path.exists(cwd):
        print >>werr, 'CWD %s is not exists, use /tmp instead' % cwd
        cwd = '/tmp'
    
    try:
        p = subprocess.Popen(command, stdout=wout, stderr=werr,
                cwd=cwd, env=env, shell=shell)
        self.ps[task.task_id.value] = p
        
        reply_status(driver, task, mesos_pb2.TASK_RUNNING)
        p.wait()
        code = p.returncode
        if code == 0 or code is None:
            status = mesos_pb2.TASK_FINISHED
        else:
            print >>werr, ' '.join(command) + ' exit with %s' % code
            status = mesos_pb2.TASK_FAILED
        del self.ps[task.task_id.value]

    except Exception, e:
        status = mesos_pb2.TASK_FAILED
        import traceback
        print >>werr, 'exception while open', ' '.join(command)
        traceback.print_exc(file=werr)
    
    wout.close()
    werr.close()
    t1.join()
    t2.join()

    reply_status(driver, task, status)


class MyExecutor(mesos.Executor):
    def init(self, driver, args):
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

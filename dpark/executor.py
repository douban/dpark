#/usr/bin/env python
import logging
import os, sys, time
import os.path
import threading
import marshal
import cPickle
import socket
import multiprocessing

import zmq
import mesos
import mesos_pb2

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(s):
        pass

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dpark.accumulator import Accumulator
from dpark.schedule import Success, OtherFailure
from dpark.env import env

TASK_RESULT_LIMIT = 1024 * 1024 * 2

def reply_status(driver, task, status, data=None):
    update = mesos_pb2.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = status
    if data is not None:
        if len(data) > TASK_RESULT_LIMIT:
            driver.sendFrameworkMessage("warning: task result is too large size=%d" % len(data))
            if len(data) > TASK_RESULT_LIMIT * 10:
                driver.sendFrameworkMessage("error: drop the result with size=%d" % len(data))
                data = ''
        update.data = data
    driver.sendStatusUpdate(update)

def run_task(task, aid):
    try:
        setproctitle('dpark worker: run task %s' % task)
        Accumulator.clear()
        result = task.run(aid)
        accUpdate = Accumulator.values()
        setproctitle('dpark worker: idle')
        return mesos_pb2.TASK_FINISHED, cPickle.dumps((task.id, Success(), result, accUpdate), -1)
    except Exception, e:
        import traceback
        msg = traceback.format_exc()
        setproctitle('dpark worker: idle')
        return mesos_pb2.TASK_FAILED, cPickle.dumps((task.id, OtherFailure(msg), None, None), -1)

def init_env(args):
    setproctitle('dpark worker: idle')
    env.start(False, args)

def forword(fd, addr, prefix=''):
    f = os.fdopen(fd, 'r')
    ctx = zmq.Context()
    out = ctx.socket(zmq.PUSH)
    out.connect(addr)
    buf = []
    while True:
        try:
            line = f.readline()
            if not line: break
            buf.append(line)
            if line.endswith('\n'):
                out.send(prefix+''.join(buf))
                buf = []
        except IOError:
            break
    if buf:
        out.send(''.join(buf))
    out.close()
    f.close()
    ctx.shutdown()

def start_forword(addr, prefix=''):
    rfd, wfd = os.pipe()
    t = threading.Thread(target=forword, args=[rfd, addr, prefix])
    t.daemon = True
    t.start()    
    return t, os.fdopen(wfd, 'w', 0) 

class MyExecutor(mesos.Executor):
    def init(self, driver, args):
        cwd, python_path, parallel, out_logger, err_logger, args = marshal.loads(args.data)
        try:
            os.chdir(cwd)
        except OSError:
            driver.sendFrameworkMessage("switch cwd failed: %s not exists!" % cwd)
        sys.path = python_path
        self.outt, sys.stdout = start_forword(out_logger)
        self.errt, sys.stderr = start_forword(err_logger)
        self.pool = multiprocessing.Pool(parallel, init_env, [args])

    def launchTask(self, driver, task):
        try:
            t, aid = cPickle.loads(task.data)
        except Exception, e:
            import traceback
            msg = traceback.format_exc()
            reply_status(driver, task, mesos_pb2.TASK_LOST, msg)
            return

        def callback((state, data)):
            reply_status(driver, task, state, data)
        
        reply_status(driver, task, mesos_pb2.TASK_RUNNING)
        self.pool.apply_async(run_task, [t, aid], callback=callback)
    
    def killTask(self, driver, taskId):
        #driver.sendFrameworkMessage('kill task %s' % taskId)
        pass

    def shutdown(self, driver):
        # flush
        sys.stdout.close()
        sys.stderr.close()
        self.outt.join()
        self.errt.join()
        for p in self.pool._pool:
            try: p.terminate()
            except: pass
        #for p in self.pool._pool:
        #    try: p.join()
        #    except: pass

    def error(self, driver, code, message):
        logging.error("error: %s, %s", code, message)

    def frameworkMessage(self, driver, data):
        pass

def run():
    executor = MyExecutor()
    driver = mesos.MesosExecutorDriver(executor)
    driver.run()

if __name__ == '__main__':
    run()

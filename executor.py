#/usr/bin/env python
import logging
import os, sys, time
import threading
import cPickle
import socket
import multiprocessing

import mesos
import mesos_pb2

from accumulator import Accumulator
from schedule import Success, OtherFailure
import env

def reply_status(driver, task, status, data=None):
    update = mesos_pb2.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = status
    if data is not None:
        update.data = data
    driver.sendStatusUpdate(update)

def run_task(task, aid):
    try:
        Accumulator.clear()
        result = task.run(aid)
        accUpdate = Accumulator.values()
        return mesos_pb2.TASK_FINISHED, cPickle.dumps((task.id, Success(), result, accUpdate))
    except Exception, e:
        import traceback
        msg = traceback.format_exc()
        return mesos_pb2.TASK_FAILED, cPickle.dumps((task.id, OtherFailure(msg), None, None))

def init_env(*args):
    env.env.start(False, *args)

class MyExecutor(mesos.Executor):
    def __init__(self):
        self.pool = None

    def init(self, driver, args):
        cwd, args = cPickle.loads(args.data)
        try: os.chdir(cwd)
        except: pass
        if self.pool:
            self.shutdown(driver)
        self.pool = multiprocessing.Pool(16, init_env, args)
        driver.sendFrameworkMessage('inited %d' % os.getpid() )

    def launchTask(self, driver, task):
        reply_status(driver, task, mesos_pb2.TASK_RUNNING)
        def callback((state, data)):
            reply_status(driver, task, state, data)
        t, aid = cPickle.loads(task.data)
        self.pool.apply_async(run_task, [t, aid], callback=callback)
        driver.sendFrameworkMessage('launch task %s' % t)
    
    def killTask(self, driver, taskId):
        driver.sendFrameworkMessage('kill task %s' % taskId)

    def shutdown(self, driver):
        self.pool.close()
        self.pool.join()
        driver.sendFrameworkMessage('shutdown')

    def error(self, driver, code, message):
        logging.error("error: %s, %s", code, message)

    def frameworkMessage(self, driver, data):
        driver.sendFrameworkMessage('got message: %s' % str(data))
        if data == 'shutdown':
            self.shutdown()
            sys.exit(0)

if __name__ == '__main__':
    executor = MyExecutor()
    driver = mesos.MesosExecutorDriver(executor)
    executor.driver = driver
    driver.run()

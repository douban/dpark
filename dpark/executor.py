#/usr/bin/env python
import logging
import os, sys, time
import os.path
import threading
import cPickle
import socket
import multiprocessing

import mesos
import mesos_pb2

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dpark.accumulator import Accumulator
from dpark.schedule import Success, OtherFailure
from dpark.env import env

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
        d = cPickle.dumps((task.id, Success(), result, accUpdate), -1)
        if len(d) > 1024*1024:
            return mesos_pb2.TASK_FAILED, cPickle.dumps((task.id, OtherFailure("Task result is too large"), None, None), -1)
        return mesos_pb2.TASK_FINISHED, d
    except Exception, e:
        import traceback
        msg = traceback.format_exc()
        return mesos_pb2.TASK_FAILED, cPickle.dumps((task.id, OtherFailure(msg), None, None), -1)

def init_env(args):
    env.start(False, args)

class MyExecutor(mesos.Executor):
    def init(self, driver, args):
        cwd, python_path, paralell, args = cPickle.loads(args.data)
        try:
            os.chdir(cwd)
        except OSError:
            driver.sendFrameworkMessage("switch cwd failed: %s not exists!" % cwd)
        sys.path = python_path
        self.pool = multiprocessing.Pool(paralell, init_env, [args])

    def launchTask(self, driver, task):
        reply_status(driver, task, mesos_pb2.TASK_RUNNING)
        def callback((state, data)):
            reply_status(driver, task, state, data)
        t, aid = cPickle.loads(task.data)
        self.pool.apply_async(run_task, [t, aid], callback=callback)
    
    def killTask(self, driver, taskId):
        #driver.sendFrameworkMessage('kill task %s' % taskId)
        pass

    def shutdown(self, driver):
        for p in self.pool._pool:
            p.terminate()
        for p in self.pool._pool:
            p.join()

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

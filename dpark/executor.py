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

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
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
        return mesos_pb2.TASK_FINISHED, cPickle.dumps((task.id, Success(), result, accUpdate), -1)
    except Exception, e:
        import traceback
        msg = traceback.format_exc()
        return mesos_pb2.TASK_FAILED, cPickle.dumps((task.id, OtherFailure(msg), None, None), -1)

def init_env(args):
    env.start(False, args)

class MyExecutor(mesos.Executor):
    def init(self, driver, args):
        cwd, paralell, args = cPickle.loads(args.data)
        try: os.chdir(cwd)
        except: pass
        self.pool = multiprocessing.Pool(paralell, init_env, [args])
        #driver.sendFrameworkMessage('inited %d' % os.getpid() )

    def launchTask(self, driver, task):
        reply_status(driver, task, mesos_pb2.TASK_RUNNING)
        def callback((state, data)):
            reply_status(driver, task, state, data)
        t, aid = cPickle.loads(task.data)
        self.pool.apply_async(run_task, [t, aid], callback=callback)
        #driver.sendFrameworkMessage('launch task %s' % t)
    
    def killTask(self, driver, taskId):
        #driver.sendFrameworkMessage('kill task %s' % taskId)
        pass

    def shutdown(self, driver):
        self.pool.terminate()
        self.pool.join()

    def error(self, driver, code, message):
        logging.error("error: %s, %s", code, message)

    def frameworkMessage(self, driver, data):
        driver.sendFrameworkMessage('got message: %s' % str(data))
        if data == 'shutdown':
            self.shutdown(driver)
            os._exit(0)

def run():
    executor = MyExecutor()
    driver = mesos.MesosExecutorDriver(executor)
    executor.driver = driver
    driver.run()

if __name__ == '__main__':
    run()

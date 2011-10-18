#/usr/bin/env python
import logging
import os, sys, time
import threading
import pickle
import socket

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

def run_task(task, driver, cacheAddr, mapOutputAddr):
    reply_status(driver, task, mesos_pb2.TASK_RUNNING)

    try:
        env.env.start(False, cacheAddr, mapOutputAddr)
        Accumulator.clear()
        t, aid = pickle.loads(task.data)
        result = t.run(aid)
        accUpdate = Accumulator.values()
        data = pickle.dumps((t.id, Success(), result, accUpdate))
        reply_status(driver, task, mesos_pb2.TASK_FINISHED, data)
    except Exception, e:
        raise
        import traceback
        msg = traceback.format_exc()
        data = pickle.dumps((t.id, OtherFailure(msg), None, None))
        reply_status(driver, task, mesos_pb2.TASK_FAILED, data)

class MyExecutor(mesos.Executor):
    def init(self, driver, args):
        cwd, self.cacheAddr, self.mapOutputAddr = pickle.loads(args.data)
        os.chdir(cwd)
        # TODO broadcast
        self.tasks = {}

    def launchTask(self, driver, task):
        t = threading.Thread(target=run_task, 
            args=[task, driver, self.cacheAddr, self.mapOutputAddr])
        t.daemon = True
        t.start()
        self.tasks[task.task_id.value] = t

    def killTask(self, driver, taskId):
        pass

    def shutdown(self, driver):
        pass

    def error(self, driver, code, message):
        logging.error("error: %s, %s", code, message)

    def frameworkMessage(self, driver, data):
        pass

if __name__ == '__main__':
    executor = MyExecutor()
    mesos.MesosExecutorDriver(executor).run()

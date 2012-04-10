import os, sys, time
import multiprocessing
import pickle
import unittest

import logging
from dpark.executor import *

class TestTask:
    def __init__(self, id):
        self.id = id

    def run(self, aid):
        import logging
        import time
        logging.info("run task %s", self)
        return aid

class MockExecutorDriver:
    def __init__(self, executor):
        pass

    def sendStatusUpdate(self, update):
        logging.info("recv status update %s", update)

    def sendFrameworkMessage(self, data):
        logging.info("recv msg: %s", data)


class TestExecute(unittest.TestCase):
    def test_executor(self):
        executor = MyExecutor()
        driver = MockExecutorDriver(executor)
       
        args = mesos_pb2.ExecutorArgs()
        args.framework_id.value = "test"
        args.executor_id.value = "test-id"
        args.slave_id.value = "test-slave"
        args.hostname = socket.gethostname()
        args.data = marshal.dumps(("./", os.getcwd(), sys.path, 8, "", "", env.environ))
        executor.init(driver, args)
        assert executor.pool

        task = mesos_pb2.TaskDescription()
        task.name = 'test-task'
        task.task_id.value = '1'
        task.slave_id.value = 'test-slave'
        task.data = pickle.dumps((TestTask(1), 1), -1)
        executor.launchTask(driver, task)

        task.task_id.value = '2'
        task.data = pickle.dumps((TestTask(2), 1), -1)
        executor.launchTask(driver, task)
        
        executor.frameworkMessage(driver, 'data')
        executor.killTask(driver, task.task_id)
        

if __name__ == '__main__':
    unittest.main()

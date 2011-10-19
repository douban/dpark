import os, sys, time
import multiprocessing

import logging
sys.path.append('/Users/davies/work/mesos/lib/python')
from executor import *

class TestTask:
    def __init__(self, id):
        self.id = id

    def run(self, aid):
        import logging
        import time
        logging.info("run task %s", self)
        time.sleep(2)
#            logging.info("task %s complete", self.id)

class MockExecutorDriver:
    def __init__(self, executor):
        pass

    def sendStatusUpdate(self, update):
        logging.info("recv status update %s", update)

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)
    executor = MyExecutor()
    driver = MockExecutorDriver(executor)
   
    args = mesos_pb2.ExecutorArgs()
    args.framework_id.value = "test"
    args.executor_id.value = "test-id"
    args.slave_id.value = "test-slave"
    args.hostname = socket.gethostname()
    args.data = pickle.dumps(("./", "tcp://localhost:5555", "tcp://localhost:5556"))
    executor.init(driver, args)


    task = mesos_pb2.TaskDescription()
    task.name = 'test-task'
    task.task_id.value = '1'
    task.slave_id.value = 'test-slave'
    task.data = pickle.dumps((TestTask(1), 1))
    executor.launchTask(driver, task)
    time.sleep(1)

    task.task_id.value = '2'
    task.data = pickle.dumps((TestTask(2), 2))
    executor.launchTask(driver, task)
    time.sleep(1)
    
    executor.frameworkMessage(driver, 'data')
    executor.killTask(driver, task.task_id)
    time.sleep(3)
    executor.shutdown(driver)

import sys
import unittest
import socket

from dpark.job import *
import pymesos as mesos
from mesos.interface import mesos_pb2

logging.getLogger('dpark').setLevel(logging.ERROR)

class MockSchduler:
    def taskEnded(self, task, reason, result, update):
        pass
    def requestMoreResources(self):
        pass
    def jobFinished(self, job):
        pass
    def killTask(self, job_id, task_id, tried):
        pass

class MockTask:
    def __init__(self, id):
        self.id = id
    def preferredLocations(self):
        return []

class TestJob(unittest.TestCase):
    def test_job(self):
        sched = MockSchduler()
        tasks = [MockTask(i) for i in range(10)]
        job = SimpleJob(sched, tasks, 1, 10)
        ts = [job.slaveOffer('localhost') for i in range(10)]
        assert len(ts) == 10
        assert job.tasksLaunched == 10
        assert job.slaveOffer('localhost') is None
        [job.statusUpdate(t.id, 0, mesos_pb2.TASK_FINISHED) for t in ts]
        assert job.tasksFinished == 10

    def test_retry(self):
        sched = MockSchduler()
        tasks = [MockTask(i) for i in range(10)]
        job = SimpleJob(sched, tasks)
        ts = [job.slaveOffer('localhost') for i in range(10)]
        [job.statusUpdate(t.id, 0, mesos_pb2.TASK_FINISHED) for t in ts[1:]]
        assert job.tasksFinished == 9
        job.statusUpdate(ts[0].id, 0, mesos_pb2.TASK_FAILED)
        t = job.slaveOffer('localhost1')
        assert t.id == 0
        assert job.slaveOffer('localhost') is None
        assert job.tasksLaunched == 10
        job.statusUpdate(t.id, 1, mesos_pb2.TASK_FINISHED)
        assert job.tasksFinished == 10

if __name__ == '__main__':
    sys.path.append('../')
    logging.basicConfig(level=logging.INFO)
    unittest.main()

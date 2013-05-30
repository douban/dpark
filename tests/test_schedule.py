import sys
import pickle
import unittest

from dpark.task import *
from dpark.schedule import *
from dpark.env import env
from dpark.context import parse_options

class MockDriver:
    def __init__(self):
        self.tasks = []
    def reviveOffers(self):
        pass
    def launchTasks(self, oid, tasks):
        self.tasks.extend(tasks)


class TestScheduler(unittest.TestCase):
    def setUp(self):
        return
        env.start(True)
        self.sched = MesosScheduler('mesos://localhost:5050', parse_options())
        self.driver = MockDriver()
        self.sched.driver = self.driver
        self.sched.start()

    def test_sched(self):
        return
        sched = self.sched
        driver = self.driver

        self.sched.registered(driver, "test")
        assert self.sched.isRegistered is True

        info = self.sched.getExecutorInfo()
        assert isinstance(info, mesos_pb2.ExecutorInfo)

        tasks = [Task() for i in range(10)]
        sched.submitTasks(tasks)
        assert len(sched.activeJobs) == 1
        assert len(sched.activeJobQueue) == 1
        assert len(sched.jobTasks) == 1

        offer = mesos_pb2.SlaveOffer()
        offer.hostname = 'localhost'
        offer.slave_id.value = '1'
        cpus = offer.resources.add()
        cpus.name = 'cpus'
        cpus.type = mesos_pb2.Resource.SCALAR
        cpus.scalar.value = 15
        mem = offer.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Resource.SCALAR
        mem.scalar.value = 2000
        sched.resourceOffer(driver, "1", [offer])
        assert len(driver.tasks)  == 10
        assert len(sched.jobTasks[1]) == 10
        assert len(sched.taskIdToSlaveId) == 10

        status = mesos_pb2.TaskStatus()
        status.state = 3
        status.task_id.value = ":1:"
        status.data = pickle.dumps((1, OtherFailure("failed"), [], {}))
        sched.statusUpdate(driver, status)
        assert len(self.taskIdToSlaveId) == 9

        sched.resourceOffer(driver, "2", [offer])
        assert len(self.driver.tasks)  == 11

        status.state = 2 # finished
        for i in range(10):
            status.task_id.value = ":%s:" % str(tasks[i].id)
            sched.statusUpdate(driver, status)
        assert len(sched.jobTasks) == 0
        assert len(sched.taskIdToSlaveId) == 0
        assert len(sched.activeJobs) == 0
        assert len(sched.activeJobQueue) == 0

        sched.error(driver, 1, 'error')

if __name__ == '__main__':
    unittest.main()

from __future__ import absolute_import
import sys
import time
import unittest
import logging

from dpark.taskset import TaskSet
from dpark.hostatus import HostStatus, TaskHostManager
from dpark.task import TaskState, OtherFailure, TaskEndReason, DAGTask
from six.moves import range
from addict import Dict

logging.getLogger('dpark').setLevel(logging.ERROR)


class MockSchduler:

    def taskEnded(self, task, reason, result, update, stats):
        pass

    def requestMoreResources(self):
        pass

    def tasksetFinished(self, taskset):
        pass

    def killTask(self, task_id, tried):
        pass


class MockTask(DAGTask):

    def __init__(self, id):
        DAGTask.__init__(self, 1, 1, id)

    def preferredLocations(self):
        return []


def create_offer(hostname):
    offer = Dict()
    offer.hostname = hostname
    return offer


class TestTaskSet(unittest.TestCase):

    def test_taskset(self):
        sched = MockSchduler()
        tasks = [MockTask(i) for i in range(10)]
        taskset = TaskSet(sched, tasks, 1, 10)
        offer = create_offer('localhost')
        host_offers = {'localhost': (0, offer)}
        taskset.task_host_manager.register_host('localhost')
        cpus = [10]
        mems = [10]
        gpus = [0]
        # the return of taskOffer is a list whose item is TUPLE with Index of offer,
        # information of Offer,
        # description of Task
        ts = sum([taskset.taskOffer(host_offers, cpus, mems, gpus) for i in range(10)], [])
        assert len(ts) == 10
        assert taskset.counter.launched == 10
        assert not taskset.taskOffer(host_offers, cpus, mems, gpus)
        [taskset.statusUpdate(t[2].id, 1, TaskState.finished) for t in ts]
        assert taskset.counter.finished == 10

    def test_retry(self):
        sched = MockSchduler()
        tasks = [MockTask(i) for i in range(10)]
        taskset = TaskSet(sched, tasks, 1, 10)
        offer = create_offer('localhost')
        host_offers = {'localhost': (0, offer)}
        # the host register should with purge elapsed 0, otherwise the failure
        # will forbit the localhost
        taskset.task_host_manager.register_host('localhost', purge_elapsed=0)
        cpus = [1]
        mems = [10]
        gpus = [0]
        ts = sum([taskset.taskOffer(host_offers=host_offers, cpus=cpus,
                                   mems=mems, gpus=gpus) for i in range(10)], [])
        [taskset.statusUpdate(t[2].id, 1, TaskState.finished) for t in ts[1:]]
        assert taskset.counter.finished == 9
        taskset.statusUpdate(ts[0][2].id, 1, TaskState.failed)
        t = taskset.taskOffer(host_offers=host_offers, cpus=cpus,
                              mems=mems, gpus=gpus)[0]
        assert t[2].id == "1_0"
        assert not taskset.taskOffer(
            host_offers=host_offers, cpus=cpus, mems=mems, gpus=gpus
        )
        assert taskset.counter.launched == 10
        taskset.statusUpdate(t[2].id, 1, TaskState.finished)
        assert taskset.counter.finished == 10


class TestHostStatus(unittest.TestCase):
    def test_single_hostatus(self):
        ht = HostStatus('localhost', purge_elapsed=3)
        ht.task_succeed(2)
        assert len(ht.succeeded_log) == 1
        assert ht.recent_succeed_rate() == 1.0
        time.sleep(1)
        ht.task_succeed(1)
        ht.task_failed(3)
        assert len(ht.succeeded_log) == 2
        assert len(ht.failed_log) == 1
        assert 3 in ht.failed_tasks
        assert ht.recent_succeed_rate() == 2.0 / 3
        time.sleep(2)
        ht.purge_old()
        assert len(ht.succeeded_log) == 1
        assert len(ht.failed_log) == 1
        assert ht.recent_succeed_rate() == 0.5
        assert ht.should_forbit(3)
        time.sleep(4)
        assert not ht.should_forbit(3)

    def test_task_host_manager(self):
        manager = TaskHostManager()
        manager.register_host('fake1', purge_elapsed=1)
        manager.register_host('fake2', purge_elapsed=1)
        manager.register_host('fake3', purge_elapsed=1)
        host_offers = {'fake1': (1, None), 'fake2': (2, None),
                       'fake3': (3, None)}
        manager.task_failed(1, 'fake2', OtherFailure('Mock failed'))
        assert manager.offer_choice(1, host_offers, ['fake3'])[0] == 1
        time.sleep(1)
        manager.task_failed(1, 'fake1', OtherFailure('Mock failed'))
        assert manager.offer_choice(1, host_offers, [])[0] == 3
        assert manager.offer_choice(1, host_offers, ['fake3'])[0] is None
        manager.task_succeed(2, 'fake2', TaskEndReason.success)
        assert manager.offer_choice(1, host_offers, ['fake3'])[0] is None
        time.sleep(1)
        assert manager.offer_choice(1, host_offers, ['fake3'])[0] == 2


if __name__ == '__main__':
    sys.path.append('../')
    logging.basicConfig(level=logging.INFO)
    unittest.main()

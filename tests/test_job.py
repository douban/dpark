from __future__ import absolute_import
import sys
import time
import math
import unittest
import logging

from dpark.job import SimpleJob
from dpark.hostatus import HostStatus, TaskHostManager
from dpark.schedule import OtherFailure, Success
from six.moves import range

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
        [job.statusUpdate(t.id, 0, 'TASK_FINISHED') for t in ts]
        assert job.tasksFinished == 10

    def test_retry(self):
        sched = MockSchduler()
        tasks = [MockTask(i) for i in range(10)]
        job = SimpleJob(sched, tasks)
        ts = [job.slaveOffer('localhost') for i in range(10)]
        [job.statusUpdate(t.id, 0, 'TASK_FINISHED') for t in ts[1:]]
        assert job.tasksFinished == 9
        job.statusUpdate(ts[0].id, 0, 'TASK_FAILED')
        t = job.slaveOffer('localhost1')
        assert t.id == 0
        assert job.slaveOffer('localhost') is None
        assert job.tasksLaunched == 10
        job.statusUpdate(t.id, 1, 'TASK_FINISHED')
        assert job.tasksFinished == 10


class TestHostStatus(unittest.TestCase):
        def test_single_hostatus(self):
            ht = HostStatus('localhost', purge_elapsed=3)
            ht.task_succeed(2)
            assert len(ht.succeeded_log) == 1
            assert ht.succeed_rate() == 1.0
            time.sleep(1)
            ht.task_succeed(1)
            ht.task_failed(3)
            assert len(ht.succeeded_log) == 2
            assert len(ht.failed_log) == 1
            assert 3 in ht.failed_tasks
            assert ht.succeed_rate() == 2.0 / 3
            time.sleep(2)
            ht.purge_old()
            assert len(ht.succeeded_log) == 1
            assert len(ht.failed_log) == 1
            assert ht.succeed_rate() == 0.5
            assert ht.has_failed_on(3)
            assert not ht.has_failed_on(2)
            assert not ht.should_forbit(3)
            ht.task_failed(3)
            assert ht.should_forbit(3)

        def test_task_host_manager(self):
            manager = TaskHostManager()
            manager.host_dict['fake1'] = HostStatus('fake1',
                                                    purge_elapsed=3)
            manager.host_dict['fake2'] = HostStatus('fake2',
                                                    purge_elapsed=3)
            manager.host_dict['fake3'] = HostStatus('fake3',
                                                    purge_elapsed=3)
            manager.task_failed(1, 'fake2', OtherFailure('Mock failed'))
            host_offers = {'fake1': (1, None), 'fake2': (2, None),
                           'fake3': (3, None)}
            assert manager.offer_choice(1, host_offers, ['fake3'])[0] == 1
            manager.task_failed(1, 'fake1', OtherFailure('Mock failed'))
            assert manager.offer_choice(1, host_offers, [])[0] == 3
            manager.task_succeed(2, 'fake2', Success())
            time.sleep(1)
            manager.task_succeed(3, 'fake1', Success())
            manager.task_failed(1, 'fake2', OtherFailure('Mock Failed'))
            assert manager.offer_choice(1, host_offers, ['fake3'])[0] == 1
            assert manager.offer_choice(1, host_offers, ['fake1', 'fake3'])[0] is None
            time.sleep(2)
            for fk in ['fake1', 'fake2', 'fake3']:
                ht = manager.host_dict[fk]
                ht.purge_old()
            assert manager.task_failed_on_host(1, 'fake2')
            assert manager.offer_choice(1, host_offers, ['fake1', 'fake3'])[0] == 2

if __name__ == '__main__':
    sys.path.append('../')
    logging.basicConfig(level=logging.INFO)
    unittest.main()

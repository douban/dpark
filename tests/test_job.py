import sys
sys.path.append('./')
import mesos
import mesos_pb2

from dpark.job import *

class MockSchduler:
    def taskEnded(self, task, reason, result, update):
        pass

    def jobFinished(self, job):
        pass

class MockTask:
    def __init__(self, id):
        self.id = id
    def preferredLocations(self):
        return []

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    sched = MockSchduler()
    tasks = [MockTask(i) for i in range(10)]
    job = SimpleJob(sched, tasks)
    ts = [job.slaveOffer('host', 5) for i in range(10)]
    assert len(ts) == 10
    time.sleep(1)
    [job.statusUpdate(t.id, mesos_pb2.TASK_FINISHED) for t in ts[1:]]
    time.sleep(1)
    assert job.slaveOffer('host', 5).id == 0
    assert job.slaveOffer('host', 5) is None
    job.statusUpdate(ts[0].id, mesos_pb2.TASK_FAILED)
    t = job.slaveOffer('host', 5)
    assert t.id == 0
    job.statusUpdate(t.id, mesos_pb2.TASK_FINISHED)

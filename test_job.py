
import mesos
import mesos_pb2

from job import *

class MockSchduler:
    def taskEnded(self, task, reason, result, update):
        pass

    def jobFinished(self, job):
        pass

class MockTask:
    def __init__(self, id):
        self.id = id
        self.preferredLocations = []

class MockOffer:
    hostname = 'host'

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    sched = MockSchduler()
    tasks = [MockTask(i) for i in range(10)]
    job = SimpleJob(sched, tasks)
    ts = [job.slaveOffer(MockOffer(), 5) for i in range(10)]
    [job.statusUpdate(t.id, mesos_pb2.TASK_FINISHED) for t in ts[1:]]
    job.statusUpdate(ts[0].id, mesos_pb2.TASK_FAILED)
    t = job.slaveOffer(MockOffer(), 5)
    job.statusUpdate(t.id, mesos_pb2.TASK_FINISHED)

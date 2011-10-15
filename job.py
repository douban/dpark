
class Job:
    def __init__(self, jobId):
        self.jobId = jobId

    def slaveOffer(self, s, availableCpus):
        pass

    def statusUpdate(self, t):
        pass

    def error(self, code, message):
        pass

    def getId():
        return self.jobId


class SimpleJob:
    def __init__(self, sched, tasks, jobId):
        self.jobId = jobId
        self.sched = sched
        self.tasks = tasks


class Job:
    def __init__(self, jobId):
        self.jobId = jobId

    def slaveOffer(self, s, availableCpus):
        raise NotImplementedError

    def statusUpdate(self, t):
        raise NotImplementedError

    def error(self, code, message):
        raise NotImplementedError

    def getId():
        return self.jobId

LOCALITY_WAIT = 5
MAX_TASK_FAILURES = 4
CPUS_PER_TASK = 1

# A Job that runs a set of tasks with no interdependencies.
class SimpleJob(Job):

    def __init__(self, sched, tasks, jobId):
        self.jobId = jobId
        self.sched = sched
        self.tasks = tasks

        self.launched = [False] * len(tasks)
        self.finished = [False] * len(tasks)
        self.numFailures = [0] * len(tasks)
        self.tidToIndex = {}

        self.tasksLaunchhes = 0
        self.tasksFinished = 0

        self.lastPreferredLaunchTime = time.time()

        self.pendingTasksForHost = {}
        self.pendingTasksWithNoPrefs = []
        self.allPendingTasks = []

        self.failed = False
        self.causeOfFailure = ""

        for i in reversed(range(len(tasks))):
            self.addPendingTask(i)

    def addPendingTask(self, i):
        loc = self.tasks[i].preferredLocations
        if not loc:
            self.pendingTasksWithNoPrefs.append(i)
        else:
            for host in loc:
                self.pendingTasksForHost.setdefault(host, []).append(i)
        self.allPendingTasks(i)

    def getPendingTasksForHost(self, host):
        return self.pendingTasksForHost.setdefault(host, [])

    def findTaskFromList(self, l):
        while l:
            i = l.pop()
            if not self.launched[i] and not self.finished[i]:
                return i

    def findTask(self, host, localOnly):
        localTask = self.findTaskFromList(self.getPendingTasksForHost(host))
        if localTask: return localTask
        noPrefTask = self.findTaskFromList(self.pendingTasksWithNoPrefs)
        if noPrefTask: return noPrefTask
        if not localOnly:
            return self.findTaskFromList(self.allPendingTasks)

    def isPreferredLocation(self, task, host):
        locs = task.preferredLocations
        return host in locs or not locs

    # Respond to an offer of a single slave from the scheduler by finding a task
    def slaveOffer(self, offer, availableCpus): 
        if self.taskLaunched >= self.numTasks or availableCpus < CPUS_PER_TASK:
            return
        now = time.time()
        localOnly = (now - self.lastPreferredLaunchTime < LOCALITY_WAIT)
        host = offer.hostname
        i =  self.findTask(host, localOnly)
        if i is not None:
            task = self.tasks[i]
            taskId = sched.newTaskId()
            preferred = isPreferredLocation(task, host)
            prefStr = preferred and "preferred" or "non-preferred"
            logging.info("Starting task %d:%d as TID %s on slave %s: %s (%s)", self.jobId, 
                    i, task, offer, host, prefStr)
            self.tidToIndex[tid] = i
            self.launhed[i] = True
            if preferred:
                self.lastPreferredLaunchTime = now
            return task

    def statusUpdate(self, status):
        if status == TASK_FINISHED:
            self.taskFinished(status)
        elif statue in (TASK_LOST, TASK_FAILED, TASK_KILLED):
            self.taskLost(status)

    def taskFinished(self, tid, status, result, update):
        i = self.tidToIndex[tid]
        if not self.finished[i]:
            self.finished[i] = True
            logging.info("Finished TID %s (progress: %d/%d)", tid, self.tasksFinished, self.numTasks)
            self.sched.taskEnded(self.tasks[i], Success, result, update)
            if self.tasksFinished == self.numTasks:
                self.sched.jobFinished(self)
        else:
            logging.info("Ignoring task-finished event for TID %d because task %d is already finished", tid, index)

    def taskLost(self, tid, status, reason):
        index = self.tidToIndex[tid]
        if not self.finished[index]:
            logging.info("Lost TID %s (task %d:%d)", tid, self.jobId, index)
            self.launched[index] = False
            if isinstance(reason, FetchFailed):
                logging.info("Loss was due to fetch failure from %s", reason.serverUri)
                self.sched.taskEnded(self.tasks[index], reason, None, None)
                self.finished[index] = True
                if self.tasksFinished == self.numTasks:
                    self.sched.jobFinished(self)
                return
            # On other failures, re-enqueue the task as pending for a max number of retries
            self.addPendingTask(index)
            if status == TASK_FAILED || TASK_LOST:
                self.numFailures[index] += 1
                if self.numFailures[index] > MAX_TASK_FAILURES:
                    logging.error("Task %d:%d failed more than %d times; aborting job", index, MAX_TASK_FAILURES)
                    self.abort("Task %d failed more than %d times" % (index, MAX_TASK_FAILURES))

        else:
            logging.info("Ignoring task-lost event for TID %d because task %d is already finished")

    def error(self, code, message):
        self.abort("Mesos error: %s (error code: %d)" % (message, code))

    def abort(self, message):
        self.failed = true
        self.causeOfFailure = message
        sche.jobFinished(self)

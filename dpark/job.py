import sys
import time
import socket
from operator import itemgetter

from dpark.util import get_logger
logger = get_logger(__name__)


def readable(size):
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit = 0
    while size > 1024:
        size /= 1024.0
        unit += 1
    return '%.1f%s' % (size, units[unit])


class Job:

    def __init__(self):
        self.id = self.newJobId()
        self.start = time.time()

    def slaveOffer(self, s, availableCpus):
        raise NotImplementedError

    def statusUpdate(self, t):
        raise NotImplementedError

    def error(self, code, message):
        raise NotImplementedError

    nextJobId = 0

    @classmethod
    def newJobId(cls):
        cls.nextJobId += 1
        return cls.nextJobId

LOCALITY_WAIT = 0
WAIT_FOR_RUNNING = 10
MAX_TASK_FAILURES = 4
MAX_TASK_MEMORY = 15 << 10  # 15GB

# A Job that runs a set of tasks with no interdependencies.


class SimpleJob(Job):

    def __init__(self, sched, tasks, cpus=1, mem=100):
        Job.__init__(self)
        self.sched = sched
        self.tasks = tasks

        for t in tasks:
            t.status = None
            t.tried = 0
            t.used = 0
            t.cpus = cpus
            t.mem = mem

        self.launched = [False] * len(tasks)
        self.finished = [False] * len(tasks)
        self.numFailures = [0] * len(tasks)
        self.blacklist = [[] for i in xrange(len(tasks))]
        self.tidToIndex = {}
        self.numTasks = len(tasks)
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.total_used = 0

        self.lastPreferredLaunchTime = time.time()

        self.pendingTasksForHost = {}
        self.pendingTasksWithNoPrefs = []
        self.allPendingTasks = []

        self.reasons = set()
        self.failed = False
        self.causeOfFailure = ''
        self.last_check = 0

        for i in range(len(tasks)):
            self.addPendingTask(i)
        self.host_cache = {}

    @property
    def taskEverageTime(self):
        if not self.tasksFinished:
            return 10
        return max(self.total_used / self.tasksFinished, 5)

    def addPendingTask(self, i):
        loc = self.tasks[i].preferredLocations()
        if not loc:
            self.pendingTasksWithNoPrefs.append(i)
        else:
            for host in loc:
                self.pendingTasksForHost.setdefault(host, []).append(i)
        self.allPendingTasks.append(i)

    def getPendingTasksForHost(self, host):
        try:
            return self.host_cache[host]
        except KeyError:
            v = self._getPendingTasksForHost(host)
            self.host_cache[host] = v
            return v

    def _getPendingTasksForHost(self, host):
        try:
            h, hs, ips = socket.gethostbyname_ex(host)
        except Exception:
            h, hs, ips = host, [], []
        tasks = sum((self.pendingTasksForHost.get(h, [])
                     for h in [h] + hs + ips), [])
        st = {}
        for t in tasks:
            st[t] = st.get(t, 0) + 1
        ts = sorted(st.items(), key=itemgetter(1), reverse=True)
        return [t for t, _ in ts]

    def findTaskFromList(self, l, host, cpus, mem):
        for i in l:
            if self.launched[i] or self.finished[i]:
                continue
            if host in self.blacklist[i]:
                continue
            t = self.tasks[i]
            if t.cpus <= cpus + 1e-4 and t.mem <= mem:
                return i

    def findTask(self, host, localOnly, cpus, mem):
        localTask = self.findTaskFromList(
            self.getPendingTasksForHost(host), host, cpus, mem)
        if localTask is not None:
            return localTask, True
        noPrefTask = self.findTaskFromList(
            self.pendingTasksWithNoPrefs, host, cpus, mem)
        if noPrefTask is not None:
            return noPrefTask, True
        if not localOnly:
            return self.findTaskFromList(
                self.allPendingTasks, host, cpus, mem), False
#        else:
#            print repr(host), self.pendingTasksForHost
        return None, False

    # Respond to an offer of a single slave from the scheduler by finding a
    # task
    def slaveOffer(self, host, availableCpus=1, availableMem=100):
        now = time.time()
        localOnly = (now - self.lastPreferredLaunchTime < LOCALITY_WAIT)
        i, preferred = self.findTask(
            host, localOnly, availableCpus, availableMem)
        if i is not None:
            task = self.tasks[i]
            task.status = 'TASK_STAGING'
            task.start = now
            task.host = host
            task.tried += 1
            prefStr = preferred and 'preferred' or 'non-preferred'
            logger.debug('Starting task %d:%d as TID %s on slave %s (%s)',
                         self.id, i, task, host, prefStr)
            self.tidToIndex[task.id] = i
            self.launched[i] = True
            self.tasksLaunched += 1
            self.blacklist[i].append(host)
            if preferred:
                self.lastPreferredLaunchTime = now
            return task
        logger.debug('no task found %s', localOnly)

    def statusUpdate(self, tid, tried, status, reason=None,
                     result=None, update=None):
        logger.debug('job status update %s %s %s', tid, status, reason)
        if tid not in self.tidToIndex:
            logger.error('invalid tid: %s', tid)
            return
        i = self.tidToIndex[tid]
        if self.finished[i]:
            if status == 'TASK_FINISHED':
                logger.debug('Task %d is already finished, ignore it', tid)
            return

        task = self.tasks[i]
        task.status = status
        # when checking, task been masked as not launched
        if not self.launched[i]:
            self.launched[i] = True
            self.tasksLaunched += 1

        if status == 'TASK_FINISHED':
            self.taskFinished(tid, tried, result, update)
        elif status in ('TASK_LOST', 'TASK_FAILED', 'TASK_KILLED'):
            self.taskLost(tid, tried, status, reason)
        task.start = time.time()

    def taskFinished(self, tid, tried, result, update):
        i = self.tidToIndex[tid]
        self.finished[i] = True
        self.tasksFinished += 1
        task = self.tasks[i]
        task.used += time.time() - task.start
        self.total_used += task.used
        if getattr(sys.stderr, 'isatty', lambda: False)():
            title = 'Job %d: task %s finished in %.1fs (%d/%d)     ' % (
                self.id, tid, task.used, self.tasksFinished, self.numTasks)
            logger.info('Task %s finished in %.1fs (%d/%d)'
                        '      \x1b]2;%s\x07\x1b[1A',
                        tid, task.used, self.tasksFinished,
                        self.numTasks, title)

        from dpark.schedule import Success
        self.sched.taskEnded(task, Success(), result, update)

        for t in range(task.tried):
            if t + 1 != tried:
                self.sched.killTask(self.id, task.id, t + 1)

        if self.tasksFinished == self.numTasks:
            ts = [t.used for t in self.tasks]
            tried = [t.tried for t in self.tasks]
            logger.info('Job %d finished in %.1fs: min=%.1fs, '
                        'avg=%.1fs, max=%.1fs, maxtry=%d',
                        self.id, time.time() - self.start,
                        min(ts), sum(ts) / len(ts), max(ts), max(tried))
            from dpark.accumulator import LocalReadBytes, RemoteReadBytes
            lb, rb = LocalReadBytes.reset(), RemoteReadBytes.reset()
            if rb > 0:
                logger.info('read %s (%d%% localized)',
                            readable(lb + rb), lb * 100 / (rb + lb))

            self.sched.jobFinished(self)

    def taskLost(self, tid, tried, status, reason):
        index = self.tidToIndex[tid]

        from dpark.schedule import FetchFailed
        if isinstance(reason, FetchFailed) and self.numFailures[index] >= 1:
            logger.warning('Task %s was Lost due to fetch failure from %s',
                           tid, reason.serverUri)
            self.sched.taskEnded(self.tasks[index], reason, None, None)
            # cancel tasks
            if not self.finished[index]:
                self.finished[index] = True
                self.tasksFinished += 1
            for i in range(len(self.finished)):
                if not self.launched[i]:
                    self.launched[i] = True
                    self.tasksLaunched += 1
                    self.finished[i] = True
                    self.tasksFinished += 1
            if self.tasksFinished == self.numTasks:
                self.sched.jobFinished(self)  # cancel job
            return

        task = self.tasks[index]
        if status == 'TASK_KILLED':
            task.mem = min(task.mem * 2, MAX_TASK_MEMORY)
            for i, t in enumerate(self.tasks):
                if not self.launched[i]:
                    t.mem = max(task.mem, t.mem)

        elif status == 'TASK_FAILED':
            _logger = logger.error if self.numFailures[index] == MAX_TASK_FAILURES\
                else logger.warning
            if reason not in self.reasons:
                _logger(
                    'task %s failed @ %s: %s\n%s',
                    task.id,
                    task.host,
                    task,
                    reason)
                self.reasons.add(reason)
            else:
                _logger('task %s failed @ %s: %s', task.id, task.host, task)

        elif status == 'TASK_LOST':
            logger.warning('Lost Task %d (task %d:%d:%s) %s at %s',
                           index, self.id, tid, tried, reason, task.host)

        self.numFailures[index] += 1
        if self.numFailures[index] > MAX_TASK_FAILURES:
            logger.error('Task %d failed more than %d times; aborting job',
                         index, MAX_TASK_FAILURES)
            self.abort('Task %d failed more than %d times'
                       % (index, MAX_TASK_FAILURES))

        self.launched[index] = False
        if self.tasksLaunched == self.numTasks:
            self.sched.requestMoreResources()
        for i in xrange(len(self.blacklist)):
            self.blacklist[i] = []
        self.tasksLaunched -= 1

    def check_task_timeout(self):
        now = time.time()
        if self.last_check + 5 > now:
            return False
        self.last_check = now

        n = self.launched.count(True)
        if n != self.tasksLaunched:
            logger.warning(
                'bug: tasksLaunched(%d) != %d',
                self.tasksLaunched,
                n)
            self.tasksLaunched = n

        for i in xrange(self.numTasks):
            task = self.tasks[i]
            if (self.launched[i] and task.status == 'TASK_STAGING'
                    and task.start + WAIT_FOR_RUNNING < now):
                logger.debug('task %d timeout %.1f (at %s), re-assign it',
                             task.id, now - task.start, task.host)
                self.launched[i] = False
                self.tasksLaunched -= 1

        if self.tasksFinished > self.numTasks * 2.0 / 3:
            scale = 1.0 * self.numTasks / self.tasksFinished
            avg = max(self.taskEverageTime, 10)
            tasks = sorted((task.start, i, task)
                           for i, task in enumerate(self.tasks)
                           if self.launched[i] and not self.finished[i])
            for _t, idx, task in tasks:
                used = now - task.start
                if used > avg * (2 ** task.tried) * scale:
                    # re-submit timeout task
                    if task.tried <= MAX_TASK_FAILURES:
                        logger.debug('re-submit task %s for timeout %.1f, '
                                     'try %d', task.id, used, task.tried)
                        task.used += used
                        task.start = now
                        self.launched[idx] = False
                        self.tasksLaunched -= 1
                    else:
                        logger.error('task %s timeout, aborting job %s',
                                     task, self.id)
                        self.abort('task %s timeout' % task)
                else:
                    break
        return self.tasksLaunched < n

    def abort(self, message):
        logger.error('abort the job: %s', message)
        tasks = ' '.join(str(i) for i in xrange(len(self.finished))
                         if not self.finished[i])
        logger.error('not finished tasks: %s', tasks)
        self.failed = True
        self.causeOfFailure = message
        self.sched.jobFinished(self)
        self.sched.shutdown()

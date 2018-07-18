from __future__ import absolute_import, print_function
import math
import time
import socket
from operator import itemgetter

from dpark.utils.tdigest import TDigest
from dpark.utils.log import (
    get_logger, make_progress_bar
)
from dpark.hostatus import TaskHostManager
from six.moves import range

logger = get_logger(__name__)


def readable(size):
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit = 0
    while size > 1024:
        size /= 1024.0
        unit += 1
    return '%.1f%s' % (size, units[unit])


class Job(object):

    next_job_id = 0

    def __init__(self):
        self.id = self.new_job_id()
        self.start = time.time()

    @classmethod
    def new_job_id(cls):
        cls.next_job_id += 1
        return cls.next_job_id


LOCALITY_WAIT = 0
WAIT_FOR_RUNNING = 10
MAX_TASK_FAILURES = 4
MAX_TASK_MEMORY = 20 << 10  # 20GB


# A Job that runs a set of tasks with no interdependencies.


class SimpleJob(Job):

    def __init__(self, sched, tasks, cpus=1, mem=100, gpus=0,
                 task_host_manager=None):
        Job.__init__(self)
        self.sched = sched
        self.tasks = tasks

        for t in self.tasks:
            t.status = None
            t.tried = 0
            t.used = 0
            t.cpus = cpus
            t.mem = mem
            t.gpus = gpus

        self.launched = [False] * len(tasks)
        self.finished = [False] * len(tasks)
        self.numFailures = [0] * len(tasks)
        self.running_hosts = [[] for _ in range(len(tasks))]
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
        self.task_host_manager = task_host_manager if task_host_manager is not None \
            else TaskHostManager()
        self.id_retry_host = {}
        self.task_local_set = set()
        self.mem_digest = TDigest()
        self.mem90 = 0

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
        ts = sorted(list(st.items()), key=itemgetter(1), reverse=True)
        return [t for t, _ in ts]

    def findTaskFromList(self, l, host, cpus, mem, gpus):
        for i in l:
            if self.launched[i] or self.finished[i]:
                continue
            if host in self.running_hosts[i]:
                continue
            t = self.tasks[i]
            if self.task_host_manager.task_failed_on_host(t.id, host):
                continue
            if t.cpus <= cpus + 1e-4 and t.mem <= mem and t.gpus <= gpus:
                return i

    def taskOffer(self, host_offers, cpus, mems, gpus):
        prefer_list = []
        for host in host_offers:
            i, o = host_offers[host]
            local_task = self.findTaskFromList(
                self.getPendingTasksForHost(host), host,
                cpus[i], mems[i], gpus[i])
            if local_task is not None:
                result_tuple = self._try_update_task_offer(local_task, i, o, cpus, mems, gpus)
                if result_tuple is None:
                    continue
                prefer_list.append(result_tuple)
        if prefer_list:
            return prefer_list
        for idx in range(len(self.tasks)):
            if not self.launched[idx] and not self.finished[idx]:
                i, o = self.task_host_manager.offer_choice(self.tasks[idx].id, host_offers,
                                                           self.running_hosts[idx])
                if i is None:
                    continue
                result_tuple = self._try_update_task_offer(idx, i, o, cpus, mems, gpus)
                if result_tuple:
                    return [result_tuple]
        return []

    def _try_update_task_offer(self, task_idx, i, o, cpus, mem, gpus):
        t = self.tasks[task_idx]
        if t.cpus <= cpus[i] + 1e-4 and t.mem <= mem[i] and t.gpus <= gpus[i]:
            t.status = 'TASK_STAGING'
            t.start = time.time()
            t.host = o.hostname
            t.tried += 1
            self.id_retry_host[(t.id, t.tried)] = o.hostname
            logger.debug('Starting task %d:%d as TID %s on slave %s',
                         self.id, task_idx, t, o.hostname)
            self.tidToIndex[t.id] = task_idx
            self.launched[task_idx] = True
            self.tasksLaunched += 1
            self.running_hosts[task_idx].append(o.hostname)
            host_set = set(self.tasks[task_idx].preferredLocations())
            if o.hostname in host_set:
                self.task_local_set.add(t.id)
            return i, o, t
        return None

    def statusUpdate(self, tid, tried, status, reason=None,
                     result=None, update=None, stats=None):
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
            self._task_finished(tid, tried, result, update, stats)
        elif status in ('TASK_LOST', 'TASK_FAILED', 'TASK_KILLED'):
            self._task_lost(tid, tried, status, reason)

        task.start = time.time()
        if stats:
            self.mem_digest.add(stats.bytes_max_rss / (1024. ** 2))

    def progress(self, ending=''):
        n = self.numTasks
        ratio = self.tasksFinished * 1. / n
        bar = make_progress_bar(ratio)
        if self.tasksFinished:
            elasped = time.time() - self.start
            avg = self.total_used / self.tasksFinished
            eta = (n - self.tasksFinished) * elasped / self.tasksFinished
            m, s = divmod(int(eta), 60)
            h, m = divmod(m, 60)

            tmpl = 'Job:%4s {{GREEN}}%s{{RESET}}%5.1f%% (% {width}s/% {width}s) ETA:% 2d:%02d:%02d AVG:%.1fs\x1b[K%s'
            fmt = tmpl.format(width=int(math.log10(self.numTasks)) + 1)

            msg = fmt % (
                self.id, bar, ratio * 100, self.tasksFinished, n, h, m, s,
                avg, ending
            )
            msg = msg.ljust(80)
            logger.info(msg)
        else:

            tmpl = 'Job:%4s {{GREEN}}%s{{RESET}}%5.1f%% (% {width}s/% {width}s) ETA:--:--:-- AVG:N/A\x1b[K%s'
            fmt = tmpl.format(width=int(math.log10(self.numTasks)) + 1)

            msg = fmt % (self.id, bar, ratio * 100, self.tasksFinished, n, ending)
            msg = msg.ljust(80)
            logger.info(msg)

    def _task_finished(self, tid, tried, result, update, stats):
        i = self.tidToIndex[tid]
        self.finished[i] = True
        self.tasksFinished += 1
        task = self.tasks[i]
        hostname = self.id_retry_host[(task.id, tried)] \
            if (task.id, tried) in self.id_retry_host else task.host
        task.used += time.time() - task.start
        self.total_used += task.used
        if getattr(self.sched, 'color', False):
            title = 'Job %d: task %s finished in %.1fs (%d/%d)     ' % (
                self.id, tid, task.used, self.tasksFinished, self.numTasks)
            msg = '\x1b]2;%s\x07\x1b[1A' % title
            logger.info(msg)

        from dpark.schedule import Success
        self.sched.taskEnded(task, Success(), result, update, stats)
        self.running_hosts[i] = []
        self.task_host_manager.task_succeed(task.id, hostname,
                                            Success())

        for t in range(task.tried):
            if t + 1 != tried:
                self.sched.killTask(self.id, task.id, t + 1)

        if self.tasksFinished == self.numTasks:
            ts = [t.used for t in self.tasks]
            tried = [t.tried for t in self.tasks]
            elasped = time.time() - self.start
            logger.info('Job %d finished in %.1fs: min=%.1fs, '
                        'avg=%.1fs, max=%.1fs, maxtry=%d, speedup=%.1f, local=%.1f%%',
                        self.id, elasped, min(ts), sum(ts) / len(ts), max(ts),
                        max(tried), self.total_used / elasped,
                        len(self.task_local_set) * 100. / len(self.tasks)
                        )
            self.sched.jobFinished(self)

    def _task_lost(self, tid, tried, status, reason):
        index = self.tidToIndex[tid]

        from dpark.schedule import FetchFailed
        if isinstance(reason, FetchFailed) and self.numFailures[index] >= 1:
            logger.warning('Cancel task %s after fetch fail twice from %s',
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
        hostname = self.id_retry_host[(task.id, tried)] \
            if (task.id, tried) in self.id_retry_host else task.host

        if status == 'TASK_KILLED' or str(reason).startswith('Memory limit exceeded:'):
            task.mem = min(task.mem * 2, MAX_TASK_MEMORY)
            logger.info("task %s oom, enlarge memory limit to %d, origin %d", task.id, task.mem, task.rdd.mem)

            mem90 = self.mem_digest.quantile(0.9)
            if not math.isnan(mem90):
                mem90 = int(mem90)
                if mem90 > self.mem90:
                    logger.info("enlarge memory limit of remaining task from >%d to >%d (mem90)", self.mem90, mem90)
                    self.mem90 = mem90
                    for i, t in enumerate(self.tasks):
                        if not self.launched[i]:
                            t.mem = max(mem90, t.mem)

        elif status == 'TASK_FAILED':
            _logger = logger.error if self.numFailures[index] == MAX_TASK_FAILURES \
                else logger.warning
            if reason not in self.reasons:
                _logger(
                    'task %s failed @ %s: %s : %s',
                    task.id,
                    hostname,
                    task,
                    reason)
                self.reasons.add(reason)
            else:
                _logger('task %s failed @ %s: %s', task.id, hostname, task)

        elif status == 'TASK_LOST':
            logger.warning('Lost Task %d (task %d:%d:%s) %s at %s',
                           index, self.id, tid, tried, reason, task.host)

        self.numFailures[index] += 1
        if self.numFailures[index] > MAX_TASK_FAILURES:
            logger.error('Task %d failed more than %d times; aborting job',
                         self.tasks[index].id, MAX_TASK_FAILURES)
            self._abort('Task %d failed more than %d times' % (self.tasks[index].id, MAX_TASK_FAILURES))
        self.task_host_manager.task_failed(task.id, hostname, reason)
        self.launched[index] = False
        if self.tasksLaunched == self.numTasks:
            self.sched.requestMoreResources()
        self.running_hosts[index] = []
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

        for i in range(self.numTasks):
            task = self.tasks[i]
            if (self.launched[i] and task.status == 'TASK_STAGING'
                    and task.start + WAIT_FOR_RUNNING < now):
                logger.info('task %d timeout %.1f (at %s), re-assign it',
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
                        logger.info('re-submit task %s for timeout %.1f, '
                                    'try %d', task.id, used, task.tried)
                        task.used += used
                        task.start = now
                        self.launched[idx] = False
                        self.tasksLaunched -= 1
                    else:
                        logger.error('task %s timeout, aborting job %s',
                                     task, self.id)
                        self._abort('task %s timeout' % task)
                else:
                    break
        return self.tasksLaunched < n

    def _abort(self, message):
        logger.error('abort the job: %s', message)
        tasks = ' '.join(str(i) for i in range(len(self.finished))
                         if not self.finished[i])
        logger.error('not finished tasks: %s', tasks)
        self.failed = True
        self.causeOfFailure = message
        self.sched.jobFinished(self)
        self.sched.abort()

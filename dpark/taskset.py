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
from dpark.task import TaskState, TaskEndReason, TaskReason
from six.moves import range

logger = get_logger(__name__)


def readable(size):
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit = 0
    while size > 1024:
        size /= 1024.0
        unit += 1
    return '%.1f%s' % (size, units[unit])


LOCALITY_WAIT = 0
WAIT_FOR_RUNNING = 30
MAX_TASK_FAILURES = 4
MAX_TASK_MEMORY = 20 << 10  # 20GB


class TaskCounter(object):

    def __init__(self, n):
        self.n = n
        self.launched = 0
        self.finished = 0

        self.fail_oom = 0
        self.fail_fetch = 0
        self.fail_run_timeout = 0
        self.fail_staging_timeout = 0
        self.fail_all = 0  # include oom, not include timeout

    @property
    def running(self):
        return self.launched - self.finished

    def get_fail_types(self):
        prefix = "fail"
        return [a for a in self.__dict__ if a.startswith(prefix)]


class TaskSet(object):
    """ A TaskSet runs a set of tasks of a Stage with retry.

        - Task_id seen by TaskSet not include task.num_try
        - Each task try four times before abort.
        - Enlarge task.mem if fail for OOM.
        - Retry for lagging tasks.
    """

    def __init__(self, sched, tasks, cpus=1, mem=100, gpus=0,
                 task_host_manager=None):
        self.start_time = time.time()
        self.sched = sched
        self.tasks = tasks
        self.id = tasks[0].taskset_id
        self.ttids = set()

        for t in self.tasks:
            t.status = None
            t.num_try = 0
            t.time_used = 0
            t.cpus = cpus
            t.mem = mem
            t.gpus = gpus

        self.launched = [False] * len(tasks)
        self.finished = [False] * len(tasks)
        self.numFailures = [0] * len(tasks)
        self.running_hosts = [[] for _ in range(len(tasks))]
        self.tidToIndex = {}
        self.counter = TaskCounter(len(tasks))

        self.total_time_used = 0
        self.max_task_time = 0

        self.lastPreferredLaunchTime = time.time()

        self.pendingTasksForHost = {}
        self.pendingTasksWithNoPrefs = []
        self.allPendingTasks = []

        self.reasons = set()
        self.failed = False
        self.causeOfFailure = ''
        self.last_check = 0

        for i in range(len(tasks)):
            self._addPendingTask(i)
        self.host_cache = {}
        self.task_host_manager = task_host_manager if task_host_manager is not None \
            else TaskHostManager()
        self.id_retry_host = {}
        self.task_local_set = set()
        self.mem_digest = TDigest()
        self.max_stage_time = 0
        self.mem90 = 0  # TODO: move to stage

    @property
    def taskEverageTime(self):
        if not self.counter.finished:
            return 10
        return max(self.total_time_used / self.counter.finished, 5)

    def _addPendingTask(self, i):
        loc = self.tasks[i].preferredLocations()
        if not loc:
            self.pendingTasksWithNoPrefs.append(i)
        else:
            for host in loc:
                self.pendingTasksForHost.setdefault(host, []).append(i)
        self.allPendingTasks.append(i)

    def _getPendingTasksForHostWithCache(self, host):
        tasks = self.host_cache.get(host)
        if tasks:
            return tasks
        else:
            tasks = self._getPendingTasksForHost(host)
            self.host_cache[host] = tasks
            return tasks

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

    def _findTaskFromList(self, l, host, cpus, mem, gpus):
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
            local_task = self._findTaskFromList(
                self._getPendingTasksForHostWithCache(host), host,
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
            t.status = TaskState.staging
            t.host = o.hostname
            t.try_next()
            self.id_retry_host[(t.id, t.num_try)] = o.hostname
            logger.debug('Starting task %s on slave %s',
                         t.try_id, o.hostname)
            self.tidToIndex[t.id] = task_idx
            self.launched[task_idx] = True
            self.counter.launched += 1
            self.running_hosts[task_idx].append(o.hostname)
            host_set = set(self.tasks[task_idx].preferredLocations())
            if o.hostname in host_set:
                self.task_local_set.add(t.id)
            return i, o, t
        return None

    def statusUpdate(self, task_id, num_try, status, reason=None, message=None,
                     result=None, update=None, stats=None):
        logger.debug('taskset status update %s, status %s, reason %s', task_id, status, reason)
        if task_id not in self.tidToIndex:
            logger.error('invalid task_id: %s, status %s, reason %s', task_id, status, reason)
            return
        i = self.tidToIndex[task_id]
        task = self.tasks[i]
        task.update_status(status, num_try)

        if self.finished[i]:
            if status == TaskState.finished:
                logger.debug('Task %s is already finished, ignore it', task_id)
            return

        # when checking, task been masked as not launched
        if not self.launched[i]:
            self.launched[i] = True
            self.counter.launched += 1

        if status == TaskState.running:
            task.start_time = time.time()
            self.max_stage_time = max(self.max_stage_time, task.start_time - task.stage_time)
        elif status == TaskState.finished:
            if stats:
                self.mem_digest.add(stats.bytes_max_rss / (1024. ** 2))
            if task.tries[num_try].reason in (TaskReason.run_timeout, TaskReason.stage_timeout):
                logger.warning("task timeout works: try %s finshed. History: %s",
                               num_try, ". ".join(map(str, task.tries.values())))

            self._task_finished(task_id, num_try, result, update, stats)
        else:  # failed, killed, lost, error
            self._task_lost(task_id, num_try, status, reason, message, exception=result)

    def progress(self, ending=''):
        n = self.counter.n
        ratio = self.counter.finished * 1. / n
        bar = make_progress_bar(ratio)
        if self.counter.finished:
            elasped = time.time() - self.start_time
            avg = self.total_time_used / self.counter.finished
            eta = (n - self.counter.finished) * elasped / self.counter.finished
            m, s = divmod(int(eta), 60)
            h, m = divmod(m, 60)

            tmpl = 'taskset:%4s {{GREEN}}%s{{RESET}}%5.1f%% (% {width}s/% {width}s) ETA:% 2d:%02d:%02d AVG:%.1fs\x1b[K%s'
            fmt = tmpl.format(width=int(math.log10(self.counter.n)) + 1)

            msg = fmt % (
                self.id, bar, ratio * 100, self.counter.finished, n, h, m, s,
                avg, ending
            )
            msg = msg.ljust(80)
            logger.info(msg)
        else:

            tmpl = 'taskset:%4s {{GREEN}}%s{{RESET}}%5.1f%% (% {width}s/% {width}s) ETA:--:--:-- AVG:N/A\x1b[K%s'
            fmt = tmpl.format(width=int(math.log10(self.counter.n)) + 1)

            msg = fmt % (self.id, bar, ratio * 100, self.counter.finished, n, ending)
            msg = msg.ljust(80)
            logger.info(msg)

    def _task_finished(self, task_id, num_try, result, update, stats):
        i = self.tidToIndex[task_id]
        self.finished[i] = True
        self.counter.finished += 1
        task = self.tasks[i]
        hostname = self.id_retry_host[(task.id, num_try)] \
            if (task.id, num_try) in self.id_retry_host else task.host
        task.time_used += time.time() - task.start_time
        self.total_time_used += task.time_used
        self.max_task_time = max(self.max_task_time, task.time_used)
        if getattr(self.sched, 'color', False):
            title = 'taskset %s: task %s finished in %.1fs (%d/%d)     ' % (
                self.id, task_id, task.time_used, self.counter.finished, self.counter.n)
            msg = '\x1b]2;%s\x07\x1b[1A' % title
            logger.info(msg)

        self.sched.taskEnded(task, TaskEndReason.success, result, update, stats)
        self.running_hosts[i] = []
        self.task_host_manager.task_succeed(task.id, hostname,
                                            TaskEndReason.success)

        for t in range(task.num_try):
            if t + 1 != num_try:
                self.sched.killTask(task.id, t + 1)

        if self.counter.finished == self.counter.n:
            ts = [t.time_used for t in self.tasks]
            num_try = [t.num_try for t in self.tasks]
            elasped = time.time() - self.start_time
            logger.info('taskset %s finished in %.1fs: min=%.1fs, '
                        'avg=%.1fs, max=%.1fs, maxtry=%d, speedup=%.1f, local=%.1f%%',
                        self.id, elasped, min(ts), sum(ts) / len(ts), max(ts),
                        max(num_try), self.total_time_used / elasped,
                        len(self.task_local_set) * 100. / len(self.tasks)
                        )
            self.sched.tasksetFinished(self)

    def _task_lost(self, task_id, num_try, status, reason, message, exception=None):
        index = self.tidToIndex[task_id]
        task = self.tasks[index]

        if reason == TaskEndReason.fetch_failed and self.numFailures[index] >= 1:
            self.counter.fail_fetch += 1
            logger.warning('Cancel task %s after fetch fail twice from %s',
                           task_id, exception.serverUri)
            self.sched.taskEnded(self.tasks[index], reason, exception, None)
            # cancel tasks
            if not self.finished[index]:
                self.finished[index] = True
                self.counter.finished += 1
            for i in range(len(self.finished)):
                if not self.launched[i]:
                    self.launched[i] = True
                    self.counter.launched += 1
                    self.finished[i] = True
                    self.counter.finished += 1
            if self.counter.finished == self.counter.n:
                self.sched.tasksetFinished(self)  # cancel taskset
            return

        hostname = self.id_retry_host[(task.id, num_try)] \
            if (task.id, num_try) in self.id_retry_host else task.host

        abort = (self.numFailures[index] >= MAX_TASK_FAILURES)

        if TaskEndReason.maybe_oom(reason):
            self.counter.fail_oom += 1
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

        else:
            _logger = logger.error if abort else logger.warning

            err_msg_simple = '{} id: {}, host: {}, reason: {}'.format(status, task.id, hostname, reason)
            err_msg = "{} message: {}".format(err_msg_simple, message)

            if status == TaskState.failed:
                if reason is not None:  # for tests with master=local
                    if reason.startswith("FATAL_EXCEPTION"):
                        self._abort('Job abort without retry for {}.'.format(reason))
                if reason not in self.reasons:
                    self.reasons.add(reason)
                elif not abort:
                    err_msg = err_msg_simple
            _logger(err_msg)

        self.counter.fail_all += 1
        self.numFailures[index] += 1

        if abort:
            self._abort('Task %s failed more than %d times' % (self.tasks[index].id, MAX_TASK_FAILURES))

        task.reason_next = "fail"

        self.task_host_manager.task_failed(task.id, hostname, reason)
        self.launched[index] = False
        if self.counter.launched == self.counter.n:
            self.sched.requestMoreResources()
        self.running_hosts[index] = []
        self.counter.launched -= 1

    def check_task_timeout(self):
        """In lock, so be fast!"""

        now = time.time()
        if self.last_check + 5 > now:
            return False
        self.last_check = now

        n = self.launched.count(True)
        if n != self.counter.launched:
            logger.warning(
                'bug: counter.launched(%d) != %d',
                self.counter.launched,
                n)
            self.counter.launched = n

        # staged but not run for too long
        # mesos may be busy.
        num_resubmit = 0
        for i in range(self.counter.n):
            task = self.tasks[i]
            if (self.launched[i] and task.status == TaskState.staging
                    and task.stage_time + self.max_stage_time + WAIT_FOR_RUNNING < now):
                logger.warning('task %s staging timeout %.1f (at %s), re-assign it',
                               task.id, now - task.stage_time, task.host)
                self.counter.fail_staging_timeout += 1
                task.reason_next = TaskReason.stage_timeout
                self.launched[i] = False
                self.counter.launched -= 1
                num_resubmit += 1
                if num_resubmit > 3:
                    break

        # running for too long
        num_resubmit = 0
        if self.counter.finished > self.counter.n * 0.8:
            scale = 1.0 * self.counter.n / self.counter.finished
            tasks = sorted((task.start_time, i, task)
                           for i, task in enumerate(self.tasks)
                           if self.launched[i] and not self.finished[i] and task.status == TaskState.running)
            for _t, idx, task in tasks:
                time_used = now - task.start_time
                if time_used > self.max_task_time * (4 ** task.num_try) * scale:  # num_try starts from 1
                    # re-submit timeout task
                    self.counter.fail_run_timeout += 1
                    if task.num_try <= MAX_TASK_FAILURES:
                        logger.info('re-submit task %s for run timeout %.1f, max finished = %d, try %d',
                                    task.id, time_used, int(self.max_task_time), task.num_try)
                        task.time_used += time_used
                        task.stage_time = 0
                        task.start_time = 0
                        self.launched[idx] = False
                        self.counter.launched -= 1
                        task.reason_next = TaskReason.run_timeout
                    else:
                        logger.error('task %s timeout, aborting taskset %s',
                                     task, self.id)
                        self._abort('task %s timeout' % task)
                else:
                    break
                num_resubmit += 1
                if num_resubmit > 3:
                    break
        return self.counter.launched < n

    def _abort(self, message):
        logger.error('abort the taskset: %s', message)
        tasks = ' '.join(str(i) for i in range(len(self.finished))
                         if not self.finished[i])
        logger.error('not finished tasks: %s', tasks)
        self.failed = True
        self.causeOfFailure = message
        self.sched.tasksetFinished(self)
        self.sched.abort()

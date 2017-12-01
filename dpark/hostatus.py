import time
import random
from six.moves import filter
from dpark.util import get_logger
PURGE_ELAPSED = 60 * 5
FAILED_TIMES = 2
logger = get_logger(__name__)


class HostStatus:
    def __init__(self, hostname, purge_elapsed=PURGE_ELAPSED,
                 tolerate_times=FAILED_TIMES):
        self.hostname = hostname
        self.failed_log = []
        self.succeeded_log = []
        self.failed_cnt = 0
        self.succeed_cnt = 0
        self.duration = 0
        self.failed_tasks = {}
        self.purge_elapsed = purge_elapsed
        self.tolerate_times = tolerate_times

    def task_succeed(self, task_id):
        self.succeeded_log.append(time.time())
        if task_id in self.failed_tasks:
            del self.failed_tasks[task_id]

    def task_failed(self, task_id):
        cur_ts = time.time()
        self.failed_log.append(cur_ts)
        if task_id in self.failed_tasks:
            self.failed_tasks[task_id].append(cur_ts)
        else:
            self.failed_tasks[task_id] = [cur_ts]

    def purge_old(self):
        cur_ts = time.time()
        prev_ts = cur_ts - self.purge_elapsed
        key_list = list(self.failed_tasks)
        for task_id in key_list:
            self.failed_tasks[task_id] = list(filter(lambda x: x > prev_ts,
                                                     self.failed_tasks[task_id]))
            if not self.failed_tasks[task_id]:
                del self.failed_tasks[task_id]
        self.failed_log = list(filter(lambda x: x > prev_ts, self.failed_log))
        self.succeeded_log = list(filter(lambda x: x > prev_ts,
                                         self.succeeded_log))
        self.failed_cnt = len(self.failed_log)
        self.succeed_cnt = len(self.succeeded_log)
        self.duration = self._log_duration()

    def succeed_rate(self):
        self.purge_old()
        if self.failed_cnt + self.succeed_cnt < 1:
            return 1
        return self.succeed_cnt * 1.0 / (self.succeed_cnt + self.failed_cnt)

    def has_failed_on(self, task_id):
        return task_id in self.failed_tasks

    def should_forbit(self, task_id):
        self.purge_old()
        if task_id in self.failed_tasks:
            return len(self.failed_tasks[task_id]) >= self.tolerate_times
        return False

    def _log_duration(self):
        if not self.failed_log and not self.succeeded_log:
            return 0
        elif not self.failed_log:
            return self.succeeded_log[-1] - self.succeeded_log[0]
        elif not self.succeeded_log:
            return self.failed_log[-1] - self.failed_log[0]
        else:
            return max(self.failed_log[-1], self.succeeded_log[-1]) - \
                   min(self.failed_log[0], self.succeeded_log[0])

    def total_task_run(self):
        return self.succeed_cnt + self.failed_cnt


class TaskHostManager:
    def __init__(self):
        self.host_dict = {}

    def register_host(self, hostname):
        if hostname not in self.host_dict:
            logger.debug('register %s to the task host manager', hostname)
            self.host_dict[hostname] = HostStatus(hostname)

    def task_failed_on_host(self, task_id, host):
        if host in self.host_dict:
            host_status = self.host_dict[host]
            return host_status.has_failed_on(task_id)
        return False

    def offer_choice(self, tid, host_offers, blacklist):
        ordi_hosts = []
        fail_hosts = []
        forbit_host = []
        for host in host_offers:
            host_status = self.host_dict[host]
            if host in blacklist or host_status.should_forbit(tid):
                forbit_host.append(host)
            elif self.task_failed_on_host(tid, host):
                    fail_hosts.append((host, host_status.succeed_rate()))
            else:
                ordi_hosts.append((host, host_status.succeed_rate()))
        logger.debug('split the offer in to three parts \n '
                     'ordinary %s \nonce failed %s blacklist host %s',
                     str(ordi_hosts), str(fail_hosts), str(forbit_host))
        if ordi_hosts:
            return host_offers[self._random_weighted_choice(ordi_hosts)]
        elif fail_hosts:
            return host_offers[self._random_weighted_choice(fail_hosts)]
        return None, None

    def _random_weighted_choice(self, w_list):
        total = sum(w for h, w in w_list)
        chosen_w = random.uniform(0, total)
        cur_w = 0
        for h, w in w_list:
            if cur_w + w >= chosen_w:
                return h
            cur_w += w
        assert False, 'Should not get here'

    def task_succeed(self, task_id, hostname, reason):
        logger.debug('task %d %s', task_id, str(reason))
        if hostname in self.host_dict:
            host_status = self.host_dict[hostname]
            host_status.task_succeed(task_id)

    def task_failed(self, task_id, hostname, reason):
        logger.debug('task %d failed with message %s', task_id, str(reason))
        if hostname in self.host_dict:
            host_status = self.host_dict[hostname]
            host_status.task_failed(task_id)

    def is_unhealthy_host(self, host):
        if host not in self.host_dict:
            return False
        host_status = self.host_dict[host]
        succeed_rate = host_status.succeed_rate()
        duration = host_status.duration
        total_tasks = host_status.total_task_run()
        if duration > 30 and total_tasks > 20 and succeed_rate < 0.1:
            logger.debug('the host %s will be judge unhealthy for '
                         'succeed rate %.1f%% with %d tasks in '
                         'duration more than %.3fs',
                         host, succeed_rate, total_tasks, duration)
            return True

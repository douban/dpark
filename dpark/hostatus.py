import time
import random
from six.moves import filter
from dpark.utils.log import get_logger

PURGE_ELAPSED = 60 * 5
FAILED_TIMES = 2
logger = get_logger(__name__)


class HostStatus:
    def __init__(self, hostname, purge_elapsed=PURGE_ELAPSED):
        self.hostname = hostname
        self.failed_log = []
        self.succeeded_log = []
        self.failed_cnt = 0
        self.succeed_cnt = 0
        self.start_point = 0
        self.failed_tasks = {}
        self.purge_elapsed = purge_elapsed

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
        self.failed_log = list(filter(lambda x: x > prev_ts, self.failed_log))
        self.succeeded_log = list(filter(lambda x: x > prev_ts,
                                         self.succeeded_log))
        self.failed_cnt = len(self.failed_log)
        self.succeed_cnt = len(self.succeeded_log)
        self.start_point = self._begin_log_ts()

    def recent_succeed_rate(self):
        self.purge_old()
        if self.failed_cnt + self.succeed_cnt < 1:
            return 1
        return self.succeed_cnt * 1.0 / (self.succeed_cnt + self.failed_cnt)

    def failed_on(self, task_id):
        return task_id in self.failed_tasks

    def should_forbit(self, task_id):
        self.purge_old()
        if task_id in self.failed_tasks:
            cur_elapsed = time.time() - self.failed_tasks[task_id][-1]
            mask_elapsed = self.purge_elapsed * pow(2, len(self.failed_tasks[task_id]))
            return cur_elapsed < mask_elapsed

        return False

    def _begin_log_ts(self):
        ts = [time.time()]
        if self.failed_log:
            ts.append(self.failed_log[0])
        if self.succeeded_log:
            ts.append(self.succeeded_log[0])
        return min(ts)

    def total_recent_task_run(self):
        return self.succeed_cnt + self.failed_cnt

    def erase_failed_task(self, task_id):
        if task_id in self.failed_tasks:
            del self.failed_tasks[task_id]


class TaskHostManager:
    def __init__(self):
        self.host_dict = {}
        self.task_host_failed_dict = {}

    def register_host(self, hostname, purge_elapsed=PURGE_ELAPSED):
        if hostname not in self.host_dict:
            # logger.debug('register %s to the task host manager', hostname)
            self.host_dict[hostname] = HostStatus(hostname, purge_elapsed=purge_elapsed)

    def task_failed_on_host(self, task_id, host):
        if host in self.host_dict:
            host_status = self.host_dict[host]
            return host_status.failed_on(task_id)
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
                fail_hosts.append((host, host_status.recent_succeed_rate()))
            else:
                ordi_hosts.append((host, host_status.recent_succeed_rate()))
        logger.debug('split the offer in to three parts \n '
                     'ordinary %s \nonce failed %s blacklist host %s',
                     str(ordi_hosts), str(fail_hosts), str(forbit_host))
        if ordi_hosts:
            return host_offers[self._random_weighted_choice(ordi_hosts)]
        elif fail_hosts:
            return host_offers[self._random_weighted_choice(fail_hosts)]
        return None, None

    @staticmethod
    def _random_weighted_choice(w_list):
        total = sum(w for h, w in w_list)
        chosen_w = random.uniform(0, total)
        cur_w = 0
        for h, w in w_list:
            if cur_w + w >= chosen_w:
                return h
            cur_w += w
        assert False, 'Should not get here'

    def task_succeed(self, task_id, hostname, reason):
        logger.debug('task %s %s', task_id, str(reason))
        if hostname in self.host_dict:
            host_status = self.host_dict[hostname]
            host_status.task_succeed(task_id)
        if task_id in self.task_host_failed_dict:
            for host in self.task_host_failed_dict[task_id]:
                self.host_dict[host].erase_failed_task(task_id)
            logger.debug('the failed hosts %s for task %s',
                         str(self.task_host_failed_dict[task_id]), task_id)
            del self.task_host_failed_dict[task_id]

    def task_failed(self, task_id, hostname, reason):
        logger.debug('task %s failed with message %s', task_id, str(reason))
        if hostname in self.host_dict:
            host_status = self.host_dict[hostname]
            host_status.task_failed(task_id)
            if task_id not in self.task_host_failed_dict:
                self.task_host_failed_dict[task_id] = set()
            self.task_host_failed_dict[task_id].add(hostname)

    def is_unhealthy_host(self, host):
        if host not in self.host_dict:
            return False
        host_status = self.host_dict[host]
        succeed_rate = host_status.recent_succeed_rate()
        duration = time.time() - host_status.start_point
        total_tasks = host_status.total_recent_task_run()
        if duration > 30 and total_tasks > 20 and succeed_rate < 0.1:
            logger.debug('the host %s will be judge unhealthy for '
                         'succeed rate %.1f%% with %d tasks in '
                         'duration more than %.3fs',
                         host, succeed_rate, total_tasks, duration)
            return True

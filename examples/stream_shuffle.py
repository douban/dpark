# -*- coding: utf-8 -*-

from __future__ import absolute_import
from six.moves import range
import time
import os
import gc
import sys
import time
import unittest
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dpark import DparkContext
from dpark.utils.nested_groupby import GroupByNestedIter
from dpark.shuffle import GroupByNestedIter, AutoBatchedSerializer
from dpark.utils.profile import profile
import dpark.conf

GroupByNestedIter.NO_CACHE = True
print_mem_incr = True
print_mem_incr = False
dc = DparkContext('mesos')
RC = dpark.conf.rddconf
M = 1024 * 1024


def rss_func(p):
    if hasattr(p, "memory_info"):
        mem_info = getattr(p, "memory_info")
    else:
        mem_info = getattr(p, 'get_memory_info')

    def _():
        return mem_info().rss / M

    return _


class BenchShuffle(object):

    def __init__(self, num_key, num_value_per_key, num_map, num_reduce=1):
        self.num_key = num_key
        self.num_value_per_key = num_value_per_key
        self.num_map = num_map
        self.num_reduce = num_reduce

        self.exp_num_key = 0
        self.exp_num_value = 0

    def gen_data(self, dup_key):
        num_key = self.num_key
        num_value_per_key = self.num_value_per_key

        def _fm(map_id):
            for i in range(num_key):
                for j in range(num_value_per_key):
                    if dup_key:
                        yield (i, j)
                    else:
                        yield ((map_id, i), j)

        self.exp_num_key = self.num_key
        if not dup_key:
            self.exp_num_key *= self.num_map

        data = range(self.num_map)
        data = list(data)
        rdd = dc.makeRDD(data, self.num_map)
        rdd = rdd.flatMap(_fm)
        return rdd

    def count(self, rdd, count_value=False, multi_value=False):
        exp_num_key = self.exp_num_key
        print_mem_interval = exp_num_key // 100

        M = 1024 * 1024

        # @profile()
        def _count_time(it):
            st = time.time()
            nk = 0
            nv = 0
            np = 0

            import psutil
            proc = psutil.Process()
            get_rss = rss_func(proc)

            for x in it:
                k, v = x
                if count_value:
                    if multi_value:
                        for vv in v:
                            for _ in vv:
                                nv += 1
                    else:
                        for _ in v:
                            nv += 1
                nk += 1

                if print_mem_incr and nk % print_mem_interval == 0:
                    np += 1
                    print("%d%%: num=%d, key=%s, %s" % (np, nk, k, get_rss()))

            mm = max_rss() / M

            m0 = get_rss()
            gc.collect()
            m1 = get_rss()
            mp = AutoBatchedSerializer.size_loaded / M
            print("Mem(MB) before gc %d, after gc %d, max %d, size_loaed %d" % (m0, m1, mm, mp))

            ed = time.time()
            reduce_time = ed - st

            return [(nk, nv, mm, reduce_time)]

        tst = time.time()
        res = rdd.mapPartition(_count_time).collect()
        total_time = time.time() - tst
        nkey, nvalue, mm, reduce_time = res[0]

        print("reduce/total time = %d/%d, max_rss(M) = %d" % (reduce_time, total_time, mm))
        assert nkey == exp_num_key, (nkey, exp_num_key)
        if count_value:
            assert nvalue == self.exp_num_value, (nvalue, self.exp_num_value)

        return mm, total_time, reduce_time

    def test_reducebykey(self, rddconf, dup_key=False, taskMemory=None):
        rdd = self.gen_data(dup_key=dup_key)
        rdd = rdd.reduceByKey(lambda x, y: y, numSplits=self.num_reduce, rddconf=rddconf, taskMemory=taskMemory)
        return self.count(rdd)

    def test_groupbykey(self, rddconf, count_value=True, dup_key=True, taskMemory=None):
        rdd = self.gen_data(dup_key=dup_key)
        rdd = rdd.groupByKey(numSplits=self.num_reduce, rddconf=rddconf, taskMemory=taskMemory)
        self.exp_num_value = self.num_key * self.num_value_per_key * self.num_map
        return self.count(rdd, count_value)

    def test_cogroup(self, rddconf, count_value=True, dup_key=True, taskMemory=None):
        rdd1 = self.gen_data(dup_key=dup_key)
        rdd2 = self.gen_data(dup_key=dup_key)
        rdd = rdd1.groupWith(rdd2, numSplits=self.num_reduce, rddconf=rddconf, taskMemory=taskMemory)
        self.exp_num_value = self.num_map * self.num_key * self.num_value_per_key * 2
        return self.count(rdd, count_value, multi_value=True)

    def test_join(self, rddconf, count_value=True, dup_key=True, taskMemory=None):
        rdd1 = self.gen_data(dup_key=dup_key)
        rdd2 = BenchShuffle(self.num_key, 2, 2).gen_data(dup_key=dup_key)
        rdd = rdd1.join(rdd2, numSplits=self.num_reduce, rddconf=rddconf, taskMemory=taskMemory)
        self.exp_num_key = self.exp_num_value = self.num_map * self.num_key * self.num_value_per_key * 2 * 2
        return self.count(rdd)


def max_rss():
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1000


# all time is reduce task func run time
class TestShuffle(unittest.TestCase):

    def test_max_open_file(self):
        n = dpark.conf.MAX_OPEN_FILE
        for num_map in [n, n + 1]:
            # should see waring 'fall back to SHUFFLE_DISK' for n+1
            rddconf = dpark.conf.rddconf(sort_merge=True)
            BenchShuffle(10, 10, num_map).test_groupbykey(rddconf=rddconf, taskMemory=50)

    def test_oom_many_key(self):
        params = [
            (RC(), 2500),  # 30 sec
            (RC(disk_merge=True), 2500),  # 30 sec
            (RC(disk_merge=True, dump_mem_ratio=0.6), 1000),  # 145 sec
            (RC(sort_merge=True), 50),  # 43 sec
        ]
        for rc, m in params:
            BenchShuffle(1024 * 128, 2, 100).test_reducebykey(rddconf=rc, dup_key=False, taskMemory=m)

    def test_oom_many_value(self):
        params = [
            (RC(), 4000),  # 16 sec
            (RC(disk_merge=True, dump_mem_ratio=0.7), 1000),  # 17 sec
            (RC(sort_merge=True), 200),  # 16 sec
            (RC(sort_merge=True, iter_group=True), 50),  # 16 sec
        ]
        for rc, m in params:
            BenchShuffle(1024, 1024, 100).test_groupbykey(rddconf=rc, taskMemory=m)

    def test_oom_onebigkey(self):
        params = [
            (RC(), 3500),  # 24 sec
            (RC(sort_merge=True, iter_group=True), 50),  # 15 sec
        ]
        for rc, m in params:
            BenchShuffle(1, 1024 * 1024, 100).test_groupbykey(rddconf=rc, taskMemory=m)

    def test_cogroup(self):
        params = [
            (RC(), 7000),  # 40 sec
            (RC(sort_merge=True, iter_group=True), 50),  # 45 sec
        ]
        for rc, m in params:
            BenchShuffle(1, 1024 * 1024, 100).test_cogroup(rddconf=rc, taskMemory=m)

    def test_join(self):
        params = [
            (RC(sort_merge=True, iter_group=True), 50),  # 208 sec
            (RC(), 3500),  # 115 sec
        ]
        for rc, m in params:
            BenchShuffle(1, 1024 * 1024, 100).test_join(rddconf=rc, taskMemory=m)

    def test_disk_merge(self):

        values_per_key = 1024

        def _test(num_key, map_mem, map_disk_merge):

            def mp1(it):
                for v in range(values_per_key):
                    for k in range(num_key):
                        yield k, v

            def mp2(it):
                n = 0
                for k, g in it:
                    for v in g:
                        n += 1
                yield n

            rdd0 = dc.makeRDD([0, 1], 2).mapPartition(mp1)
            rdd0.mem = map_mem

            rdd1 = rdd0.groupByKey(numSplits=2, rddconf=RC(disk_merge=True, dump_mem_ratio=0.6)).mapPartition(mp2)
            res = rdd1.collect()
            assert (sum(res) == num_key * values_per_key * 2)
            st = dc.scheduler.jobstats[-1]
            from pprint import pprint
            pprint(st)

        _test(50 * 1024, 100, True)


if __name__ == "__main__":
    import dpark.conf

    dpark.conf.MULTI_SEGMENT_DUMP = True
    rc = dpark.conf.default_rddconf
    rc.disk_merge = False
    rc.sort_merge = False
    rc.iter_group = False
    rc.ordered_group = False
    rc.dump_mem_ratio = False
    unittest.main(verbosity=2)

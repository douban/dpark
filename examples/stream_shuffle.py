# -*- coding: utf-8 -*-

from __future__ import absolute_import
from six.moves import range
import time
import os
import gc
import sys
import time
import unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dpark import DparkContext
from dpark.nested_groupby import GroupByNestedIter
from dpark.shuffle import GroupByNestedIter, AutoBatchedSerializer
from dpark.schedule import profile

GroupByNestedIter.NO_CACHE = True
print_mem_incr = True
print_mem_incr = False
dc = DparkContext('mesos')


class BenchShuffle(object):

    def __init__(self, num_key, num_value_per_key, num_map):
        self.num_key = num_key
        self.num_value_per_key = num_value_per_key
        self.num_map = num_map
        self.num_reduce = 1
        self.exp_num_key = 0
        self.exp_num_value = 0

    def gen_data(self, dup):
        num_key = self.num_key
        num_value_per_key = self.num_value_per_key

        def _fm(x):
            for i in range(num_key):
                for j in range(num_value_per_key):
                    if dup:
                        yield (i, j)
                    else:
                        yield ((x, i), j)

        self.exp_num_key = self.num_key
        if not dup:
            self.exp_num_key *= self.num_map

        return dc.makeRDD(list(range(self.num_map)), self.num_map).flatMap(_fm)

    def count(self, rdd, count_value=False, multi_value=False):
        exp_num_key = self.exp_num_key
        print_mem_interval = exp_num_key // 100

        M = 1024*1024

        #@profile
        def _count_time(it):
            st = time.time()
            nk = 0
            nv = 0
            np = 0

            import psutil
            proc = psutil.Process()
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
                    print("%d%%: num=%d, key=%s, %s" % (np, nk, k, proc.get_memory_info()))

            mm = max_rss()/M

            m0 = proc.get_memory_info().rss/M
            gc.collect()
            m1 = proc.get_memory_info().rss/M
            mp = AutoBatchedSerializer.size_loaded/M
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

    def test_reducebykey(self, sort_shuffle, dup=False, taskMemory=None):
        rdd = self.gen_data(dup=dup)
        rdd = rdd.reduceByKey(lambda x, y: y, numSplits=self.num_reduce, sort_shuffle=sort_shuffle,taskMemory=taskMemory)
        return self.count(rdd)

    def test_groupbykey(self, sort_shuffle, iter_values, count_value, dup=True, taskMemory=None):
        rdd = self.gen_data(dup=dup)
        rdd = rdd.groupByKey(numSplits=self.num_reduce, sort_shuffle=sort_shuffle, iter_values=iter_values, taskMemory=taskMemory)
        self.exp_num_value = self.num_key * self.num_value_per_key * self.num_map
        return self.count(rdd, count_value)

    def test_cogroup(self, sort_shuffle, iter_values, count_value, dup=True, taskMemory=None):
        rdd1 = self.gen_data(dup=dup)
        rdd2 = self.gen_data(dup=dup)
        rdd = rdd1.groupWith(rdd2, numSplits=self.num_reduce, sort_shuffle=sort_shuffle, iter_values=iter_values, taskMemory=taskMemory)
        self.exp_num_value = self.num_map * self.num_key * self.num_value_per_key * 2
        return self.count(rdd, count_value, multi_value=True)

    def test_join(self, sort_shuffle, iter_values, count_value, dup=True, taskMemory=None):
        rdd1 = self.gen_data(dup=dup)
        rdd2= BenchShuffle(self.num_key, 2, 2).gen_data(dup=dup)
        rdd = rdd1.join(rdd2, numSplits=self.num_reduce, sort_shuffle=sort_shuffle, iter_values=iter_values, taskMemory=taskMemory)
        self.exp_num_key = self.exp_num_value = self.num_map * self.num_key * self.num_value_per_key * 2 * 2
        return self.count(rdd)


def max_rss():
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1000


# all time is reduce task func run time
class TestShuffle(unittest.TestCase):

    def test_max_open_file(self):
        BenchShuffle(10, 10, 950).test_groupbykey(True, False, False)


    def test_oom_many_key(self):
        # 84 sec
        BenchShuffle(1024*128, 2, 100).test_reducebykey(sort_shuffle=True, dup=False, taskMemory=50)
        # 7 sec
        BenchShuffle(1024*128, 2, 100).test_reducebykey(sort_shuffle=False, dup=False, taskMemory=2500)


    def test_oom_many_value(self):
        # 32 sec
        BenchShuffle(1024, 1024, 100).test_groupbykey(sort_shuffle=True, iter_values=True, count_value=True,taskMemory=150)
        # 12 sec
        BenchShuffle(1024, 1024, 100).test_groupbykey(sort_shuffle=True, iter_values=False, count_value=True, taskMemory=200)
        # 5 sec
        BenchShuffle(1024, 1024, 100).test_groupbykey(sort_shuffle=False, iter_values=False, count_value=True, taskMemory=3000)


    def test_oom_onebigkey(self):
        # 20 sec
        BenchShuffle(1, 1024 * 1024, 100).test_groupbykey(True, True, True, taskMemory=50)
        # 2 sec
        BenchShuffle(1, 1024 * 1024, 100).test_groupbykey(False, False, False, taskMemory=3500)
        #BenchShuffle(1, 1024 * 1024, 100).test_groupbykey(True, False, False, taskMemory=400) # same mem as (False, False, False), but slow


    def test_cogroup(self):
        # 40 sec
        BenchShuffle(1, 1024*1024, 100).test_cogroup(True, True, count_value=True, taskMemory=50)
        # 4 sec
        BenchShuffle(1, 1024*1024, 100).test_cogroup(False, False, False, taskMemory=7000)


    def test_join(self):
        # 123 sec
        BenchShuffle(1, 1024*1024, 100).test_join(True, True, True, taskMemory=50)
        # 108 sec
        BenchShuffle(1, 1024*1024, 100).test_join(False, False, False, taskMemory=3500)


if __name__ == "__main__":
    unittest.main(verbosity=2)

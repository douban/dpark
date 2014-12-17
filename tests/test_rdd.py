import sys, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import cPickle
import unittest
import pprint
import random
import operator
import shutil
import logging
from dpark.context import *
from dpark.rdd import *
from dpark.accumulator import *

logging.getLogger('dpark').setLevel(logging.ERROR)

class TestRDD(unittest.TestCase):
    def setUp(self):
        self.sc = DparkContext("local")

    def tearDown(self):
        self.sc.stop()

    def test_parallel_collection(self):
        slices = ParallelCollection.slice(xrange(5), 3)
        self.assertEqual(len(slices), 3)
        self.assertEqual(list(slices[0]), range(2))
        self.assertEqual(list(slices[1]), range(2, 4))
        self.assertEqual(list(slices[2]), range(4, 5))

    def test_basic_operation(self):
        d = range(4)
        nums = self.sc.makeRDD(d, 2)
        self.assertEqual(len(nums.splits), 2)
        self.assertEqual(nums.collect(), d)
        self.assertEqual(nums.reduce(lambda x,y:x+y), sum(d))
        self.assertEqual(nums.map(lambda x:str(x)).collect(), ["0", "1", "2", "3"])
        self.assertEqual(nums.filter(lambda x:x>1).collect(), [2, 3])
        self.assertEqual(nums.flatMap(lambda x:range(x)).collect(), [0, 0,1, 0,1,2])
        self.assertEqual(nums.union(nums).collect(), d + d)
        self.assertEqual(nums.cartesian(nums).map(lambda (x,y):x*y).reduce(lambda x,y:x+y), 36)
        self.assertEqual(nums.glom().map(lambda x:list(x)).collect(),[[0,1],[2,3]])
        self.assertEqual(nums.mapPartitions(lambda x:[sum(x)]).collect(),[1, 5])
        self.assertEqual(nums.map(lambda x:str(x)+"/").reduce(lambda x,y:x+y),
            "0/1/2/3/")
        self.assertEqual(nums.pipe('grep 3').collect(), ['3'])
        self.assertEqual(nums.sample(0.5, True).count(), 2)

        self.assertEqual(len(nums[:1]), 1)
        self.assertEqual(nums[:1].collect(), range(2))
        self.assertEqual(len(nums.mergeSplit(2)), 1)
        self.assertEqual(nums.mergeSplit(2).collect(), range(4))
        self.assertEqual(nums.zipWith(nums).collectAsMap(), dict(zip(d,d)))

    def test_ignore_bad_record(self):
        d = range(100)
        self.sc.options.err = 0.02
        nums = self.sc.makeRDD(d, 2)
        self.assertEqual(nums.filter(lambda x:1.0/x).count(), 99)
        self.assertEqual(nums.map(lambda x:1/x).count(), 99)
        self.assertEqual(nums.flatMap(lambda x:[1/x]).count(), 99)
        self.assertEqual(nums.reduce(lambda x,y:x+100/y), 431)

    def test_pair_operation(self):
        d = zip([1,2,3,3], range(4,8))
        nums = self.sc.makeRDD(d, 2)
        self.assertEqual(nums.reduceByKey(lambda x,y:x+y).collectAsMap(), {1:4, 2:5, 3:13})
        self.assertEqual(nums.reduceByKeyToDriver(lambda x,y:x+y), {1:4, 2:5, 3:13})
        self.assertEqual(nums.groupByKey().collectAsMap(), {1:[4], 2:[5], 3:[6,7]})

        # join
        nums2 = self.sc.makeRDD(zip([2,3,4], [1,2,3]), 2)
        self.assertEqual(nums.join(nums2).collect(),
                [(2, (5, 1)), (3, (6, 2)), (3, (7, 2))])
        self.assertEqual(sorted(nums.leftOuterJoin(nums2).collect()),
                [(1, (4,None)), (2, (5, 1)), (3, (6, 2)), (3, (7, 2))])
        self.assertEqual(sorted(nums.rightOuterJoin(nums2).collect()),
                [(2, (5,1)), (3, (6,2)), (3, (7,2)), (4,(None,3))])
        self.assertEqual(nums.innerJoin(nums2).collect(),
                [(2, (5, 1)), (3, (6, 2)), (3, (7, 2))])

        self.assertEqual(nums.mapValue(lambda x:x+1).collect(),
                [(1, 5), (2, 6), (3, 7), (3, 8)])
        self.assertEqual(nums.flatMapValue(lambda x:range(x)).count(), 22)
        self.assertEqual(nums.groupByKey().lookup(3), [6,7])

        # group with
        self.assertEqual(sorted(nums.groupWith(nums2).collect()),
                [(1, ([4],[])), (2, ([5],[1])), (3,([6,7],[2])), (4,([],[3]))])
        nums3 = self.sc.makeRDD(zip([4,5,1], [1,2,3]), 1).groupByKey(2).flatMapValue(lambda x:x)
        self.assertEqual(sorted(nums.groupWith([nums2, nums3]).collect()),
                [(1, ([4],[],[3])), (2, ([5],[1],[])), (3,([6,7],[2],[])),
                (4,([],[3],[1])), (5,([],[],[2]))])

        # update
        rdd4 = self.sc.makeRDD([('foo', 1), ('wtf', 233)])
        rdd5 = self.sc.makeRDD([('foo', 2), ('bar', 3), ('wtf', None)])
        rdd6 = self.sc.makeRDD([('dup', 1), ('dup', 2), ('duq', 3), ('duq', 4),
                                ('foo', 5)])
        rdd7 = self.sc.makeRDD([('duq', 6), ('duq', 7), ('duq', 8), ('dup', 9),
                                ('bar', 10)])
        dct = rdd6.update(rdd7).collectAsMap()
        dct2 = rdd7.update(rdd6).collectAsMap()

        self.assertEqual(
            rdd4.update(rdd5, replace_only=True).collectAsMap(),
            dict([('foo', 2), ('wtf', None)])
        )
        self.assertEqual(
            rdd5.update(rdd4, replace_only=True).collectAsMap(),
            dict([('foo', 1), ('bar', 3), ('wtf', 233)])
        )
        self.assertEqual(
            rdd4.update(rdd5).collectAsMap(),
            dict([('foo', 2), ('bar', 3), ('wtf', None)])
        )
        self.assertEqual(
            rdd5.update(rdd4).collectAsMap(),
            dict([('foo', 1), ('bar', 3), ('wtf', 233)])
        )
        self.assertEqual(dct.get('dup'), 9)
        self.assertEqual(dct.get('foo'), 5)
        self.assertTrue(dct.get('duq') in {6, 7, 8})
        self.assertEqual(dct.get('bar'), 10)
        self.assertTrue(dct2.get('dup') in {1, 2})
        self.assertEqual(dct2.get('foo'), 5)
        self.assertTrue(dct2.get('duq') in {3, 4})
        self.assertEqual(dct2.get('bar'), 10)

    def test_accumulater(self):
        d = range(4)
        nums = self.sc.makeRDD(d, 2)

        acc = self.sc.accumulator()
        nums.map(lambda x: acc.add(x)).count()
        self.assertEqual(acc.value, 6)

        acc = self.sc.accumulator([], listAcc)
        nums.map(lambda x: acc.add([x])).count()
        self.assertEqual(list(sorted(acc.value)), range(4))

    def test_sort(self):
        d = range(100)
        self.assertEqual(self.sc.makeRDD(d, 10).collect(), range(100))
        random.shuffle(d)
        rdd = self.sc.makeRDD(d, 10)
        self.assertEqual(rdd.sort(numSplits=10).collect(), range(100))
        self.assertEqual(rdd.sort(reverse=True, numSplits=5).collect(), list(reversed(range(100))))
        self.assertEqual(rdd.sort(key=lambda x:-x, reverse=True, numSplits=4).collect(), range(100))

        self.assertEqual(rdd.top(), range(90, 100)[::-1])
        self.assertEqual(rdd.top(15, lambda x:-x), range(0, 15))

        for i in range(10):
            for j in range(i+1):
                d.append(i)
        rdd = self.sc.makeRDD(d, 10)
        self.assertEqual(rdd.hot(), zip(range(9, -1, -1), range(11, 1, -1)))

    def test_empty_rdd(self):
        rdd = self.sc.union([])
        self.assertEqual(rdd.count(), 0)
        self.assertEqual(rdd.sort().collect(), [])

    def test_text_file(self):
        path = 'tests/test_rdd.py'
        f = self.sc.textFile(path, splitSize=1000).mergeSplit(numSplits=1)
        n = len(open(path).read().split())
        fs = f.flatMap(lambda x:x.split()).cache()
        self.assertEqual(fs.count(), n)
        self.assertEqual(fs.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).collectAsMap()['class'], 1)
        prefix = 'prefix:'
        self.assertEqual(f.map(lambda x:prefix+x).saveAsTextFile('/tmp/tout'),
            ['/tmp/tout/0000'])
        self.assertEqual(f.map(lambda x:('test', prefix+x)).saveAsTextFileByKey('/tmp/tout'),
            ['/tmp/tout/test/0000'])
        d = self.sc.textFile('/tmp/tout')
        n = len(open(path).readlines())
        self.assertEqual(d.count(), n)
        self.assertEqual(fs.map(lambda x:(x,1)).reduceByKey(operator.add
            ).saveAsCSVFile('/tmp/tout'),
            ['/tmp/tout/0000.csv'])
        shutil.rmtree('/tmp/tout')

    def test_compressed_file(self):
        # compress
        d = self.sc.makeRDD(range(100000), 1)
        self.assertEqual(d.map(str).saveAsTextFile('/tmp/tout', compress=True),
            ['/tmp/tout/0000.gz'])
        rd = self.sc.textFile('/tmp/tout', splitSize=10<<10)
        self.assertEqual(rd.count(), 100000)

        self.assertEqual(d.map(lambda i:('x', str(i))).saveAsTextFileByKey('/tmp/tout', compress=True),
            ['/tmp/tout/x/0000.gz'])
        rd = self.sc.textFile('/tmp/tout', splitSize=10<<10)
        self.assertEqual(rd.count(), 100000)
        shutil.rmtree('/tmp/tout')

    def test_binary_file(self):
        d = self.sc.makeRDD(range(100000), 1)
        self.assertEqual(d.saveAsBinaryFile('/tmp/tout', fmt="I"),
            ['/tmp/tout/0000.bin'])
        rd = self.sc.binaryFile('/tmp/tout', fmt="I", splitSize=10<<10)
        self.assertEqual(rd.count(), 100000)
        shutil.rmtree('/tmp/tout')

    def test_table_file(self):
        N = 100000
        d = self.sc.makeRDD(zip(range(N), range(N)), 1)
        self.assertEqual(d.saveAsTableFile('/tmp/tout'), ['/tmp/tout/0000.tab',])
        rd = self.sc.tableFile('/tmp/tout', splitSize=64<<10)
        self.assertEqual(rd.count(), N)
        self.assertEqual(rd.map(lambda x:x[0]).reduce(lambda x,y:x+y), sum(xrange(N)))

        d.asTable(['f1', 'f2']).save('/tmp/tout')
        rd = self.sc.table('/tmp/tout')
        self.assertEqual(rd.map(lambda x:x.f1+x.f2).reduce(lambda x,y:x+y), 2*sum(xrange(N)))
        shutil.rmtree('/tmp/tout')

    def test_batch(self):
        from math import ceil
        d = range(1234)
        rdd = self.sc.makeRDD(d, 10).batch(100)
        self.assertEqual(rdd.flatMap(lambda x:x).collect(), d)
        self.assertEqual(rdd.filter(lambda x: len(x)<=2 or len(x) >100).collect(), [])

    def test_partial_file(self):
        p = 'tests/test_rdd.py'
        l = 300
        d = open(p).read(l+50)
        start = 100
        while d[start-1] != '\n':
            start += 1
        while d[l-1] != '\n':
            l += 1
        d = d[start:l-1]
        rdd = self.sc.partialTextFile(p, start, l, l)
        self.assertEqual('\n'.join(rdd.collect()), d)
        rdd = self.sc.partialTextFile(p, start, l, (l-start)/5)
        self.assertEqual('\n'.join(rdd.collect()), d)

    def test_beansdb(self):
        N = 100
        l = range(N)
        d = zip(map(str, l), l)
        rdd = self.sc.makeRDD(d, 10)
        self.assertEqual(rdd.saveAsBeansdb('/tmp/beansdb'),
                       ['/tmp/beansdb/%03d.data' % i for i in range(10)])
        rdd = self.sc.beansdb('/tmp/beansdb', depth=0)
        self.assertEqual(len(rdd), 10)
        self.assertEqual(rdd.count(), N)
        self.assertEqual(sorted(rdd.map(lambda (k,v):(k,v[0])).collect()), sorted(d))
        s = rdd.map(lambda x:x[1][0]).reduce(lambda x,y:x+y)
        self.assertEqual(s, sum(l))

        rdd = self.sc.beansdb('/tmp/beansdb', depth=0, fullscan=True)
        self.assertEqual(len(rdd), 10)
        self.assertEqual(rdd.count(), N)
        self.assertEqual(sorted(rdd.map(lambda (k,v):(k,v[0])).collect()), sorted(d))
        s = rdd.map(lambda x:x[1][0]).reduce(lambda x,y:x+y)
        self.assertEqual(s, sum(l))
        shutil.rmtree('/tmp/beansdb')

    def test_beansdb_invalid_key(self):
        func = OutputBeansdbRDD.is_valid_key
        input_expect = [
            ('/test/aaa/12321', True),
            ('a' * 251, False),
            ('/a/b\n/c', False),
            ('/a/b/\r/d', False),
            ('/a/b/\0/e', False),
            ('/a/b /c', False),
            ('/a/b \n/d', False),
        ]
        for key, expect in input_expect:
            self.assertEqual(func(key), expect)

    def test_enumerations(self):
        N = 100
        p = 10
        l = range(N)
        d1 = sorted(map(lambda x: (x/p, x), l))
        d2 = sorted(map(lambda x: ((x/p, x%p), x), l))
        rdd = self.sc.makeRDD(l, p)
        self.assertEqual(sorted(rdd.enumeratePartition().collect()), d1)
        self.assertEqual(sorted(rdd.enumerate().collect()), d2)

    def test_tabular(self):
        d = range(10000)
        d = zip(d, map(str, d), map(float, d))
        path = '/tmp/tabular-%s' % os.getpid()
        try:
            self.sc.makeRDD(d).saveAsTabular(path, 'f_int, f_str, f_float', indices=['f_str', 'f_float'])
            r = self.sc.tabular(path, fields=['f_float', 'f_str']).collect()
            for f, s in r:
                self.assertEqual(type(f), float)
                self.assertEqual(type(s), str)
                self.assertEqual(str(int(f)), s)
            self.assertEqual(sorted(x.f_float for x in r), sorted(x[2] for x in d))

            r = self.sc.tabular(path, fields='f_int f_float').filterByIndex(f_float=lambda x:hash(x) % 2).collect()
            for i, f in r:
                self.assertEqual(type(i), int)
                self.assertEqual(type(f), float)
                self.assertEqual(i, int(f))
                self.assertTrue(hash(f) % 2)

            self.assertEqual(sorted(x.f_int for x in r), sorted(x[0] for x in d if hash(x[2]) %2))
        finally:
            try:
                shutil.rmtree(path)
            except OSError:
                pass

    def test_iter(self):
        d = range(1000)
        rdd = self.sc.makeRDD(d, 10)
        assert d == [i for i in rdd]


#class TestRDDInProcess(TestRDD):
#    def setUp(self):
#        self.sc = DparkContext("process")


if __name__ == "__main__":
    import logging
    unittest.main()

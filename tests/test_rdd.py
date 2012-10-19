import sys
import cPickle
import unittest
import pprint
import random
import operator
from dpark.context import *
from dpark.rdd import *
from dpark.accumulator import *

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
        self.assertEqual(nums.pipe('grep 3').collect(), ['3\n'])
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

        self.assertEqual(nums.mapValue(lambda x:x+1).collect(), 
                [(1, 5), (2, 6), (3, 7), (3, 8)])
        self.assertEqual(nums.flatMapValue(lambda x:range(x)).count(), 22)
        self.assertEqual(nums.groupByKey().lookup(3), [6,7])

        # group with
        self.assertEqual(sorted(nums.groupWith(nums2).collect()), 
                [(1, ([4],[])), (2, ([5],[1])), (3,([6,7],[2])), (4,([],[3]))])
        nums3 = self.sc.makeRDD(zip([4,5,1], [1,2,3]), 1).groupByKey(2).flatMapValue(lambda x:x)
        self.assertEqual(sorted(nums.groupWith(nums2, nums3).collect()),
                [(1, ([4],[],[3])), (2, ([5],[1],[])), (3,([6,7],[2],[])), 
                (4,([],[3],[1])), (5,([],[],[2]))])
    
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

    def test_text_file(self):
        path = 'tests/test_rdd.py'
        f = self.sc.textFile(path, splitSize=1000).mergeSplit(numSplits=1)
        n = len(open(path).read().split())
        fs = f.flatMap(lambda x:x.split()).cache()
        self.assertEqual(fs.count(), n)
        self.assertEqual(fs.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).collectAsMap()['import'], 10)
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

    def test_binary_file(self): 
        d = self.sc.makeRDD(range(100000), 1)
        self.assertEqual(d.saveAsBinaryFile('/tmp/tout', fmt="I"),
            ['/tmp/tout/0000.bin'])
        rd = self.sc.binaryFile('/tmp/tout', fmt="I", splitSize=10<<10)
        self.assertEqual(rd.count(), 100000)


#class TestRDDInProcess(TestRDD):
#    def setUp(self):
#        self.sc = DparkContext("process")


if __name__ == "__main__":
    import logging
    unittest.main()

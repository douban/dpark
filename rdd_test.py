import pickle
import unittest
import pprint
from context import *
from rdd import *

class TestRDD(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext("local", "test")
        self.sc.init()

    def tearDown(self):
        self.sc.stop()

    def test_parallel_collection(self):
        slices = ParallelCollection.slice(xrange(5), 3)
        self.assertEqual(len(slices), 3)
        self.assertEqual(slices[0], range(2))
        self.assertEqual(slices[1], range(2, 4))
        self.assertEqual(slices[2], range(4, 5))

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
        self.assertEqual(nums.cartesion(nums).map(lambda (x,y):x*y).reduce(lambda x,y:x+y), 36)
        self.assertEqual(nums.glom().map(lambda x:list(x)).collect(),[[0,1],[2,3]])
        self.assertEqual(nums.mapPartitions(lambda x:[sum(x)]).collect(),[1, 5])
        self.assertEqual(nums.map(lambda x:str(x)).map(lambda x:x+"/").reduce(lambda x,y:x+y), "0/1/2/3/")

    def test_pair_operation(self):
        d = zip([1,2,3,3], range(4,8))
        nums = self.sc.makeRDD(d, 2)
        self.assertEqual(nums.reduceByKey(lambda x,y:x+y).collectAsMap(), {1:4, 2:5, 3:13})
        self.assertEqual(nums.reduceByKeyToDriver(lambda x,y:x+y), {1:4, 2:5, 3:13})
        self.assertEqual(nums.groupByKey(2).collectAsMap(), {1:[4], 2:[5], 3:[6,7]})

    def test_process(self):
        self.sc.stop()
        self.sc = SparkContext("process", "test")
        self.sc.init()
        self.test_basic_operation()
        #self.test_pair_operation()

    def test_file(self):
        f = self.sc.textFile(__file__)
        n = len(open(__file__).read().split())
        self.assertEqual(f.flatMap(lambda x:x.split()).count(), n)
        self.assertEqual(f.flatMap(lambda x:x.split()).map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).collectAsMap()['import'], 6)
        prefix = 'prefix:'
        self.assertEqual(f.map(lambda x:prefix+x).saveAsTextFile('/tmp/tout').collect(), ['/tmp/tout/0']) 
        d = self.sc.textFile('/tmp/tout')
        n = len(open(__file__).readlines())
        self.assertEqual(d.count(), n)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.ERROR)
    unittest.main()

import pickle
import unittest
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
        return
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
        print nums.groupByKey(2).collect()

    def test_process(self):
        self.sc.stop()
        self.sc = SparkContext("process", "test")
        self.sc.init()
        self.test_basic_operation()

    def test_file(self):
        return
        f = self.sc.textFile(__file__)
        n = len(open(__file__).read().split())
        self.assertEqual(f.flatMap(lambda x:x.split()).count(), n)
        #.map(lambda x:1).reduce(lambda x,y:x+y)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

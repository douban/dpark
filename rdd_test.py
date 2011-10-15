import pickle
import unittest
from context import *
from rdd import *

class TestRDD(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parallel_collection(self):
        slices = ParallelCollection.slice(xrange(5), 3)
        self.assertEqual(len(slices), 3)
        self.assertEqual(slices[0], range(2))
        self.assertEqual(slices[1], range(2, 4))
        self.assertEqual(slices[2], range(4, 5))

        

    def test_basic_operation(self):
        sc = SparkContext("process", "test")
        sc.init()
        pickle.dumps(sc)
        d = range(4)
        nums = sc.makeRDD(d, 2)
        pickle.dumps(nums)
        pickle.dumps(nums.map(lambda x:str(x)))
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
        sc.stop()

if __name__ == "__main__":
    unittest.main()

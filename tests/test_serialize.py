import unittest
from dpark.serialize import dump_closure, load_closure

class TestSerialize(unittest.TestCase):
    def testNameError(self):
        def foo():
            print x

        dumped_func = dump_closure(foo)
        func = load_closure(dumped_func)
        
        self.assertRaises(NameError, func)
        x = 10
        
    def testNoneAsFreeVar(self):
        y = None
        x = 10
        
        def foo():
            return (x, y)

        dumped_func = dump_closure(foo)
        func = load_closure(dumped_func)

        self.assertEqual(func(), (x, y))
        

import sys
import unittest
import timeit
from dpark.serialize import dump_closure, load_closure, dumps, loads
from contextlib import contextmanager

MAIN = sys.modules['__main__']


@contextmanager
def main_environ(exec_str):
    org = MAIN.__dict__.copy()
    exec exec_str in MAIN.__dict__
    yield
    for k in MAIN.__dict__.keys():
        if k not in org:
            del MAIN.__dict__[k]


class TestSerialize(unittest.TestCase):

    def testNameError(self):
        def foo():
            print x

        dumped_func = dump_closure(foo)
        func = load_closure(dumped_func)

        self.assertRaises(NameError, func)
        x = 10

    def test_big_object_performance(self):
        t1 = max(timeit.repeat('dumps(d)',
                               'from dpark.serialize import dumps;'
                               'd = {(str(i),):i for i in xrange(10000)}',
                               repeat=3, number=1))
        t2 = max(timeit.repeat('dumps(d, -1)',
                               'from pickle import dumps;'
                               'd = {(str(i),):i for i in xrange(10000)}',
                               repeat=3, number=1))
        assert t1 < t2 * 2.5

    def testNoneAsFreeVar(self):
        y = None
        x = 10

        def foo():
            return (x, y)

        dumped_func = dump_closure(foo)
        func = load_closure(dumped_func)

        self.assertEqual(func(), (x, y))

    def testRandomSample(self):
        from random import sample, Random
        _sample = loads(dumps(sample))
        assert _sample.im_class is sample.im_class
        assert _sample.im_func is sample.im_func
        assert isinstance(_sample.im_self, Random)
        assert isinstance(sample.im_self, Random)

    def testLocalMethod(self):
        exec_str = """
x = 1
class Foo(object):
    def func1(self):
        return x
    @classmethod
    def func2(self):
        return x + 1
    def func3(self):
        try:
            return x+2
        except:
            return self.func3()
foo = Foo()
func1 = foo.func1
func2 = foo.func2
func3 = foo.func3
"""
        with main_environ(exec_str):
            _func1 = loads(dumps(MAIN.func1))
            _func2 = loads(dumps(MAIN.func2))
            _func3 = loads(dumps(MAIN.func3))

            assert _func1() == MAIN.x
            assert _func2() == MAIN.x + 1
            assert _func3() == MAIN.x + 2

    def testLocalMethodCallChain(self):
        exec_str = """
x = 1
class Bar(object):
    @classmethod
    def func1(self):
        return x
    def func2(self):
        return self.func1() + 1
    def func3(self):
        try:
            return x+2
        except:
            return self.func2()
foo = Bar()
func1 = foo.func1
func2 = foo.func2
func3 = foo.func3
"""
        with main_environ(exec_str):
            _func1 = loads(dumps(MAIN.func1))
            _func2 = loads(dumps(MAIN.func2))
            _func3 = loads(dumps(MAIN.func3))

            assert _func1() == MAIN.x
            assert _func2() == MAIN.x + 1
            assert _func3() == MAIN.x + 2

    def testLocalMethodCallChain2(self):
        exec_str = """
x = 1
class FooBar(object):
    @classmethod
    def func1(self):
        return x
    def func2(self):
        return self.func1() + 1
    def func3(self):
        try:
            return x+2
        except:
            return self.func3()
foo = FooBar()
func1 = foo.func1
func2 = foo.func2
func3 = foo.func3
"""
        with main_environ(exec_str):
            _func1 = loads(dumps(MAIN.func1))
            _func2 = loads(dumps(MAIN.func2))
            _func3 = loads(dumps(MAIN.func3))

            assert _func1() == MAIN.x
            assert _func2() == MAIN.x + 1
            assert _func3() == MAIN.x + 2

    def testMemberDescriptor(self):
        exec_str = """
class _ClsWithSlots(object):
    __slots__=['x', 'y']
"""
        with main_environ(exec_str):
            f = MAIN._ClsWithSlots()
            _f = loads(dumps(f))
            assert _f.__slots__ == f.__slots__

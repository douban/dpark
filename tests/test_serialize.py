from __future__ import absolute_import
from __future__ import print_function
import six
import sys
import unittest
import timeit
from flaky import flaky
from dpark.serialize import dump_closure, load_closure, dumps, loads
from contextlib import contextmanager

MAIN = sys.modules['__main__']


@contextmanager
def main_environ(exec_str):
    org = MAIN.__dict__.copy()
    exec(exec_str, MAIN.__dict__)
    yield
    for k in list(MAIN.__dict__.keys()):
        if k not in org:
            del MAIN.__dict__[k]


class TestSerialize(unittest.TestCase):

    def testNameError(self):
        def foo():
            print(x)

        dumped_func = dump_closure(foo)
        func = load_closure(dumped_func)

        self.assertRaises(NameError, func)
        x = 10

    @flaky
    def test_big_object_performance(self):
        t1 = max(timeit.repeat('dumps(d)',
                               'from dpark.serialize import dumps;'
                               'd = {(str(i),):i for i in range(10000)}',
                               repeat=3, number=1))
        _import = ('from pickle import dumps;'
                   if six.PY2
                   else 'from pickle import _dumps as dumps;')
        t2 = max(timeit.repeat('dumps(d, 2)',
                               _import +
                               'd = {(str(i),):i for i in range(10000)}',
                               repeat=3, number=1))
        assert t1 < t2 * 2.5

    def testNoneAsFreeVar(self):
        y = None
        x = 10

        def foo():
            return x, y

        dumped_func = dump_closure(foo)
        func = load_closure(dumped_func)

        self.assertEqual(func(), (x, y))

    def testRandomSample(self):
        from random import sample, Random
        _sample = loads(dumps(sample))
        assert _sample.__self__.__class__ is sample.__self__.__class__
        assert _sample.__func__ is sample.__func__
        assert isinstance(_sample.__self__, Random)
        assert isinstance(sample.__self__, Random)

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

    def testOldStyleClass(self):
        exec_str = """
import csv

class DumDialect(csv.Dialect):
    delimiter = '\\t'
    quotechar = '"'
    escapechar = '\\\\'
    doublequote = False
    skipinitialspace = False
    lineterminator = '\\n'
    quoting = csv.QUOTE_MINIMAL
"""
        with main_environ(exec_str):
            loads(dumps(MAIN.DumDialect))

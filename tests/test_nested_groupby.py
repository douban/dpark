from __future__ import absolute_import
import sys
import os
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dpark.utils.nested_groupby import (
    GroupByNestedIter, list_value, group_by_simple, cogroup_no_dup, list_nested_cogroup
)


class TestGroup(unittest.TestCase):

    def setUp(self):
        GroupByNestedIter.NO_CACHE = False

    def _test(self, lst):
        exp = list(group_by_simple(lst))

        def _(x):
            return list(map(list_value, x))

        nit1 = GroupByNestedIter(iter(lst))
        res1 = _(nit1)

        self.assertEqual(exp, res1, res1)
        self.assertFalse(nit1.is_cached)
        self.assertRaises(StopIteration, next, nit1)

        nit2 = GroupByNestedIter(iter(lst))
        res2 = list(nit2)
        res2 = _(res2)

        self.assertEqual(exp, res2, res2)
        self.assertRaises(StopIteration, next, nit2)
        if len(lst) > 1:
            self.assertTrue(nit2.is_cached, res2)

        if lst:
            nit3 = GroupByNestedIter(iter(lst))
            k1, vit1 = next(nit3)
            res3 = _(nit3)
            vs1 = list(vit1)
            self.assertRaises(StopIteration, next, vit1)
            res3 = [(k1, vs1)] + res3
            self.assertEqual(exp, res3, res3)

    def test_groupby(self):
        self._test([])
        self._test([(1, [1, 1]), (1, [3, 3])])
        self._test([(1, []), (1, [3, 3])])
        self._test([(1, []), (1, [])])
        self._test([(1, []), (1, []), (2, [1])])
        n = 10
        m = 3
        lists = [(i // m, [i]) for i in range(n * m)]
        self._test(lists)


class TestCoGroup(unittest.TestCase):

    def setUp(self):
        GroupByNestedIter.NO_CACHE = True

    def tearDown(self):
        GroupByNestedIter.NO_CACHE = False

    def test_cogroup(self):
        lsts = [[('k', range(i, 10, 3))] for i in range(3)]
        res = cogroup_no_dup(lsts)
        res = list_nested_cogroup(res)
        exp = [('k', tuple([list(range(i, 10, 3)) for i in range(3)]))]
        self.assertEqual(exp, res)

        # test empty iter

    def test_empty(self):
        lsts = [[(1, [1, 1])], [], [(1, [3, 3])]]
        res = cogroup_no_dup(lsts)
        res = list_nested_cogroup(res)
        exp = [(1, ([1, 1], [], [3, 3]))]
        self.assertEqual(exp, res)

    def test_nested_cogroup(self):
        def _(i):
            v = list(range(i, 10, 3))
            lst = [('k1', v)] * 2 + [('k2', list(v))]
            return GroupByNestedIter(lst)

        lsts = [_(i) for i in range(3)]
        res = cogroup_no_dup(lsts)
        res = list_nested_cogroup(res)

        exp = [('k1', tuple([list(range(i, 10, 3)) * 2 for i in range(3)]))] + \
              [('k2', tuple([list(range(i, 10, 3)) for i in range(3)]))]
        self.assertEqual(exp, res)


if __name__ == "__main__":
    unittest.main()

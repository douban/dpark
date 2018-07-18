# -*- coding: utf-8 -*-

from __future__ import absolute_import
import random
import sys
import os
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dpark.utils.heaponkey import HeapOnKey
from pprint import pprint


class TestHeap(unittest.TestCase):

    def test_merge(self):
        N = 100
        n = 13
        a = list(range(N))
        random.shuffle(a)
        a = list(enumerate(a))
        b = a
        lsts = []
        while len(b):
            lsts.append(b[:n])
            b = b[n:]

        key = lambda x: x[1]
        lsts = list(map(lambda x: sorted(x, key=key), lsts))
        # pprint(lsts)

        h = HeapOnKey(key=key, min_heap=True)
        r = list(h.merge(lsts))
        exp = sorted(a, key=key)
        # pprint(exp)
        # pprint(r)

        assert r == exp


if __name__ == "__main__":
    unittest.main()

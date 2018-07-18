from __future__ import absolute_import
import os, sys
import unittest
import random
from six.moves import range

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dpark.utils.bitindex import BitIndex, Bloomfilter


class TestBitIndex(unittest.TestCase):
    def test_sets(self):
        a = BitIndex()
        self.assertEqual(a.size, 0)
        self.assertEqual(len(a.array), 0)
        self.assertFalse(bool(a))
        self.assertEqual(str(a), '')

        a.set(100, True)
        self.assertEqual(a.size, 101)
        self.assertEqual(len(a.array), (a.size + 7) >> 3)
        self.assertEqual(a.array, b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10')
        self.assertTrue(bool(a))
        self.assertEqual(str(a), ''.join(['0'] * 100 + ['1']))

        a.sets([1, 3, 16])
        self.assertEqual(a.size, 101)
        self.assertEqual(len(a.array), (a.size + 7) >> 3)
        self.assertEqual(a.array, b'\x0a\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10')

        a.append()
        self.assertEqual(a.size, 102)
        self.assertEqual(len(a.array), (a.size + 7) >> 3)
        self.assertEqual(a.array, b'\x0a\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x30')

        a.append(0)
        self.assertEqual(a.size, 103)
        self.assertEqual(len(a.array), (a.size + 7) >> 3)
        self.assertEqual(a.array, b'\x0a\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x30')

        a.appends([1, 0, 1, True, 1, None])
        self.assertEqual(a.size, 109)
        self.assertEqual(len(a.array), (a.size + 7) >> 3)
        self.assertEqual(a.array, b'\x0a\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb0\x0e')

    def test_gets(self):
        a = BitIndex()
        pos = [random.randint(0, 1000) for i in range(100)]
        a.sets(pos)
        self.assertEqual(a.size, max(pos) + 1)
        self.assertEqual(len(a.array), (a.size + 7) >> 3)

        self.assertTrue(a.get(pos[0]))
        self.assertTrue(all(a.gets(pos)))
        self.assertEqual(list(a.positions()), sorted(set(pos)))

    def test_operations(self):
        a = BitIndex()
        pos_a = set(random.randint(0, 1000) for i in range(100))
        a.sets(pos_a)
        b = BitIndex()
        pos_b = set(random.randint(0, 1000) for i in range(100))
        b.sets(pos_b)
        c = BitIndex()
        pos_c = set(random.randint(0, 1000) for i in range(100))
        c.sets(pos_c)

        self.assertEqual(list(a.intersect(b)), sorted(pos_a & pos_b))
        self.assertEqual(list(a.intersect(b, c)), sorted(pos_a & pos_b & pos_c))

        self.assertEqual(list(a.union(b)), sorted(pos_a | pos_b))
        self.assertEqual(list(a.union(b, c)), sorted(pos_a | pos_b | pos_c))

        self.assertEqual(list(a.excepts(b)), sorted(pos_a ^ (pos_a & pos_b)))
        self.assertEqual(list(a.excepts(b, c)), sorted(pos_a ^ (pos_a & (pos_b | pos_c))))

        self.assertEqual(list(a.xor(b)), sorted(pos_a ^ pos_b))

    def test_bloomfilter(self):
        m, k = Bloomfilter.calculate_parameters(100000, 0.01)
        b = Bloomfilter(m, k)
        keys = [random.randint(0, 80000) for i in range(40000)]
        b.add(keys)
        self.assertTrue(keys[0] in b)
        self.assertTrue(all(b.match(keys)))
        self.assertTrue(len([_f for _f in b.match(range(80000, 100000)) if _f]) < 100000 * 0.01)

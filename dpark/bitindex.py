import itertools
import marshal
import math
from dpark.portable_hash import portable_hash

BYTE_SHIFT = 3
BYTE_SIZE = 1 << BYTE_SHIFT
BYTE_MASK = BYTE_SIZE - 1

_table = [(), (0,), (1,), (0,1), (2,), (0,2), (1,2), (0,1,2), (3,),
          (0,3), (1,3), (0,1,3), (2,3), (0,2,3), (1,2,3), (0,1,2,3)]

class BitIndex(object):
    def __init__(self):
        self.array = bytearray()
        self.size = 0

    def __nonzero__(self):
        return any(self.array)

    def __repr__(self):
        def to_bin(x, s):
            return bin(x)[2:].zfill(s)[::-1]

        index, offset = self._get_offset(self.size)
        rep = [to_bin(x, BYTE_SIZE) for x in self.array[:index]]
        if offset:
            rep.append(to_bin(self.array[index] & (1 << offset) - 1, offset))
        return ''.join(rep)

    def __len__(self):
        return self.size

    @staticmethod
    def _get_offset(pos):
        return pos >> BYTE_SHIFT, pos & BYTE_MASK

    @staticmethod
    def _bitwise(iterable, op):
        for i, it in enumerate(iterable):
            if op:
                byte = reduce(op, it)
            else:
                byte = it

            if not byte:
                continue

            for x in _table[byte & 0xF]:
                yield i * BYTE_SIZE + x

            for x in _table[byte >> 4]:
                yield i * BYTE_SIZE + 4 + x

    def set(self, pos, value = True):
        if pos < 0:
            raise ValueError('pos must great or equal zero!')

        index, offset = self._get_offset(pos)
        length = len(self.array)
        if index >= length:
            self.array.extend([0] * (index - length + 1))

        if value:
            self.array[index] |= (1 << offset)
        else:
            self.array[index] &= ~(1 << offset)

        self.size = max(self.size, pos + 1)

    def sets(self, positions, value = True):
        for pos in positions:
            self.set(pos, value)

    def append(self, value = True):
        self.set(self.size, value)

    def appends(self, values):
        for value in values:
            self.append(value)

    def get(self, pos):
        if pos < 0:
            raise ValueError('pos must great or equal zero!')

        if pos >= self.size:
            return False

        index, offset = self._get_offset(pos)
        return (self.array[index] & (1 << offset)) != 0

    def gets(self, positions):
        for pos in positions:
            yield self.get(pos)

    def intersect(self, *other):
        return self._bitwise(
            itertools.izip(self.array, *[o.array for o in other]), lambda x, y: x & y)

    def union(self, *other):
        return self._bitwise(
            itertools.izip_longest(self.array, *[o.array for o in other], fillvalue=0),
            lambda x, y: x | y)

    def xor(self, other):
        return self._bitwise(itertools.izip_longest(self.array, other.array, fillvalue=0),
                        lambda x, y: x ^ y)

    def excepts(self, *other):
        return self._bitwise(
            itertools.izip_longest(self.array, *[o.array for o in other], fillvalue=0),
            lambda x, y: x & ~y)

    def positions(self):
        return self._bitwise(self.array, None)

class Bloomfilter(object):
    def __init__(self, m, k):
        self.m = m
        self.k = k
        self.bitindex = BitIndex()

    """ Bloomfilter calculator (http://hur.st/bloomfilter)
    n: Number of items in the filter
    p: Probability of false positives, float between 0 and 1 or a number
    indicating 1-in-p
    m: Number of bits in the filter
    k: Number of hash functions
    m = ceil((n * log(p)) / log(1.0 / (pow(2.0, log(2.0)))))
    k = round(log(2.0) * m / n)
    """
    @staticmethod
    def calculate_parameters(n, p):
        m = int(math.ceil(n * math.log(p) * -2.0813689810056073))
        k = int(round(0.6931471805599453 * m / n))
        return m, k


    '''
    we're using only two hash functions with different settings, as described
    by Kirsch & Mitzenmacher: http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
    '''
    def _get_offsets(self, obj):
        hash_1 = portable_hash(obj)
        hash_2 = portable_hash(marshal.dumps(obj))

        for i in xrange(self.k):
            yield ((hash_1 + i * hash_2) & 0xFFFFFFFF) % self.m

    def add(self, objs):
        for obj in objs:
            self.bitindex.sets(self._get_offsets(obj))

    def _match(self, objs):
        for obj in objs:
            yield all(self.bitindex.gets(self._get_offsets(obj)))

    def match(self, objs):
        return list(self._match(objs))

    def __contains__(self, obj):
        return self._match([obj]).next()


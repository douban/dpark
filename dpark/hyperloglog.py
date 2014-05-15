import math
from bisect import bisect_right
import array

try:
    import pyhash
    hash_func = pyhash.murmur2_x64_64a()
    HASH_LEN = 64
    raise ImportError
except ImportError:
    HASH_LEN = 30
    def hash_func(v):
        return hash(v) & 0x3fffffff

SPARSE = 0
NORMAL = 1

class HyperLogLog(object):
    def __init__(self, items=[], b=None, err=0.01):
        assert 0.005 <= err < 0.14, 'must 0.005 < err < 0.14'
        if b is None:
            b = int(math.ceil(math.log((1.04 / err) ** 2, 2)))
        self.alpha = self._get_alpha(b)
        self.b = b
        self.m = 1 << b
        self.M = None #array.array('B', [0] * self.m)
        self.threshold = self.m / 20
        self.items = set(items)
        self.mask = (1<<b) -1
        self.big = 1L << (HASH_LEN - b - 1)
        self.big2 = 1L << (HASH_LEN - b - 2)
        self.bitcount_arr = [1L << i for i in range(HASH_LEN - b + 1)]

    @staticmethod
    def _get_alpha(b):
        assert 4 <= b <= 16, 'b=%d should be in range [4,16]' % b
        alpha = (0.673, 0.697, 0.709)
        if b <= 6:
            return alpha[b-4]
        return 0.7213 / (1.0 + 1.079 / (1 << b))

    def _get_rho(self, w):
        # fast path
        if w > self.big:
            return 1
        if w > self.big2:
            return 2
        return len(self.bitcount_arr) - bisect_right(self.bitcount_arr, w)

    def convert(self):
        self.M = array.array('B', [0] * self.m)
        for i in self.items:
            self.add(i)
        self.items = None

    def add(self, value):
        if self.M is None:
            self.items.add(value)
            if len(self.items) > self.threshold:
                self.convert()
            return

        x = hash_func(value)
        j = x & self.mask
        w = x >> self.b
        h = self._get_rho(w)
        if h > self.M[j]:
            self.M[j] = h

    def update(self, other):
        if other.M is None:
            if self.M is None:
                self.items.update(other.items)
            else:
                for i in other.items:
                    self.add(i)
            return

        if self.M is None:
            self.convert()
        self.M = array.array('B', map(max, zip(self.M, other.M)))

    def __len__(self):
        if self.M is None:
            return len(self.items)

        S = sum(math.pow(2.0, -x) for x in self.M)
        E = self.alpha * self.m * self.m / S
        if E <= 2.5 * self.m: # small range correction
            V = self.M.count(0)
            return self.m * math.log(self.m / float(V)) if V > 0 else E
        elif E <= float(1L << HASH_LEN) / 30.0: # intermidiate range correction -> No correction
            return E
        else:
            return -(1L << HASH_LEN) * math.log(1.0 - E / (1L << HASH_LEN))

def test(l, err=0.03):
    hll = HyperLogLog(err)
    for i in l:
        hll.add(str(i)+'ip')
    le = len(hll)
    print err*100.0, len(hll.M), len(l), le, (le-len(l)) * 100.0 / len(l)

if __name__ == '__main__':
    for e in (0.005, 0.01, 0.03, 0.05, 0.1):
        test(xrange(100), e)
        test(xrange(10000), e)
        test(xrange(100000), e)
#        test(xrange(1000000), e)

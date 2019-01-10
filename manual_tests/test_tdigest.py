import sys
import pickle
import random
from math import isnan, asin, pi
from functools import reduce
from flaky import flaky
from dpark.utils.tdigest import TDigest

if sys.version_info.major < 3:
    range = xrange


def rint(x):
    if x % 1 == 0.5 and not int(x) % 2:
        return int(x)
    return round(x)


def quantile(data, q):
    if len(data) == 0:
        return float('nan')

    if q == 1 or len(data) == 1:
        return data[-1]

    index = q * len(data)
    if index < 0.5:
        return data[0]
    elif len(data) - index < 0.5:
        return data[-1]

    index -= 0.5
    idx = int(index)
    return data[idx + 1] * (index - idx) + data[idx] * (idx + 1 - index)


def cdf(data, x):
    n1 = n2 = 0
    for d in data:
        n1 += 1 if d < x else 0
        n2 += 1 if d <= x else 0

    return (n1 + n2) / 2. / len(data)


def test_single_value():
    digest = TDigest()
    value = random.random() * 1000
    digest.add(value)
    for q in [0, random.random(), 1]:
        assert abs(value - digest.quantile(q)) < 1e-3


def test_few_values():
    digest = TDigest()
    length = random.randint(1, 10)
    values = []
    for i in range(length):
        if i == 0 or random.random() < 0.5:
            value = random.random() * 100
        else:
            value = values[-1]

        digest.add(value)
        values.append(value)

    values = sorted(values)
    assert len(digest.centroids) == len(values)
    for q in [0, 1e-10, random.random(), 0.5, 1 - 1e-10, 1]:
        q1 = quantile(values, q)
        q2 = digest.quantile(q)
        assert abs(q1 - q2) < 0.03


def test_empty_digest():
    digest = TDigest()
    assert len(digest.centroids) == 0


def test_empty():
    digest = TDigest()
    q = random.random()
    assert isnan(digest.quantile(q))


def test_more_than_2billion_values():
    digest = TDigest()
    for i in range(1000):
        digest.add(random.random())

    for i in range(10):
        digest.add(random.random(), 1 << 28)

    assert len(digest) == 1000 + 10 * (1 << 28)
    prev = None
    for q in sorted([0, 0.1, 0.5, 0.9, 1, random.random()]):
        v = digest.quantile(q)
        assert prev is None or v >= prev
        prev = v


def test_sorted():
    digest = TDigest()
    for i in range(10000):
        digest.add(random.random(), 1 + random.randint(0, 10))

    prev = None
    for c in digest.centroids:
        assert prev is None or prev.mean <= c.mean
        prev = c


def test_nan():
    digest = TDigest()
    iters = random.randint(0, 10)
    for i in range(iters):
        digest.add(random.random(), 1 + random.randint(0, 10))

    try:
        if random.random() < 0.5:
            digest.add(float('nan'))
        else:
            digest.add(float('nan'), 1)

        assert False
    except ValueError:
        pass


def _test_distribution(digest, data, q_values, size_guide):
    for d in data:
        digest.add(d)

    data = sorted(data)
    x_values = [quantile(data, q) for q in q_values]
    qz = iz = 0
    for c in digest.centroids:
        qz += c.count
        iz += 1

    assert qz == len(digest)
    assert iz == len(digest.centroids)
    assert len(digest.centroids) < 20 * size_guide

    soft_errors = 0
    for x, q in zip(x_values, q_values):
        esimate = digest.cdf(x)
        assert abs(q - esimate) < 0.005

        estimate = cdf(data, digest.quantile(q))
        if abs(q - estimate) > 0.005:
            soft_errors += 1

        assert abs(q - estimate) < 0.012

    assert soft_errors < 3


def test_uniform():
    digest = TDigest()
    _test_distribution(
        digest, [random.uniform(0, 1) for _ in range(10000)],
        [0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999], 100
    )


def test_gamma():
    digest = TDigest()
    _test_distribution(
        digest, [random.gammavariate(0.1, 0.1) for _ in range(10000)],
        [0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999], 100
    )


def test_narrow_normal():
    digest = TDigest()
    data = []
    for i in range(10000):
        if random.random() < 0.5:
            data.append(random.gauss(0, 1e-5))
        else:
            data.append(random.uniform(-1, 1))

    _test_distribution(
        digest, data, [0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999], 100
    )


@flaky
def test_repeated_values():
    digest = TDigest()
    data = [rint(random.uniform(0, 1) * 10) / 10. for _ in range(10000)]

    for d in data:
        digest.add(d)

    assert len(digest.centroids) < 10 * 1000.
    for i in range(10):
        z = i / 10.
        for delta in [0.01, 0.02, 0.03, 0.07, 0.08, 0.09]:
            q = z + delta
            cdf = digest.cdf(q)
            assert abs(z + 0.05 - cdf) < 0.02

            estimate = digest.quantile(q)
            assert abs(rint(q * 10) / 10. - estimate) < 0.001


def test_sequential_points():
    digest = TDigest()
    data = [i * pi * 1e-5 for i in range(10000)]

    _test_distribution(
        digest, data, [0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999], 100
    )


def test_three_point_example():
    digest = TDigest()
    x0 = 0.18615591526031494
    x1 = 0.4241943657398224
    x2 = 0.8813006281852722
    digest.add(x0)
    digest.add(x1)
    digest.add(x2)

    p10 = digest.quantile(0.1)
    p50 = digest.quantile(0.5)
    p90 = digest.quantile(0.9)
    p95 = digest.quantile(0.95)
    p99 = digest.quantile(0.99)

    assert p10 <= p50
    assert p50 <= p90
    assert p90 <= p95
    assert p95 <= p99

    assert x0 == p10
    assert x2 == p90


def test_singleton_in_a_crowd():
    compression = 100
    digest = TDigest(compression=compression)
    for i in range(10000):
        digest.add(10)

    digest.add(20)
    digest.compress()

    assert digest.quantile(0) == 10.0
    assert digest.quantile(0.5) == 10.0
    assert digest.quantile(0.8) == 10.0
    assert digest.quantile(0.9) == 10.0
    assert digest.quantile(0.99) == 10.0
    assert digest.quantile(1) == 20.0


@flaky
def test_merge():
    for parts in [2, 5, 10, 20, 50, 100]:
        data = []
        digest = TDigest()
        subs = [TDigest() for _ in range(parts)]
        cnt = [0] * parts

        for i in range(10000):
            x = random.random()
            data.append(x)
            digest.add(x)
            subs[i % parts].add(x)
            cnt[i % parts] += 1

        digest.compress()
        data = sorted(data)

        k = 0
        for i, d in enumerate(subs):
            assert cnt[i] == len(d)
            k2 = sum(c.count for c in d.centroids)
            assert cnt[i] == k2
            k += k2

        assert k == len(data)

        digest2 = reduce(lambda x, y: x + y, subs)

        for q in [0.001, 0.01, 0.1, 0.2, 0.3, 0.5]:
            z = quantile(data, q)
            e2 = digest2.quantile(q) - z

            assert abs(e2) / q < 0.3
            assert abs(e2) < 0.015

        for q in [0.001, 0.01, 0.1, 0.2, 0.3, 0.5]:
            z = cdf(data, q)
            e2 = digest2.cdf(q) - z

            assert abs(e2) / q < 0.3
            assert abs(e2) < 0.015


def test_extreme():
    digest = TDigest()
    digest.add(10, 3)
    digest.add(20, 1)
    digest.add(40, 5)
    values = [5., 10., 15., 20., 30., 35., 40., 45., 50.]
    for q in [1.5 / 9, 3.5 / 9, 6.5 / 9]:
        assert abs(quantile(values, q) - digest.quantile(q)) < 0.01


def test_monocity():
    digest = TDigest()
    for i in range(10000):
        digest.add(random.random())

    for i in range(int(1e4) - 1):
        q1 = i * 1e-4
        q2 = (i + 1) * 1e-4
        assert digest.quantile(q1) <= digest.quantile(q2)
        assert digest.cdf(q1) <= digest.cdf(q2)


def test_serialization():
    digest = TDigest()
    for i in range(100):
        digest.add(random.random())

    digest2 = pickle.loads(pickle.dumps(digest))

    assert len(digest) == len(digest2)
    assert len(digest.centroids) == len(digest2.centroids)
    for c1, c2 in zip(digest.centroids, digest2.centroids):
        assert c1.mean == c2.mean
        assert c1.count == c2.count

    for q in range(10000):
        assert digest.quantile(q / 10000.) == digest2.quantile(q / 10000.)
        assert digest.cdf(q / 10000.) == digest2.cdf(q / 10000.)


def test_fill():
    def q_to_k(q):
        return asin(2 * min(1, q) - 1) / pi + 0.5

    delta = 300
    digest = TDigest(delta)
    for i in range(100000):
        digest.add(random.gauss(0, 1))

    q0 = 0.
    for c in digest.centroids:
        q1 = q0 + float(c.count) / len(digest)
        dk = delta * (q_to_k(q1) - q_to_k(q0))
        assert dk <= 1
        q0 = q1


def test_small_count_quantile():
    digest = TDigest(200)
    for d in [15.0, 20.0, 32.0, 60.0]:
        digest.add(d)

    assert abs(digest.quantile(0.4) - 21.2) < 1e-10

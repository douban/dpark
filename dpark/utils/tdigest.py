from math import isnan, ceil, pi
from six.moves import range


class Centroid(object):

    def __init__(self, x, w=1):
        self.__mean = float(x)
        self.__count = float(w)

    @property
    def mean(self):
        return self.__mean

    @property
    def count(self):
        return self.__count

    def __repr__(self):
        return """<Centroid: mean==%.8f, count=%d>""" % (self.mean, self.count)

    def __eq__(self, other):
        if isinstance(other, Centroid):
            return self.mean == other.mean and self.count == other.count

        return False


class TDigest(object):
    def __init__(self, compression=100, size=None):
        self._min = None
        self._max = None
        self.compression = compression
        self._total_weight = 0
        self._weight = []
        self._mean = []
        self._unmerge_weight = 0
        self._tmp_weight = []
        self._tmp_mean = []

        if size is None:
            size = int(2 * ceil(compression)) + 10

        self._size = size

    @staticmethod
    def _weighted_average(x1, w1, x2, w2):
        a, b = min(x1, x2), max(x1, x2)
        x = float(x1 * w1 + x2 * w2) / (w1 + w2)
        return max(a, min(b, x))

    def __len__(self):
        return int(self._total_weight + self._unmerge_weight)

    def __add__(self, other):
        if not isinstance(other, TDigest):
            raise TypeError('Can not add {} with {}'.format(
                self.__class__.__name__,
                other.__class__.__name__,
            ))

        if len(other) == 0:
            return self

        other.compress()

        self._tmp_mean.extend(other._mean)
        self._tmp_weight.extend(other._weight)
        total = sum(other._weight)
        self._unmerge_weight = total

        self.compress()

        return self

    def add(self, x, w=1):
        x = float(x)
        w = float(w)
        if isnan(x):
            raise ValueError('Cannot add NaN')

        if len(self._tmp_weight) + len(self._weight) >= self._size - 1:
            self.compress()

        self._tmp_weight.append(w)
        self._tmp_mean.append(x)
        self._unmerge_weight += w

    def compress(self):
        if self._unmerge_weight > 0:
            self._merge(self._tmp_weight, self._tmp_mean)
            self._tmp_weight = []
            self._tmp_mean = []
            self._unmerge_weight = 0

    def _merge(self, incoming_weight, incoming_mean):
        def _argsort(seq):
            return sorted(range(len(seq)), key=seq.__getitem__)

        incoming_weight = incoming_weight + self._weight
        incoming_mean = incoming_mean + self._mean

        assert incoming_weight

        incoming_order = _argsort(incoming_mean)

        self._total_weight += self._unmerge_weight

        normalizer = self.compression / (pi * self._total_weight)

        mean = []
        weight = []
        mean.append(incoming_mean[incoming_order[0]])
        weight.append(incoming_weight[incoming_order[0]])

        w_so_far = 0.
        for ix in incoming_order[1:]:
            proposed_weight = weight[-1] + incoming_weight[ix]
            z = proposed_weight * normalizer
            q0 = w_so_far / self._total_weight
            q2 = (w_so_far + proposed_weight) / self._total_weight
            if z * z <= q0 * (1 - q0) and z * z <= q2 * (1 - q2):
                weight[-1] += incoming_weight[ix]
                mean[-1] = mean[-1] + (incoming_mean[ix] - mean[-1]) * incoming_weight[ix] / weight[-1]

            else:
                w_so_far += weight[-1]
                mean.append(incoming_mean[ix])
                weight.append(incoming_weight[ix])

        self._mean = mean
        self._weight = weight
        # assert sum(weight) == self._total_weight

        if self._total_weight > 0:
            self._min = mean[0] if self._min is None else min(self._min, mean[0])
            self._max = mean[-1] if self._max is None else max(self._max, mean[-1])

    def quantile(self, q):
        q = float(q)
        if not 0 <= q <= 1:
            raise ValueError('q should be in [0, 1], got {}'.format(q))

        self.compress()
        weight = self._weight
        mean = self._mean

        if not weight:
            return float('nan')
        elif len(weight) == 1:
            return mean[0]

        index = q * self._total_weight
        if index < weight[0] / 2:
            return self._min + 2. * index / weight[0] * (mean[0] - self._min)

        weight_so_far = weight[0] / 2.
        for i in range(len(weight) - 1):
            dw = (weight[i] + weight[i + 1]) / 2.
            if weight_so_far + dw > index:
                z1 = index - weight_so_far
                z2 = weight_so_far + dw - index
                return self._weighted_average(mean[i], z2, mean[i + 1], z1)

            weight_so_far += dw

        assert index <= self._total_weight
        assert index >= self._total_weight - weight[-1] / 2.

        z1 = index - self._total_weight - weight[-1] / 2.
        z2 = weight[-1] / 2. - z1
        return self._weighted_average(mean[-1], z1, self._max, z2)

    def cdf(self, x):
        x = float(x)
        self.compress()
        weight = self._weight
        mean = self._mean

        if not weight:
            return float('nan')
        elif len(weight) == 1:
            width = self._max - self._min
            if x < self._min:
                return 0.
            elif x > self._max:
                return 1.
            elif x - self._min <= width:
                return 0.5
            else:
                return (x - self._min) / (self._max - self._min)

        if x < self._min:
            return 0.

        if x > self._max:
            return 1.

        if x <= mean[0]:
            if mean[0] - self._min > 0:
                return (x - self._min) / (mean[0] - self._min) * weight[0] / self._total_weight / 2.
            else:
                return 0.

        if x >= mean[-1]:
            if self._max - mean[-1] > 0:
                return 1. - (self._max - x) / (self._max - mean[-1]) * weight[-1] / self._total_weight / 2.

            else:
                return 1.

        weight_so_far = weight[0] / 2.
        for it in range(len(weight) - 1):
            if mean[it] == x:
                w0 = weight_so_far
                weight_so_far += sum(
                    weight[i] + weight[i + 1]
                    for i in range(it, len(weight) - 1)
                    if mean[i + 1] == x
                )
                return (w0 + weight_so_far) / 2. / self._total_weight

            if mean[it] <= x < mean[it + 1]:
                if mean[it + 1] - mean[it] > 0:
                    dw = (weight[it] + weight[it + 1]) / 2.
                    return (weight_so_far +
                            dw * (x - mean[it]) / (mean[it + 1] - mean[it])) / self._total_weight
                else:
                    dw = (weight[it] + weight[it + 1]) / 2.
                    return weight_so_far + dw / self._total_weight

            weight_so_far += (weight[it] + weight[it + 1]) / 2.

        assert False

    @property
    def centroids(self):
        self.compress()
        weight = self._weight
        mean = self._mean
        return [Centroid(mean[i], weight[i]) for i in range(len(self._weight))]

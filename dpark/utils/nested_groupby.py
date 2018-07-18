import weakref
import sys
from collections import deque
from dpark.utils.log import get_logger

logger = get_logger(__name__)

if sys.version_info[0] < 3:
    def next_func(it):
        return it.next
else:
    def next_func(it):
        return it.__next__


def list_value(x):
    return x[0], list(x[1])


def list_nested_group(it):
    return list(map(list_value, it))


def list_values(x):
    return x[0], tuple(map(list, x[1]))


def list_nested_cogroup(it):
    return list(map(list_values, it))


def group_by_simple(it):
    key = None
    values = []
    i = -1
    for i, (k, vs) in enumerate(it):
        if i == 0:
            key = k
            values = list(vs)
        elif k == key:
            values.extend(vs)
        else:
            yield key, values
            key = k
            values = list(vs)
    if i >= 0:
        yield key, values


class GroupBySubIter(object):

    def __init__(self, key, next_block_func):
        self._key = key
        self._next_block_func = next_block_func
        self._blocks = None
        self._finished = False

    def __iter__(self):
        next_block_func = self._next_block_func
        key = self._key
        while True:
            if self._blocks is not None:
                if self._blocks:
                    v = self._blocks.popleft()
                else:
                    break
            else:
                kv = next_block_func(key)
                if kv is None:
                    break
                v = kv[1]

            it = iter(v)
            try:
                for i in it:
                    yield i
            except StopIteration:
                pass
        self._finished = True

    def get_all_blocks(self):
        if self._finished:
            return False
        key = self._key
        next_block_func = self._next_block_func
        blocks = self._blocks = deque()
        while True:
            kv = next_block_func(key)
            if kv is None:
                break
            blocks.append(kv[1])
        self._finished = True
        return len(blocks)

    # def __del__(self):
    #     print('(Deleting %s)' % self)


class GroupByNestedIter(object):
    NO_CACHE = False

    def __init__(self, it, owner_info=None):
        self._it = iter(it)
        self._prev_key = None
        self._prev_sub_it = None
        self._next_block = None
        self.owner_info = owner_info
        self.is_cached = False

    def _next_block_for_key(self, k):
        """return None when meet a diff key or the end
           else return and then update _next_block
        """
        if self._next_block is None:  # start/end
            try:
                self._next_block = next(self._it)
            except StopIteration:
                return

        k_, v_ = self._next_block
        if k == k_:
            try:
                self._next_block = next(self._it)
            except StopIteration:
                self._next_block = None
                pass
            return k_, v_
        else:
            return

    def _next_key(self, key):
        while self._next_block_for_key(key) is not None:
            pass

    def __iter__(self):
        return self

    def __next__(self):
        if self._prev_key is not None:
            prev_sub_it = self._prev_sub_it()
            if prev_sub_it is not None:
                if prev_sub_it.get_all_blocks() and not self.is_cached:
                    self.is_cached = True
                    msg = "GroupByNestedIter caching values. owner: %s" % (self.owner_info,)
                    if GroupByNestedIter.NO_CACHE:  # for test
                        raise Exception(msg)
                    else:
                        logger.warning(msg)

        self._next_key(self._prev_key)

        if self._next_block is None:
            raise StopIteration

        key = self._next_block[0]
        sub_it = GroupBySubIter(key, self._next_block_for_key)
        self._prev_key, self._prev_sub_it = key, weakref.ref(sub_it)
        return key, iter(sub_it)

    next = __next__


def cogroup_no_dup(iters):
    iters = list(map(iter, iters))
    funcs = [next_func(it) for it in iters]
    curr = [[[None], i] for i, f in enumerate(funcs)]

    def _key(x):
        return x[0][0]

    n = len(iters)
    l0 = list([[] for _ in range(n)])

    r = 0

    while n:
        if r == 0:
            min_ = None
        else:
            min_ = min(curr, key=_key)[0][0]
            t = list(l0)
            for j, (kv, i) in enumerate(curr):
                if kv[0] == min_:
                    t[i] = kv[1]
            yield min_, tuple(t)

        to_del = []
        for j, (kv, i) in enumerate(curr):
            if kv[0] == min_:
                try:
                    curr[j] = [funcs[i](), i]
                except StopIteration:
                    to_del.append(j)
                    n -= 1
        for j, i in enumerate(to_del):
            del curr[i - j]
        r += 1

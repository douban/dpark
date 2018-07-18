from __future__ import absolute_import
from __future__ import print_function
from dpark.serialize import load_func, dump_func
import sys
import operator
from six.moves import range

if sys.version_info[0] < 3:
    def next_func(it):
        return it.next
else:
    def next_func(it):
        return it.__next__


class HeapOnKey(object):

    def __init__(self, key=None, min_heap=False):
        self.key = key
        self.min_heap = min_heap
        self._setup_cmp()

    def _setup_cmp(self):
        key = self.key
        min_heap = self.min_heap

        def _ge0(x, y):
            return not (x < y)

        def _lt(x, y):
            return key(x) < key(y)

        def _ge(x, y):
            return not (key(x) < key(y))

        if key is None:
            self.cmp_lt = operator.lt if min_heap else _ge0
        else:
            self.cmp_lt = _lt if min_heap else _ge

    def __getstate__(self):
        return dump_func(self.key), self.min_heap

    def __setstate__(self, state):
        key_f, self.min_heap = state
        self.key = load_func(key_f)
        self._setup_cmp()

    def push(self, heap, item):
        heap.append(item)
        self._sift_down(heap, 0, len(heap) - 1)

    def pop(self, heap):
        last_item = heap.pop()
        if heap:
            ret_item = heap[0]
            heap[0] = last_item
            self._sift_up(heap, 0)
        else:
            ret_item = last_item
        return ret_item

    def push_pop(self, heap, item):
        if heap and self.cmp_lt(heap[0], item):
            item, heap[0] = heap[0], item
            self._sift_up(heap, 0)
        return item

    def heapify(self, heap):
        n = len(heap)
        for i in range(n // 2 - 1, -1, -1):
            self._sift_up(heap, i)

    def _sift_down(self, heap, start_pos, pos):
        new_item = heap[pos]
        cmp_lt = self.cmp_lt
        while pos > start_pos:
            parent_pos = (pos - 1) >> 1
            parent = heap[parent_pos]
            if cmp_lt(new_item, parent):
                heap[pos] = parent
                pos = parent_pos
                continue
            break
        heap[pos] = new_item

    def _sift_up(self, heap, pos):
        end_pos = len(heap)
        child_pos = 2 * pos + 1
        cmp_lt = self.cmp_lt
        while child_pos < end_pos:
            right_pos = child_pos + 1
            if right_pos < end_pos and not cmp_lt(heap[child_pos], heap[right_pos]):
                child_pos = right_pos
            if cmp_lt(heap[pos], heap[child_pos]):
                break
            heap[pos], heap[child_pos] = heap[child_pos], heap[pos]
            pos = child_pos
            child_pos = 2 * pos + 1

    def replace(self, heap, item):
        returnitem = heap[0]  # raises appropriate IndexError if heap is empty
        heap[0] = item
        self._sift_up(heap, 0)
        return returnitem

    def merge(self, iterables, ordered_iters=0):
        """iterables: each is sorted
           ordered_iters: when come to equal value, the element in the first iter yields
                first(last) if ordered_iters >(<) 0
                not stable if ordered_iters == 0
        """
        if not ordered_iters:

            def key(x):
                return self.key(x[0])
        else:

            def key(x):
                return self.key(x[0]), x[1]

        heap = HeapOnKey(key, self.min_heap)
        _heappop, _heapreplace, _StopIteration = heap.pop, heap.replace, StopIteration
        _len = len

        h = []
        h_append = h.append
        order = -1 if ordered_iters and ((ordered_iters > 0) ^ self.min_heap) else 1

        for it_idx, it in enumerate(map(iter, iterables)):
            try:
                _next = next_func(it)
                h_append([_next(), order * it_idx, _next])
            except _StopIteration:
                pass
        heap.heapify(h)

        while _len(h) > 1:
            try:
                while 1:
                    v, _, _next = s = h[0]
                    yield v
                    s[0] = _next()  # raises StopIteration when exhausted
                    _heapreplace(h, s)  # restore heap condition
            except _StopIteration:
                _heappop(h)  # remove empty iterator
        if h:
            # fast case when only a single iterator remains
            v, _, _next = h[0]
            yield v
            for v in _next.__self__:
                yield v


def test():
    lst = [10, 9, 20, 18, 3, 24, 29, 39]
    h = HeapOnKey()
    h.heapify(lst)
    import sys
    print('the list after heapify:', lst, file=sys.stderr)
    lst.pop()
    lst[0] = 12
    h._sift_up(lst, 0)
    print('the list after sift up:', lst, file=sys.stderr)
    h.push(lst, 8)
    print('the list after push:', lst, file=sys.stderr)
    ret = h.pop(lst)
    print('the list after pop:', lst, ' with value:', ret, file=sys.stderr)
    h = HeapOnKey(min_heap=True)
    h.heapify(lst)
    h.push(lst, 12)
    print('the list after reverse:', lst, file=sys.stderr)

    class Foo:
        def __init__(self, name='', age=0, score=None):
            self.name = name
            self.age = age
            self.score = score

        def __getstate__(self):
            return self.name, self.age, self.score

        def __setstate__(self, state):
            self.name, self.age, self.score = state

        def __repr__(self):
            return '(name:' + self.name + ', age:' + str(self.age) + ', score(%d, %d, %d))' % self.score

    def key_func(foo):
        return foo.age

    lst = [Foo('aaa', 10, (60, 89, 70)), Foo('bbb', 20, (78, 75, 60)),
           Foo('ccc', 15, (60, 73, 84)), Foo('ddd', 21, (87, 64, 65)),
           Foo('eee', 30, (54, 53, 79)), Foo('fff', 10, (87, 73, 98))]
    h = HeapOnKey(key=key_func, min_heap=True)
    h.heapify(lst)
    print('the list after heapify:', lst, file=sys.stderr)

    len_l = 100000
    top_n = 10
    lst = []
    import random
    for i in range(len_l):
        lst.append(random.randint(1, 2 * len_l + 1))

    top_l = []
    call_cnt = 0

    def cnt_key(x):
        global call_cnt
        call_cnt += 1
        return x

    h = HeapOnKey(key=cnt_key)
    import time
    start = time.time()
    for i in lst:
        if len(top_l) >= top_n:
            h.push_pop(top_l, i)
        else:
            h.push(top_l, i)
    top_l.sort(key=cnt_key)
    print('after heap:', top_l, ' with elapsed:', time.time() - start, ' with cnt:', call_cnt / 2, file=sys.stderr)
    call_cnt = 0

    start = time.time()
    top_l = []
    for i in lst:
        top_l.append(i)
        top_l.sort(key=cnt_key)
        if len(top_l) > top_n:
            top_l.pop()

    print('after sort:', top_l, ' with elapsed:', time.time() - start, ' with cnt:', call_cnt / 2, file=sys.stderr)


if __name__ == '__main__':
    test()

from dpark.serialize import load_func, dump_func
import sys

class HeapOnKey:
    def __init__(self, key=None, min_heap=False):
        self.key = key
        self.min_heap = min_heap

    def __getstate__(self):
        return dump_func(self.key), self.min_heap

    def __setstate__(self, state):
        f, self.min_heap  = state
        self.key= load_func(f)

    def cmp_lt(self, x, y):
        c_x, c_y = (self.key(x), self.key(y)) if self.key else (x, y)
        if self.min_heap:
            return c_x < c_y
        else:
            return not c_x < c_y

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
        for i in xrange(n//2 - 1, -1, -1):
            self._sift_up(heap, i)

    def _sift_down(self, heap, start_pos, pos):
        new_item = heap[pos]
        while pos > start_pos:
            parent_pos = (pos - 1) >> 1
            parent = heap[parent_pos]
            if self.cmp_lt(new_item, parent):
                heap[pos] = parent
                pos = parent_pos
                continue
            break
        heap[pos] = new_item

    def _sift_up(self, heap, pos):
        end_pos = len(heap)
        child_pos = 2 * pos + 1
        while child_pos < end_pos:
            right_pos = child_pos + 1
            if right_pos < end_pos and not self.cmp_lt(heap[child_pos], heap[right_pos]):
                child_pos = right_pos
            if self.cmp_lt(heap[pos], heap[child_pos]):
                break
            heap[pos], heap[child_pos] = heap[child_pos], heap[pos]
            pos = child_pos
            child_pos = 2 * pos + 1


if __name__ == '__main__':
    l = [10, 9, 20, 18, 3, 24, 29, 39]
    h = HeapOnKey()
    h.heapify(l)
    import sys
    print >>sys.stderr, 'the list after heapify:', l
    l.pop()
    l[0] = 12
    h._sift_up(l, 0)
    print >>sys.stderr, 'the list after sift up:', l
    h.push(l, 8)
    print >>sys.stderr, 'the list after push:', l
    ret = h.pop(l)
    print >>sys.stderr, 'the list after pop:', l, ' with value:', ret
    h = HeapOnKey(min_heap=True)
    h.heapify(l)
    h.push(l, 12)
    print >>sys.stderr, 'the list after reverse:', l

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

    l = [Foo('aaa', 10, (60, 89, 70)), Foo('bbb', 20, (78, 75, 60)),
         Foo('ccc', 15, (60, 73, 84)), Foo('ddd', 21, (87, 64, 65)),
         Foo('eee', 30, (54, 53, 79)), Foo('fff', 10, (87, 73, 98))]
    h = HeapOnKey(key=key_func, min_heap=True)
    h.heapify(l)
    print >>sys.stderr, 'the list after heapify:', l

    len_l = 100000
    top_n = 10
    l = []
    import random
    for i in range(len_l):
        l.append(random.randint(1, 2 * len_l + 1))

    top_l = []
    call_cnt = 0

    def cnt_key(x):
        global call_cnt
        call_cnt += 1
        return x
    h = HeapOnKey(key=cnt_key)
    import time
    start = time.time()
    for i in l:
        if len(top_l) >= top_n:
            h.push_pop(top_l, i)
        else:
            h.push(top_l, i)
    top_l.sort(key=cnt_key)
    print >>sys.stderr, 'after heap:', top_l, ' with elapsed:', time.time() - start, ' with cnt:', call_cnt / 2
    call_cnt = 0

    start = time.time()
    top_l = []
    for i in l:
        top_l.append(i)
        top_l.sort(key=cnt_key)
        if len(top_l) > top_n:
            top_l.pop()

    print >>sys.stderr, 'after sort:', top_l, ' with elapsed:', time.time() - start, ' with cnt:', call_cnt / 2

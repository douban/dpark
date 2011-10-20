from multiprocessing import *
import mmap
import os
import struct

class circle:
    def __init__(self, buf):
        self.size = len(buf)
        self.buf = buf

    def _normalize(self, i, j):
        if i > self.size:
            i -= self.size
        if j > self.size:
            j -= self.size
        return i,j

    def __len__(self):
        return self.size

    def __getslice__(self, i, j):
        i, j = self._normalize(i,j)
        if i <= j:
            return self.buf[i:j]
        else:
            return self.buf[i:] + self.buf[:j]

    def __setslice__(self, i, j, y):
        i, j = self._normalize(i,j)
        if i <= j:
            self.buf[i:j] = y
        else:
            self.buf[i:] = y[:-j]
            self.buf[:j] = y[-j:]

class SharedDict:
    def __init__(self, size):
        self.size = size / 4096 * 4096
        self._map = circle(mmap.mmap(-1, self.size))
        self._meta = mmap.mmap(-1, 12)
        self._meta[:8] = struct.pack("II", 0, 0)
        self._lock = Lock()

    def _load(self):
        self._lock.acquire()
        self.start, self.length = struct.unpack("II", self._meta[:8])

    def _save(self):
        self._meta[:8] = struct.pack("II", self.start % self.size, self.length)
        self._lock.release()

    def _free(self):
        return self.size - self.length
            
    def _pop(self):
        klen,vlen = struct.unpack("II", self._map[self.start:self.start+8])
        self.start += 16 + klen + vlen
        self.length -= 16 + klen + vlen

    def put(self, key, value):
        vsize = 16 + len(key) + len(value)
        if vsize > self.size:
            return False
        
        self._load()
        while self._free() < vsize:
            self._pop()

        p = (self.start + self.length) % self.size
        self._map[p:p+8] = struct.pack("II", len(key), len(value))
        p += 8
        self._map[p:p+len(value)] = value
        p += len(value)
        self._map[p:p+8+len(key)] = key + struct.pack("II", len(key), len(value))
        
        self.length += vsize
        self._save() 
        return True

    def get(self, key):
        self._load()
        p = self.start + self.length
        left = self.length
        v = None
        while left > 16:
            #print p, self.size, left
            klen,vlen = struct.unpack("II", self._map[p-8:p])
            p -= 8+klen
            if self._map[p:p+klen] == key:
                v = self._map[p-vlen:p]
                break
            p -= vlen + 8
            left -= 16 + klen + vlen
        self._lock.release()
        return v

    def clear(self):
        self._lock.acquire()
        self._meta[:8] = struct.pack("II", 0, 0)
        self._lock.release()
        
class SharedDicts:
    def __init__(self, size, slots=8):
        size *= 1024 * 1024
        self.ds = [SharedDict(size/slots) for i in range(slots)]
        self.slots = slots

    def put(self, key, value):
        return self.ds[hash(key) % self.slots].put(key, value)

    def get(self, key):
        return self.ds[hash(key) % self.slots].get(key)

    def clear(self):
        [d.clear() for d in self.ds]


if __name__ == '__main__':
    r = range(5)
    c = circle(range(5))
    assert c[1:3] == r[1:3]
    assert c[-3:-1] == c[-3:-1] 
    assert c[-1:1] == [4,0]
    assert c[3:6] == [3,4,0]
    c[1:3] = [1,2]
    c[-3:-1] = [2,3]
    c[-1:1] = [4,0]
    c[3:6] = [3,4,0]
    assert c[:] == r

    dd = SharedDicts(1024*1024*512)
    dd.put('a','a')
    dd.put('c','c')
    assert dd.get('a') == 'a'
    assert dd.get('c') == 'c'
    data = '0'*1024*1024
    def f():
        for i in range(600):
            n = str(os.getpid()*1000 + i % 1000)
            dd.put(n, data)
            dd.get(n)
    f()
    p1 = Process(target=f)
    p2 = Process(target=f)
    p1.start()
    p2.start()
    p1.join()
    p2.join()

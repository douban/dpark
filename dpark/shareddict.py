from multiprocessing import Lock, Process
import mmap
import os, sys, time
import struct
import marshal
import cPickle


class circle:
    def __init__(self, buf):
        self.size = len(buf)
        self.buf = buf

    def _normalize(self, i, j):
        if j == sys.maxint:
            j = self.size
        while i < 0:
            i += self.size
        while i > self.size:
            i -= self.size
        while j < 0:
            j += self.size
        while j > self.size:
            j -= self.size
        return i,j

    def __len__(self):
        return self.size

    def __getslice__(self, i, j):
        i, j = self._normalize(i,j)
        if i < j:
            return self.buf[i:j]
        else:
            return self.buf[i:] + self.buf[:j]

    def __setslice__(self, i, j, y):
        i, j = self._normalize(i,j)
        if i < j:
            self.buf[i:j] = y
        else:
            self.buf[i:] = y[:-j]
            self.buf[:j] = y[-j:]

class SharedDict:
    def __init__(self, size):
        self.size = size / 4096 * 4096
        self._map = circle(mmap.mmap(-1, self.size))
        self._meta = mmap.mmap(-1, 8)
        self._meta[:8] = struct.pack("II", 0, 0)
        self._lock = Lock()

    def _load(self):
        self._lock.acquire()
        self.start, self.length = struct.unpack("II", self._meta)

    def _save(self):
        self._meta[:8] = struct.pack("II", self.start % self.size, self.length)
        self._lock.release()

    def _free(self):
        return self.size - self.length
            
    def _pop(self):
        klen,flag,vlen = struct.unpack("HHI", self._map[self.start:self.start+8])
        self.start += 16 + klen + vlen
        self.length -= 16 + klen + vlen

    def put(self, key, value, flag):
        vsize = 16 + len(key) + len(value)
        if vsize > self.size:
            return False
        
        self._load()
        while self._free() < vsize:
            self._pop()

        p = (self.start + self.length) % self.size
        self._map[p:p+8] = struct.pack("HHI", len(key), flag, len(value))
        p += 8
        self._map[p:p+len(value)] = value
        p += len(value)
        self._map[p:p+8+len(key)] = key + struct.pack("HHI", len(key), flag, len(value))
        
        self.length += vsize
        self._save() 
        return True

    def get(self, key):
        self._load()
        p = self.start + self.length
        left = self.length
        v = (None, None)
        while left > 16:
            klen,flag,vlen = struct.unpack("HHI", self._map[p-8:p])
            p -= 8+klen
            if self._map[p:p+klen] == key:
                v = flag, self._map[p-vlen:p]
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
    def __init__(self, size, slots=16):
        size <<= 20
        self.ds = [SharedDict(size/slots) for i in range(slots)]
        self.slots = slots

    def put(self, key, value):
        if isinstance(value, basestring):
            flag, v = 0, value
        else:
            try:
                flag, v = 1, marshal.dumps(value)
            except ValueError:
                try:
                    flag, v = 2, cPickle.dumps(value, -1)
                except Exception, e:
                    return

        return self.ds[hash(key) % self.slots].put(key, v, flag)

    def get(self, key):
        flag, v = self.ds[hash(key) % self.slots].get(key)
        if flag is None:
            return None
        elif flag == 0:
            return v
        elif flag == 1:
            return marshal.loads(v)
        elif flag == 2:
            return cPickle.loads(v)
        raise ValueError("invalid flag")

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
    print c[:]
    assert c[:] == r

    dd = SharedDicts(512, 16)
    dd.put('a','a')
    dd.put('c','c')
    assert dd.get('a') == 'a'
    assert dd.get('c') == 'c'
    data = '0'*1024*202
    def f():
        t = time.time()
        for i in range(600):
            n = str(os.getpid()*1000 + i % 1000)
            assert dd.put(n, data) == True
        for i in range(600):
            n = str(os.getpid()*1000 + i % 1000)
            assert len(dd.get(n)) == len(data)
        print 'used', time.time() - t
    f()
    ps = [Process(target=f) for i in range(4)]
    [p.start() for p in ps]
    [p.join() for p in ps]

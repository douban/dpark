from operator import add
import copy

from dpark.serialize import load_func, dump_func

class AccumulatorParam:
    def __init__(self, zero, addInPlace):
        self.zero = zero
        self.addInPlace = addInPlace

    def __getstate__(self):
        return dump_func(self.addInPlace), self.zero

    def __setstate__(self, state):
        add, self.zero = state
        self.addInPlace = load_func(add)

numAcc = AccumulatorParam(0, add)
listAcc = AccumulatorParam([], lambda x,y:x.extend(y) or x)
mapAcc = AccumulatorParam({}, lambda x,y:x.update(y) or x)
setAcc = AccumulatorParam(set(), lambda x,y:x.update(y) or x)


class Accumulator:
    def __init__(self, initialValue=0, param=numAcc):
        self.id = self.newId()
        if param is None:
            param = numAcc
        self.param = param
        self.value = initialValue
        self.register(self, True)

    def add(self, v):
        self.value = self.param.addInPlace(self.value, v)
        self.register(self, False)

    def reset(self):
        v = self.value
        self.value = copy.copy(self.param.zero)
        return v

    def __getstate__(self):
        return self.id, self.param

    def __setstate__(self, s):
        self.id, self.param = s
        self.value = copy.copy(self.param.zero)
        self.register(self, False)

    nextId = 0
    @classmethod
    def newId(cls):
        cls.nextId += 1
        return cls.nextId

    originals = {}
    localAccums = {}
    @classmethod
    def register(cls, acc, original):
        if original:
            cls.originals[acc.id] = acc
        else:
            cls.localAccums[acc.id] = acc

    @classmethod
    def clear(cls):
        for acc in cls.localAccums.values():
            acc.reset()
        cls.localAccums.clear()

    @classmethod
    def values(cls):
        v = dict((id, accum.value) for id,accum in cls.localAccums.items())
        cls.clear()
        return v

    @classmethod
    def merge(cls, values):
        for id, value in values.items():
            cls.originals[id].add(value)

ReadBytes = Accumulator()
WriteBytes = Accumulator()

RemoteReadBytes = Accumulator()
LocalReadBytes = Accumulator()

CacheHits = Accumulator()
CacheMisses = Accumulator()

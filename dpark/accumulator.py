from serialize import load_func, dump_func
from operator import add

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

    def reset(self):
        self.value = self.param.zero

    def __getstate(self):
        return self.id, self.param 

    def __setstate__(self, s):
        self.id, self.param = s
        self.value = self.param.zero
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
        cls.localAccums.clear()

    @classmethod
    def values(cls):
        return dict((id, accum.value) for id,accum in cls.localAccums.items())

    @classmethod
    def merge(cls, values):
        for id, value in values.items():
            cls.originals[id].add(value)

ReadBytes = Accumulator()
WriteBytes = Accumulator()
CacheHits = Accumulator()
CacheMisses = Accumulator()

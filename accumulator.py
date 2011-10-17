import logging
from threading import currentThread
from utils import load_func, dump_func


class AccumulatorParam:
    def __init__(self, zero, addInPlace):
        self.zero = zero
        self.addInPlace = addInPlace

    def __getstate__(self):
        return dump_func(self.addInPlace), self.zero

    def __setstate__(self, state):
        add, self.zero = state
        self.addInPlace = load_func(add)

intAcc = AccumulatorParam(0, lambda x,y:x+y)
listAcc = AccumulatorParam([], lambda x,y:x.extend(y) or x)
mapAcc = AccumulatorParam({}, lambda x,y:x.update(y) or x)


class Accumulator:
    def __init__(self, initialValue, param=intAcc):
        self.id = self.newId()
        if param is None:
            param = intAcc
        self.param = param
        self._value = initialValue
        self.deserialized = False
        self.register(self, True)

    def _get_value(self):
        return self._value
    def _set_value(self, v):
        self._value = v
    value = property(_get_value, _set_value)
  
    def add(self, v):
        self._value = self.param.addInPlace(self._value, v)
        self.register(self, not self.deserialized)

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.deserialized = True
        self._value = self.param.zero

    nextId = 0
    originals = {}
    localAccums = {}
    @classmethod
    def newId(cls):
        cls.nextId += 1
        return cls.nextId

    @classmethod
    def register(cls, acc, original):
        if original:
            cls.originals[acc.id] = acc
        else:
            accums = cls.localAccums.setdefault(currentThread(), {})
            accums[acc.id] = acc

    @classmethod
    def clear(cls):
        if currentThread() in cls.localAccums:
            del cls.localAccums[currentThread()]

    @classmethod
    def values(cls):
        accums = cls.localAccums.get(currentThread(), {})
        return dict((id, accum.value) for id,accum in accums.items())

    @classmethod
    def merge(cls, values):
        for id, value in values.items():
            if id in cls.originals:
                cls.originals[id].add(value)

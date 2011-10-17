from threading import currentThread

class Accumulator:
    def __init__(self, initialValue, param=None):
        self.id = self.newId()
        if param is None:
            param = intAcc
        self.param = param
        self._value = initialValue
        self.zero = param.zero(initialValue)
        self.deserialized = False

        self.register(self, True)

    def _get_value(self):
        return self._value
    def _set_value(self, v):
        self._value = v
    value = property(_get_value, _set_value)
  
    def add(self, v):
        self._value = self.param.addInPlace(self._value, v)

    def __str__(self):
        return str(self._value)

    def __setstate(self, d):
        self.__dict__.update(d)
        self.deserialized = True
        self._value = self.zero
    #TODO when unpickle

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
            # TODO
            accums[acc.id] = acc

    @classmethod
    def clear(cls):
        if currentThread() in cls.localAccums:
            del cls.localAccums[currentThread()]

    @classmethod
    def values(cls):
        accums = cls.localAccums.get(currentThread().name, {})
        return dict((id, accum.value) for id,accum in accums.items())

    @classmethod
    def update(cls, values):
        for id, value in values.items():
            if id in cls.originals:
                cls.originals[id].extend(value)
         

class AccumulatorParam:
    def __init__(self, addInPlace, zero):
        self.addInPlace = addInPlace
        self.zero = zero

    def __getstate__(self):
        return dump_func(self.addInPlace), dump_func(self.zero)

    def __setstate__(self, state):
        add, zero = state
        self.addInPlace = load_func(add, globals())
        self.zero = load_func(zero, globals())

intAcc = AccumulatorParam(lambda x,y:x+y, lambda x:0)
listAcc = AccumulatorParam(lambda x,y:x+[y], lambda x: list(x))


from threading import currentThread

class Accumulator:
    def __init__(self, initialValue, param):
        self.id = self.newId()
        self._value = initialValue
        self.zero = param.zero(initialValue)
        self.deserialized = False

        self.register(self, True)

    def _get_value(self):
        return self._value
    def _set_value(self, v):
        self._value = v
    value = property(_get_value, _set_value)
   
    def __str__(self):
        return str(self._value)

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
        #del cls.localAccums[currentThread()]
        pass

    @classmethod
    def values(cls):
        accums = cls.localAccums.get(currentThread().name, {})
        return dict((id, accum.value) for id,accum in accums.items())

    @classmethod
    def add(cls, values):
        for id, value in values.items():
            if id in cls.originals:
                cls.originals[id].extend(value)
         

class AccumulatorParam:
    def addInPlace(t1, t2):
        pass
    def zero(v):
        pass



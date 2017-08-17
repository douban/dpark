from libc.stdint cimport int64_t

cdef int64_t tuple_hash(tuple obj):
    cdef int64_t mul = 1000003, l = len(obj), value = 0x345678, v
    for i in obj:
        l -= 1
        v = portable_hash(i)
        if v == -1:
            return -1
        value = (value ^ v) * mul
        mul += <int64_t> (82520 + l * 2)
    value += 97531
    if value == -1:
        value = -2
    return value

cdef int64_t string_hash(bytes obj_):
    cdef int64_t l = len(obj_), value, i = 0, v
    cdef char* obj = obj_
    if l == 0:
        return 0
    v = obj[0]
    value = v << 7
    while i < l:
        v = obj[i]
        value = (1000003 * value) ^ v
        i += 1

    value ^= l
    if value == -1:
        value = -2
    return value

cdef int64_t unicode_hash(obj):
    cdef unicode s = obj
    cdef int64_t l = len(obj), value, v
    if l == 0:
        return 0
    v = ord(s[0])
    value = v << 7
    for c in s:
        v = ord(c)
        value = (1000003 * value) ^ v

    value ^= l
    if value == -1:
        value = -2
    return value


cpdef int64_t portable_hash(obj) except -1:
    t = type(obj)
    if obj is None:
        return 1315925605
    if t is bytes:
        return string_hash(obj)
    elif t is unicode:
        return unicode_hash(obj)
    elif t is tuple:
        return tuple_hash(obj)
    elif t is int or t is long or t is float:
        return hash(obj)
    else:
        try:
            import numpy as np
            if isinstance(obj, np.number):
                return hash(obj)
        except ImportError:
            pass
        raise TypeError('%s is unhashable by portable_hash' % t)

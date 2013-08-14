# -*- coding: utf-8 -*-
class LazyJIT(object):
    this = None
    def __init__(self, f, signature, kwargs):
        self.f = f
        self.signature = signature
        self.kwargs = kwargs

    def __call__(self, *args, **kwargs):
        if self.this is None:
            try:
                from numba import jit
                self.this = jit(self.signature, **self.kwargs)(self.f)
            except ImportError, e:
                self.this = self.f
        return getattr(self.this, '__call__')(*args, **kwargs)


def jit(signature, **kwargs):
    if not isinstance(signature, (str, unicode)):
        raise ValueError('First argument should be signature')
    def _(f):
        return LazyJIT(f, signature, kwargs)
    return _

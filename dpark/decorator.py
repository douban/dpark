# -*- coding: utf-8 -*-
class LazyJIT(object):
    this = None
    def __init__(self, decorator, f, *args, **kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs
        self.decorator = decorator

    def __call__(self, *args, **kwargs):
        if self.this is None:
            try:
                mod = __import__('numba', fromlist=[self.decorator])
                d = getattr(mod, self.decorator)
                self.this = d(*self.args, **self.kwargs)(self.f)
            except ImportError, e:
                self.this = self.f
        return getattr(self.this, '__call__')(*args, **kwargs)


def jit(*args, **kwargs):
    if len(args) ==1 and not kwargs and callable(args[0]):
        f = args[0]
        return LazyJIT('jit', f)
    else:
        def _(f):
            return LazyJIT('jit', f, *args, **kwargs)
        return _

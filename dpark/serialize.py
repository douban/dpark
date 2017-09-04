from __future__ import absolute_import
from __future__ import print_function
import sys
import types
try:
    from cStringIO import StringIO
except ImportError:
    from six import BytesIO as StringIO

import marshal
import types
import six.moves.cPickle
import itertools
if six.PY2:
    from pickle import Pickler
else:
    from pickle import _Pickler as Pickler
from pickle import whichmodule, PROTO, STOP
from collections import deque
from functools import partial

from dpark.util import get_logger
import six
from six.moves import range
from six.moves import copyreg
from six import int2byte
logger = get_logger(__name__)


class LazySave(object):
    '''Out of band marker for lazy saves among lazy writes.'''

    __slots__ = ['obj']

    def __init__(self, obj):
        self.obj = obj

    def __repr__(self):
        return '<LazySave %s>' % repr(self.obj)

class LazyMemo(object):
    '''Out of band marker for lazy memos among lazy writes.'''

    __slots__ = ['obj']

    def __init__( self, obj ):
        self.obj = obj

    def __repr__( self ):
        return '<LazyMemo %s>' % repr(self.obj)

class MyPickler(Pickler):
    def __init__(self, file, protocol=None):
        Pickler.__init__(self, file, protocol)
        self.lazywrites = deque()
        self.realwrite = file.write

        # Pickler.__init__ overwrites self.write, we do not want that
        del self.write

    def write(self, *args):
        if self.lazywrites:
            self.lazywrites.append(args)
        else:
            self.realwrite(*args)

    def save(self, obj):
        self.lazywrites.append(LazySave(obj))

    realsave = Pickler.save

    def lazymemoize( self, obj ):
        """Store an object in the memo."""
        if self.lazywrites:
            self.lazywrites.append(LazyMemo(obj))
        else:
            self.realmemoize(obj)

    memoize = lazymemoize

    realmemoize = Pickler.memoize

    def dump(self, obj):
        """Write a pickled representation of obj to the open file."""
        if self.proto >= 2:
            self.write(PROTO + int2byte(self.proto))
        self.realsave(obj)
        queues = deque([self.lazywrites])
        while queues:
            lws = queues[0]
            self.lazywrites = deque()
            while lws:
                lw = lws.popleft()
                if isinstance(lw, LazySave):
                    self.realsave(lw.obj)
                    if self.lazywrites:
                        queues.appendleft(self.lazywrites)
                        break
                elif isinstance(lw, LazyMemo):
                    self.realmemoize(lw.obj)
                else:
                    self.realwrite(*lw)
            else:
                queues.popleft()

        self.realwrite(STOP)

    dispatch = Pickler.dispatch.copy()

    @classmethod
    def register(cls, type, reduce):
        def dispatcher(self, obj):
            rv = reduce(obj)
            if isinstance(rv, str):
                self.save_global(obj, rv)
            else:
                self.save_reduce(obj=obj, *rv)
        cls.dispatch[type] = dispatcher


def dumps(o):
    io = StringIO()
    MyPickler(io, -1).dump(o)
    return io.getvalue()


def loads(s):
    return six.moves.cPickle.loads(s)

dump_func = dumps
load_func = loads


def reduce_module(mod):
    return load_module, (mod.__name__, )


def load_module(name):
    __import__(name)
    return sys.modules[name]

MyPickler.register(types.ModuleType, reduce_module)


class RecursiveFunctionPlaceholder(object):
    """
    Placeholder for a recursive reference to the current function,
    to avoid infinite recursion when serializing recursive functions.
    """

    def __eq__(self, other):
        return isinstance(other, RecursiveFunctionPlaceholder)

RECURSIVE_FUNCTION_PLACEHOLDER = RecursiveFunctionPlaceholder()


def marshalable(o):
    if o is None:
        return True
    t = type(o)
    if t in (six.binary_type, six.text_type, bool, int, int, float, complex):
        return True
    if t in (tuple, list, set):
        for i in itertools.islice(o, 100):
            if not marshalable(i):
                return False
        return True
    if t == dict:
        for k, v in itertools.islice(six.iteritems(o), 100):
            if not marshalable(k) or not marshalable(v):
                return False
        return True
    return False

OBJECT_SIZE_LIMIT = 100 << 10


def create_broadcast(name, obj, func_name):
    import dpark
    logger.info("use broadcast for object %s %s (used in function %s)",
                name, type(obj), func_name)
    return dpark._ctx.broadcast(obj)


def dump_obj(f, name, obj):
    if obj is f:
        # Prevent infinite recursion when dumping a recursive function
        return dumps(RECURSIVE_FUNCTION_PLACEHOLDER)

    try:
        if sys.getsizeof(obj) > OBJECT_SIZE_LIMIT:
            obj = create_broadcast(name, obj, f.__name__)
    except TypeError:
        pass

    b = dumps(obj)
    if len(b) > OBJECT_SIZE_LIMIT:
        b = dumps(create_broadcast(name, obj, f.__name__))
    if len(b) > OBJECT_SIZE_LIMIT:
        logger.warning("broadcast of %s obj too large", type(obj))
    return b


def get_co_names(code):
    co_names = code.co_names
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            co_names += get_co_names(const)

    return co_names


def dump_closure(f, skip=set()):
    def _do_dump(f):
        for i, c in enumerate(f.__closure__):
            try:
                if hasattr(c, 'cell_contents'):
                    yield dump_obj(f, 'cell%d' % i, c.cell_contents)
                else:
                    yield None
            except ValueError:
                yield None

    code = f.__code__
    glob = {}
    for n in get_co_names(code):
        r = f.__globals__.get(n)
        if r is not None and n not in skip:
            glob[n] = dump_obj(f, n, r)

    closure = None
    if f.__closure__:
        closure = tuple(_do_dump(f))
    return marshal.dumps(
        (code, glob, f.__name__, f.__defaults__, closure, f.__module__))


def load_closure(bytes):
    code, glob, name, defaults, closure, mod = marshal.loads(bytes)
    glob = dict((k, loads(v)) for k, v in glob.items())
    glob['__builtins__'] = __builtins__
    closure = closure and reconstruct_closure(closure) or None
    f = types.FunctionType(code, glob, name, defaults, closure)
    f.__module__ = mod
    # Replace the recursive function placeholders with this simulated function
    # pointer
    for key, value in glob.items():
        if RECURSIVE_FUNCTION_PLACEHOLDER == value:
            f.__globals__[key] = f
    return f


def make_cell(value):
    return (lambda: value).__closure__[0]


def make_empty_cell():
    if False:
        unreachable = None
    return (lambda: unreachable).__closure__[0]


def reconstruct_closure(closure):
    return tuple(
        [make_cell(loads(v)) if v is not None
         else make_empty_cell() for v in closure])


def get_global_function(module, name):
    __import__(module)
    mod = sys.modules[module]
    return getattr(mod, name)


def reduce_function(obj):
    name = obj.__name__
    if not name or name == '<lambda>':
        return load_closure, (dump_closure(obj),)

    module = getattr(obj, "__module__", None)
    if module is None:
        module = whichmodule(obj, name)

    if module == '__main__' and \
       name not in ('load_closure', 'load_module',
                    'load_method', 'load_local_class'):  # fix for test
        return load_closure, (dump_closure(obj),)

    try:
        f = get_global_function(module, name)
    except (ImportError, KeyError, AttributeError):
        return load_closure, (dump_closure(obj),)
    else:
        if f is not obj:
            return load_closure, (dump_closure(obj),)
        return name

classes_dumping = set()
internal_fields = {
    '__weakref__': False,
    '__dict__': False,
    '__doc__': True,
    '__slots__': True,
}

member_descripter_types = (
    types.MemberDescriptorType,
    type(LazySave.obj)
)

def dump_local_class(cls):
    name = cls.__name__
    if cls in classes_dumping:
        return dumps(name)

    classes_dumping.add(cls)
    internal = {}
    external = {}
    keys = list(cls.__dict__.keys())
    for k in keys:
        if k not in internal_fields:
            v = cls.__dict__[k]
            if isinstance(v, property):
                k = ('property', k)
                v = (v.fget, v.fset, v.fdel, v.__doc__)

            if isinstance(v, staticmethod):
                k = ('staticmethod', k)
                v = dump_closure(v.__func__, skip=set(keys))

            if isinstance(v, classmethod):
                k = ('classmethod', k)
                v = dump_closure(v.__func__, skip=set(keys))

            if isinstance(v, types.FunctionType):
                k = ('method', k)
                v = dump_closure(v, skip=set(keys))

            if not isinstance(v, member_descripter_types):
                external[k] = v

        elif internal_fields[k]:
            internal[k] = getattr(cls, k)

    result = dumps((cls.__name__, cls.__bases__, internal, dumps(external)))
    if cls in classes_dumping:
        classes_dumping.remove(cls)

    return result

classes_loaded = {}


def load_local_class(bytes):
    t = loads(bytes)
    if not isinstance(t, tuple):
        return classes_loaded[t]

    name, bases, internal, external = t
    if name in classes_loaded:
        return classes_loaded[name]

    if any(isinstance(base, type) for base in bases):
        cls = type(name, bases, internal)
    else:
        assert six.PY2
        cls = types.ClassType(name, bases, internal)

    classes_loaded[name] = cls
    external = loads(external)
    for k, v in external.items():
        if isinstance(k, tuple):
            t, k = k
            if t == 'property':
                fget, fset, fdel, doc = v
                v = property(fget, fset, fdel, doc)

            if t == 'staticmethod':
                v = load_closure(v)
                v = staticmethod(v)

            if t == 'classmethod':
                v = load_closure(v)
                v = classmethod(v)

            if t == 'method':
                v = load_closure(v)

        setattr(cls, k, v)

    return cls


def reduce_class(obj):
    name = obj.__name__
    module = getattr(obj, "__module__", None)
    if module == '__main__' and name not in (
            'MyPickler', 'RecursiveFunctionPlaceholder'):
        result = load_local_class, (dump_local_class(obj),)
        return result

    return name


def dump_method(method):
    obj = method.__self__ or method.__self__.__class__
    func = method.__func__

    return dumps((obj, func.__name__))


def load_method(bytes):
    _self, func_name = loads(bytes)
    return getattr(_self, func_name)


def reduce_method(method):
    module = method.__func__.__module__
    return load_method, (dump_method(method), )

MyPickler.register(types.LambdaType, reduce_function)
if six.PY2:
    MyPickler.register(types.ClassType, reduce_class)

MyPickler.register(type, reduce_class)
MyPickler.register(types.MethodType, reduce_method)

if __name__ == "__main__":
    assert marshalable(None)
    assert marshalable("")
    assert marshalable(u"")
    assert not marshalable(memoryview(b""))
    assert marshalable(0)
    assert marshalable(0)
    assert marshalable(0.0)
    assert marshalable(True)
    assert marshalable(complex(1, 1))
    assert marshalable((1, 1))
    assert marshalable([1, 1])
    assert marshalable(set([1, 1]))
    assert marshalable({1: None})

    some_global = 'some global'

    def glob_func(s):
        return "glob:" + s

    def get_closure(x):
        glob_func(some_global)
        last = " last"

        def foo(y): return "foo: " + y

        def the_closure(a, b=1):
            marshal.dumps(a)
            return (a * x + int(b), glob_func(foo(some_global) + last))
        return the_closure

    f = get_closure(10)
    ff = loads(dumps(f))
    # print globals()
    print(f(2))
    print(ff(2))
    glob_func = loads(dumps(glob_func))
    get_closure = loads(dumps(get_closure))

    # Test recursive functions
    def fib(n): return n if n <= 1 else fib(n - 1) + fib(n - 2)
    assert fib(8) == loads(dumps(fib))(8)

    class Foo1:

        def foo(self):
            return 1234

    class Foo2(object):

        def foo(self):
            return 5678

    class Foo3(Foo2):
        x = 1111

        def foo(self):
            return super(Foo3, self).foo() + Foo3.x

    class Foo4(object):

        @classmethod
        def x(cls):
            return 1

        @property
        def y(self):
            return 2

        @staticmethod
        def z():
            return 3

        def recursive(self, x):
            if x <= 0:
                return x
            else:
                return self.recursive(x - 1)

    df1 = dumps(Foo1)
    df2 = dumps(Foo2)
    df3 = dumps(Foo3)
    df4 = dumps(Foo4)

    del Foo1
    del Foo2
    del Foo3
    del Foo4

    Foo1 = loads(df1)
    Foo2 = loads(df2)
    Foo3 = loads(df3)
    Foo4 = loads(df4)

    f1 = Foo1()
    f2 = Foo2()
    f3 = Foo3()
    f4 = Foo4()

    assert f1.foo() == 1234
    assert f2.foo() == 5678
    assert f3.foo() == 5678 + 1111
    assert Foo4.x() == 1

    recursive = Foo4().recursive
    _recursive = loads(dumps(recursive))
    assert _recursive(5) == 0

    f = loads(dumps(lambda: (some_global for i in range(1))))
    print(list(f()))
    assert list(f()) == [some_global]

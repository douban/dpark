import sys, types
from cStringIO import StringIO
import marshal, new, cPickle
import itertools
from pickle import Pickler, whichmodule
import logging

logger = logging.getLogger(__name__)

class MyPickler(Pickler):
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
    return cPickle.loads(s)

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
    if o is None: return True
    t = type(o)
    if t in (str, unicode, bool, int, long, float, complex):
        return True
    if t in (tuple, list, set):
        for i in itertools.islice(o, 100):
            if not marshalable(i):
                return False
        return True
    if t == dict:
        for k,v in itertools.islice(o.iteritems(), 100):
            if not marshalable(k) or not marshalable(v):
                return False
        return True
    return False

OBJECT_SIZE_LIMIT = 100 << 10

_cache = {}

def create_broadcast(name, obj, func_name):
    key = (name, id(object), func_name)
    if key in _cache:
        return _cache[key]

    import dpark
    logger.info("use broadcast for object %s %s (used in function %s)", 
        name, type(obj), func_name)
    b = dpark._ctx.broadcast(obj)
    _cache[key] = b
    return b

def dump_closure(f):
    code = f.func_code
    glob = {}
    for n in code.co_names:
        r = f.func_globals.get(n)
        if r is not None:
            if r is f:
                # Prevent infinite recursion when dumping a recursive function
                glob[n] = dumps(RECURSIVE_FUNCTION_PLACEHOLDER)
            else:
                if sys.getsizeof(r) > OBJECT_SIZE_LIMIT:
                    r = create_broadcast(n, r, f.__name__)
                b = dumps(r)
                if len(b) > OBJECT_SIZE_LIMIT:
                    b = dumps(create_broadcast(n, r, f.__name__))
                glob[n] = b

    closure = f.func_closure and tuple(dumps(c.cell_contents) for c in f.func_closure) or None
    return marshal.dumps((code, glob, f.func_name, f.func_defaults, closure))

def load_closure(bytes):
    code, glob, name, defaults, closure = marshal.loads(bytes)
    glob = dict((k, loads(v)) for k,v in glob.items())
    glob['__builtins__'] = __builtins__
    closure = closure and reconstruct_closure([loads(c) for c in closure]) or None
    f = new.function(code, glob, name, defaults, closure)
    # Replace the recursive function placeholders with this simulated function pointer
    for key, value in glob.items():
        if RECURSIVE_FUNCTION_PLACEHOLDER == value:
            f.func_globals[key] = f
    return f

def reconstruct_closure(values):
    args = ','.join("_%d" % i for i in range(len(values)))
    src = "f = lambda %s: lambda : (%s)" % (args, args)
    try:
        exec src
    except Exception:
        raise SyntaxError(src)
    closure = f(*reversed(values)).func_closure
    # the cell order of closure in prebuilded Python-2.7 of Mac is different
    if closure[0].cell_contents is not values[0]:
        return f(*values).func_closure
    return closure

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

    if module == '__main__' and name != 'load_closure': # fix for test
        return load_closure, (dump_closure(obj),)

    try:
        f = get_global_function(module, name)
    except (ImportError, KeyError, AttributeError):
        return load_closure, (dump_closure(obj),)
    else:
        if f is not obj:
            return load_closure, (dump_closure(obj),)
        return name

MyPickler.register(types.LambdaType, reduce_function)


if __name__ == "__main__":
    assert marshalable(None)
    assert marshalable("")
    assert marshalable(u"")
    assert not marshalable(buffer(""))
    assert marshalable(0)
    assert marshalable(0L)
    assert marshalable(0.0)
    assert marshalable(True)
    assert marshalable(complex(1,1))
    assert marshalable((1,1))
    assert marshalable([1,1])
    assert marshalable(set([1,1]))
    assert marshalable({1:None})

    some_global = 'some global'
    def glob_func(s):
        return "glob:" + s
    def get_closure(x):
        glob_func(some_global)
        last = " last"
        def foo(y): return "foo: " + y
        def the_closure(a, b=1):
            marshal.dumps(a)
            return (a * x + int(b), glob_func(foo(some_global)+last))
        return the_closure

    f = get_closure(10)
    ff = loads(dumps(f))
    #print globals()
    print f(2)
    print ff(2)
    glob_func = loads(dumps(glob_func))
    get_closure = loads(dumps(get_closure))

    # Test recursive functions
    def fib(n): return n if n <= 1 else fib(n-1) + fib(n-2)
    assert fib(8) == loads(dumps(fib))(8)

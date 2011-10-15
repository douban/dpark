import marshal, new, pickle

def dump_object(o):
    try:
        return 3, pickle.dumps(o)
    except Exception:
        if isinstance(o, new.function):
            return 1, dump_func(o)
        if type(o) == type(marshal):
            return 2, o.__name__
        else:
            print 'error', o
            raise

def load_object((t, d), globals):
    if t == 0:
        return d
    elif t == 1:
        return load_func(d, globals)
    elif t == 2:
        return __import__(d)
    else:
        return pickle.loads(d)

def dump_func(f):
    code = f.func_code
    #glob = dict((n, dump_object(f.func_globals[n])) for n in code.co_names if n in f.func_globals)
    #marshal.dumps(glob)
    closure = f.func_closure and tuple(dump_object(c.cell_contents) for c in f.func_closure) or None 
    #print "closure", closure
    return marshal.dumps((code, {}, f.func_name, f.func_defaults, closure))

def load_func(bytes, globals):
    code, glob, name, defaults, closure = marshal.loads(bytes)
    #glob = dict((k, load_object(v)) for k,v in glob.items())
    closure = closure and reconstruct_closure([load_object(c, globals) for c in closure]) or None
    #print 'closure2', closure
    #globals().update(glob)
    glob['__builtins__'] = __builtins__
    return new.function(code, globals, name, defaults, closure)

def reconstruct_closure(values):
    #print "values", values
    ns = range(len(values))
    src = ["def f(arg):"]
    src += [" _%d = arg[%d]" % (n, n) for n in ns]
    src += [" return lambda:(%s)" % ','.join("_%d"%n for n in ns), '']
    src = '\n'.join(src)
    try:
        exec src
    except:
        raise SyntaxError(src)
    values.reverse()
    return f(values).func_closure

if __name__ == "__main__":
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

#    glob_func = load_func(dump_func(glob_func))
#    get_closure = load_func(dump_func(get_closure))
    f = get_closure(10)
    ff = load_func(dump_func(f), globals())
    #print globals()
    print f(2)
    print ff(2)

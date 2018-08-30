import os
import inspect
from collections import defaultdict


def get_path(p):
    return os.path.realpath(os.path.abspath(p))


src_dir = os.path.dirname(os.path.dirname(get_path(__file__)))


class Frame(object):

    def __init__(self, f):
        """working in func_name, exec code at pos"""
        self.path = get_path(f.f_code.co_filename)
        self.lineno = f.f_lineno
        self.lasti = f.f_lasti
        self.func_name = f.f_code.co_name

    @property
    def pos(self):
        return self.path, self.lineno, self.lasti


class Scope(object):

    scopes = {}
    calls_in_oneline = defaultdict(dict)  # (path, line_no, fname) -> [lasti...]
    gid = 0

    def __init__(self, caller, call_site, dpark_func_name):
        self.id = Scope.gid
        Scope.gid += 1
        self.caller = caller
        self.call_site = call_site
        self.dpark_func_name = dpark_func_name

    @classmethod
    def get(cls):
        callee = inspect.currentframe()
        caller = callee.f_back

        while src_dir == os.path.dirname(get_path(caller.f_code.co_filename)):
            callee = caller
            caller = caller.f_back

        caller = Frame(caller)  # the first callsite out of dpark package, where user call dpark api
        callee = Frame(callee)  # the dpark api called by user, DparkContext.xxx() or RDD.xxx()

        # deal with usage like  "rdd.map(_).map(_)"
        # distinguish same dpark api called in one line by lasti
        # to be comprehensible, replace lasti with order or appearance of same api in this line , starts with 0
        key = caller.path, caller.lineno, callee.func_name
        calls = cls.calls_in_oneline.setdefault(key, [])
        i = -1
        for i, lasti in enumerate(calls):
            if lasti == caller.lasti:
                seq = i
                break
        else:
            seq = i + 1
            calls.append(caller.lasti)

        dpark_func_name = callee.func_name
        call_site = "{}:{}@{}:{}".format(callee.func_name, seq, caller.path, caller.lineno)
        scope = cls.scopes.get(call_site)
        if scope is None:
            scope = Scope(caller, call_site, dpark_func_name)
            cls.scopes[call_site] = scope
        return scope

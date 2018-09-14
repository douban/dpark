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


def frame_tuple(f):
    return f.f_code.co_filename, f.f_lineno,  f.f_lasti


class Scope(object):

    scopes_by_id = {}
    scopes_by_stackhash = {}
    scopes_by_api_callsite_id = {}

    api_callsites = {}
    calls_in_oneline = defaultdict(dict)  # (path, line_no, fname) -> [lasti...]
    gid = 0

    def __init__(self, name, stack, stackhash, api, api_callsite):
        self.id = Scope.gid
        Scope.gid += 1
        if name is None:
            name = api
        self.name = name
        self.stack = stack
        self.stackhash = stackhash
        self.api = api
        self.api_callsite = api_callsite
        self.key = "{}@{}".format(name, self.api_callsite)
        self.api_callsite_id = self.api_callsites.get(api_callsite)
        if self.api_callsite_id is None:
            self.api_callsite_id = self.api_callsites[api_callsite] = len(self.api_callsites)
            self.scopes_by_api_callsite_id[self.api_callsite_id] = [self]
        else:
            self.scopes_by_api_callsite_id[self.api_callsite_id].append(self)
        # print(self.id, self.api_callsite_id, api_callsite)

    @classmethod
    def get_callsite(cls, caller, callee):
        """
        Deal with usage like  "rdd.map(_).map(_)", distinguish same dpark api called in one line by lasti.
        To be comprehensible, replace lasti with order of calling of same api in this line , starts with 0.
        """

        callee = Frame(callee)  # the dpark api called by user, DparkContext.xxx() or RDD.xxx()
        caller = Frame(caller)  # the first callsite out of dpark package, where user call dpark api

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

        api = callee.func_name
        api_callsite = "{}:{}@{}:{}".format(callee.func_name, seq, caller.path, caller.lineno)
        return api, api_callsite

    @classmethod
    def get(cls, name):
        callee = inspect.currentframe()
        caller = callee.f_back
        stack = []

        api_caller = None
        api_callee = None

        while caller is not None:
            stack.append(frame_tuple(caller))
            if api_callee is None and src_dir != os.path.dirname(get_path(caller.f_code.co_filename)):
                api_callee = callee  # the dpark api called by user, DparkContext.xxx() or RDD.xxx()
                api_caller = caller  # the first callsite out of dpark package, where user call dpark api
            callee = caller
            caller = caller.f_back

        stack = tuple(stack)
        stackhash = hash(stack)
        scope = cls.scopes_by_stackhash.get(stackhash)
        if scope is None:
            api, api_callsite = cls.get_callsite(api_caller, api_callee)
            scope = Scope(name, stack, stackhash, api, api_callsite)
            cls.scopes_by_stackhash[stackhash] = scope
            cls.scopes_by_id[scope.id] = scope
        return scope

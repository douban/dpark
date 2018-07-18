import os
import sys

src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STACK_FILE_NAME = 0
STACK_LINE_NUM = 1
STACK_FUNC_NAME = 2


def _get_user_call_site():
    import traceback
    stack = traceback.extract_stack(sys._getframe())
    for i in range(1, len(stack)):
        callee_path = stack[i][STACK_FILE_NAME]
        if src_dir == os.path.dirname(os.path.abspath(callee_path)):
            caller_path = stack[i - 1][STACK_FILE_NAME]
            caller_lineno = stack[i - 1][STACK_LINE_NUM]
            dpark_func_name = stack[i][STACK_FUNC_NAME]
            user_call_site = '%s:%d ' % (caller_path, caller_lineno)
            return dpark_func_name, user_call_site
    return "<func>", " <root>"


class Scope(object):
    def __init__(self):
        fn, pos = _get_user_call_site()
        self.dpark_func_name = fn
        self.call_site = "@".join([fn, pos])

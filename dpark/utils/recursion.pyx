from cpython cimport PyInt_FromLong

cdef extern from "Python.h":
    ctypedef struct PyThreadState:
        int recursion_depth
    PyThreadState *PyThreadState_GET()

cpdef int get_recursion_depth():
    cdef PyThreadState *tstate = PyThreadState_GET()
    return PyInt_FromLong(tstate.recursion_depth - 1)


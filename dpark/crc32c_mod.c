#include <Python.h>
#include <stdint.h>

uint32_t crc32c(const void *buf, size_t len, uint32_t crc);
uint32_t crc32c_sw(uint32_t crci, const void *buf, size_t len);

PyDoc_STRVAR(doc_crc32c,
"(bytes, oldcrc = 0) -> newcrc. Compute CRC-32c incrementally");

static PyObject *
crc32c_crc32c(PyObject *self, PyObject *args)
{
    Py_buffer pbin;
    unsigned char *bin_data;
    unsigned int crc = 0U;      /* initial value of CRC */
    Py_ssize_t len;
    unsigned int result;

    if ( !PyArg_ParseTuple(args, "s*|I:crc32", &pbin, &crc) )
        return NULL;

    bin_data = pbin.buf;
    len = pbin.len;

    if (__builtin_cpu_supports("sse4.2")) {
        result = crc32c(bin_data, len, crc);
    } else {
        result = crc32c_sw(crc, bin_data, len);
    }

    PyBuffer_Release(&pbin);
    return PyLong_FromLong(result);
}

static PyMethodDef crc32c_module_methods[] = {
    {"crc32c",      crc32c_crc32c,      METH_VARARGS, doc_crc32c},
    {NULL, NULL}                             /* sentinel */
};

PyDoc_STRVAR(doc_crc32c_module,
"Compute CRC32c with sse4.2 support");

#if PY_MAJOR_VERSION >= 3

static struct PyModuleDef crc32c_module_def = {
    PyModuleDef_HEAD_INIT,
    "crc32c",   /* name of module */
    doc_crc32c_module, /* module documentation, may be NULL */
    -1,       /* size of per-interpreter state of the module,
                 or -1 if the module keeps state in global variables. */
    crc32c_module_methods
};

PyObject *
PyInit_crc32c(void) {
#ifndef __clang__
    __builtin_cpu_init ();
#endif
    PyObject *module = PyModule_Create(&crc32c_module_def);
    if (module == NULL)
        return NULL;

    return module;
}

#else

void
initcrc32c(void) {
#ifndef __clang__
    __builtin_cpu_init ();
#endif
    PyObject *module, *d, *x;
    module = Py_InitModule("crc32c", crc32c_module_methods);
    if (module == NULL)
        return;

    d = PyModule_GetDict(module);
    x = PyString_FromString(doc_crc32c_module);
    PyDict_SetItemString(d, "__doc__", x);
    Py_XDECREF(x);
}

#endif

import platform

if platform.python_implementation() == 'PyPy':
    def compress(*a, **kw):
        from lz4framed import compress as _compress
        return _compress(*a, **kw)


    def decompress(*a, **kw):
        from lz4framed import decompress as _decompress
        return _decompress(*a, **kw)

else:
    from lz4framed import compress, decompress

__all__ = ['compress', 'decompress']

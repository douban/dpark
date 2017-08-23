import lz4
if hasattr(lz4, 'block'):
    # lz4 > 0.8
    compress = lz4.block.compress
    decompress = lz4.block.decompress
elif hasattr(lz4, 'FFI'):
    # lz4-cffi
    compress = lz4.compress
    decompress = lz4.uncompress
else:
    compress = lz4.compress
    decompress = lz4.decompress

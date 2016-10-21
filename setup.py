import sys
from setuptools import setup, Extension

# setuptools DWIM monkey-patch madness: http://dou.bz/37m3XL
if 'setuptools.extension' in sys.modules:
    m = sys.modules['setuptools.extension']
    m.Extension.__dict__ = m._Extension.__dict__


ext_modules = [Extension('dpark.portable_hash', ['dpark/portable_hash.pyx'])]
version = '0.3.5'

setup(name='DPark',
      version=version,
      description="Python clone of Spark, MapReduce like "
            +"computing framework supporting iterative algorithms.",
      classifiers=[
        "Programming Language :: Python",
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
      ],
      keywords='dpark python mapreduce spark',
      author='Davies Liu',
      author_email='davies.liu@gmail.com',
      license= 'BSD License',
      packages=['dpark', 'dpark.moosefs'],
      include_package_data=True,
      zip_safe=False,
      setup_requires=['setuptools_cython', 'Cython >= 0.20'],
      url="https://github.com/douban/dpark",
      download_url = 'https://github.com/douban/dpark/archive/%s.tar.gz' % version,
      install_requires=[
          'pymesos<0.2.0',
          'setuptools',
          'pyzmq',
          'msgpack-python',
          'cython',
          'lz4',
          'psutil',
      ],
      tests_require=[
          'nose',
      ],
      test_suite='nose.collector',
      ext_modules=ext_modules,
      scripts=[
          'tools/drun',
          'tools/mrun',
          'tools/executor.py',
          'tools/scheduler.py',
          'tools/dquery',
          'examples/dgrep',
      ]
)

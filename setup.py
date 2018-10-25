import os
import platform
import setuptools
from setuptools import setup, Extension, find_packages

os.environ['ARCHFLAGS'] = ''

setuptools_version = tuple(int(n) for n in setuptools.__version__.split('.'))
assert setuptools_version >= (18, 0, 0), \
    'setuptools >= 18.0.0 required for Cython extension'

ext_modules = [
    Extension('dpark.portable_hash', ['dpark/portable_hash.pyx']),
    Extension('dpark.utils.crc32c', ['dpark/utils/crc32c.c', 'dpark/utils/crc32c_mod.c'],
              extra_compile_args=['-msse4.2']),
]

if platform.python_implementation() != 'PyPy':
    ext_modules.append(Extension('dpark.utils.recursion', ['dpark/utils/recursion.pyx']))

version = '0.5.0'
req = [
    'pymesos>=0.2.10',
    'pyzmq',
    'msgpack-python',
    'psutil>=2.0.0',
    'addict',
    'pyquicklz',
    'py-lz4framed',
    'six',
]

setup(name='DPark',
      version=version,
      description="Python clone of Spark, MapReduce like "
                  + "computing framework supporting iterative algorithms.",
      classifiers=[
          "Programming Language :: Python",
          'Intended Audience :: Developers',
          'License :: OSI Approved :: BSD License',
          'Operating System :: POSIX',
      ],
      keywords='dpark python mapreduce spark',
      author='Davies Liu',
      author_email='davies.liu@gmail.com',
      license='BSD License',
      packages=find_packages(exclude=('tests', 'tests.*')),
      include_package_data=True,
      zip_safe=False,
      setup_requires=['Cython >= 0.20'],
      url="https://github.com/douban/dpark",
      download_url=(
              'https://github.com/douban/dpark/archive/%s.tar.gz' % version
      ),
      install_requires=req,
      tests_require=[
          'nose', 'flaky'
      ],
      test_suite='nose.collector',
      ext_modules=ext_modules,
      scripts=[
          'tools/drun',
          'tools/mrun',
          'tools/executor.py',
          'tools/scheduler.py',
          'tools/dquery',
          'tools/dpark_web.py',
          'tools/dpark_mfs.py',
          'examples/dgrep',
      ]
      )

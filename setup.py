from setuptools import setup, Extension
try:
    from Cython.Distutils import build_ext
except:
    use_cython = False
else:
    use_cython = True

if use_cython:
    ext_modules = [Extension('dpark.portable_hash', ['dpark/portable_hash.pyx'])]
    cmdclass = {'build_ext': build_ext}
else:
    ext_modules = [Extension('dpark.portable_hash', ['dpark/portable_hash.c'])]
    cmdclass = {}

version = '0.1'

setup(name='DPark',
      version=version,
      description="Python clone of Spark, MapReduce like "
            +"computing framework supporting iterative algorithms.",
      long_description=open("README.md").read(),
      # Get more strings from
      # http://pypi.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[
        "Programming Language :: Python",
      ],
      keywords='dpark python mapreduce spark',
      author='Davies Liu',
      author_email='davies.liu@gmail.com',
      license= 'BSD License',
      packages=['dpark', 'dpark.moosefs'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'mesos.interface',
          'pymesos',
          'setuptools',
          'pyzmq',
          'msgpack-python',
          'protobuf',
          'cython',
          'lz4',
          'psutil',
      ],
      tests_require=[
          'nose',
      ],
      test_suite='nose.collector',
      cmdclass = cmdclass,
      ext_modules = ext_modules,
      scripts = [
          'tools/drun',
          'tools/mrun',
          'tools/executor.py',
          'tools/scheduler.py',
          'tools/dquery',
          'examples/dgrep',
      ]
)

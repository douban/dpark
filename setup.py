from setuptools import setup

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
      packages=['dpark', 'dpark.moosefs', 'dpark.pymesos'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'setuptools',
          'pyzmq',
          'msgpack-python',
          'protobuf',
      ],
      tests_require=[
          'nose',
      ],
      test_suite='nose.collector',
)

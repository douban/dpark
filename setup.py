from setuptools import setup

version = '0.1'

setup(name='Dpark',
      version=version,
      description="Python clone of Spark, MapReduce "
            +"computing framework supporting regression calculation.",
      long_description=open("README").read(),
      # Get more strings from
      # http://pypi.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[
        "Programming Language :: Python",
      ],
      keywords='',
      author='Davies Liu',
      author_email='davies.liu@gmail.com',
      license= 'Douban',
      packages=['dpark'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'setuptools',
          # -*- Extra requirements: -*-
      ],
      tests_require=[
          'nose',
      ],
      test_suite='nose.collector',
)

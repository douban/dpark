DPark
=====

|pypi status| |ci status| |gitter|

DPark is a Python clone of Spark, MapReduce(R) alike computing framework
supporting iterative computation.

Installation:

.. code:: bash

    ## Due to the use of C extensions, some libraries need to be installed first.
    
    $ sudo apt-get install libtool pkg-config build-essential autoconf automake
    $ sudo apt-get install python-dev
    $ sudo apt-get install libzmq-dev
    
    ## Then just pip install dpark (``sudo`` maybe needed if you encounter permission problem).
    
    $ pip install dpark


Example for word counting (``wc.py``):

.. code:: python

     from dpark import DparkContext
     ctx = DparkContext()
     file = ctx.textFile("/tmp/words.txt")
     words = file.flatMap(lambda x:x.split()).map(lambda x:(x,1))
     wc = words.reduceByKey(lambda x,y:x+y).collectAsMap()
     print wc

This script can run locally or on a Mesos cluster without any
modification, just using different command-line arguments:

.. code:: bash

    $ python wc.py
    $ python wc.py -m process
    $ python wc.py -m host[:port]

See examples/ for more use cases.

Some more docs (in Chinese):
https://github.com/jackfengji/test\_pro/wiki

DPark can run with Mesos 0.9 or higher.

If a ``$MESOS_MASTER`` environment variable is set, you can use a
shortcut and run DPark with Mesos just by typing

.. code:: bash

    $ python wc.py -m mesos

``$MESOS_MASTER`` can be any scheme of Mesos master, such as

.. code:: bash

    $ export MESOS_MASTER=zk://zk1:2181,zk2:2181,zk3:2181/mesos_master

In order to speed up shuffling, you should deploy Nginx at port 5055 for
accessing data in ``DPARK_WORK_DIR`` (default is ``/tmp/dpark``), such
as:

.. code:: bash

            server {
                    listen 5055;
                    server_name localhost;
                    root /tmp/dpark/;
            }

Mailing list: dpark-users@googlegroups.com
(http://groups.google.com/group/dpark-users)


.. |pypi status| image:: https://img.shields.io/pypi/v/DPark.svg
   :target: https://pypi.python.org/pypi/DPark

.. |gitter| image:: https://badges.gitter.im/douban/dpark.svg
   :alt: Join the chat at https://gitter.im/douban/dpark
   :target: https://gitter.im/douban/dpark?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

.. |ci status| image:: https://travis-ci.org/douban/dpark.svg
   :target: https://travis-ci.org/douban/dpark

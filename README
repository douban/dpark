DPark is a Python clone of Spark, MapReduce computing 
framework supporting regression computation.

Word count example wc.py:

``` python
 from dpark import DparkContext
 ctx = DparkContext()
 file = ctx.textFile("/tmp/words.txt")
 words = file.flatMap(lambda x:x.split()).map(lambda x:(x,1))
 wc = words.reduceByKey(lambda x,y:x+y).collectAsMap()
 print wc
```

This scripts can run locally or on Mesos cluster without
any modification, just with different command arguments:

``` bash
$ python wc.py
$ python wc.py -m process
$ python wc.py -m mesos
```

See examples/ for more examples.

Some Chinese docs: https://github.com/jackfengji/test_pro/wiki

To run DPark on Mesos (0.9 or latest), need MESOS_MASTER and DPARK_WORK_DIR, 
such as: 

``` bash
$ export MESOS_MASTER = zk://zk1:2181,zk2:2181,zk3:2181,zk4:2181,zk5:2181/mesos_master 
$ export DPARK_WORK_DIR = /data1/dpark
```

In order to speed up shuffing, should deploy Nginx at port 5055 
for accessing data in DPARK_WORK_DIR, such as:

``` bash
        server {
                listen 5055;
                server_name localhost;
                root /data1/dpark/;
        }
```
Mailing list: dpark-users@googlegroups.com (http://groups.google.com/group/dpark-users)

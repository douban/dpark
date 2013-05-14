DPark is a Python clone of Spark, MapReduce(R) alike computing
framework supporting regression computation.

Example for word counting (wc.py):

``` python
 import dpark
 file = dpark.textFile("/tmp/words.txt")
 words = file.flatMap(lambda x:x.split()).map(lambda x:(x,1))
 wc = words.reduceByKey(lambda x,y:x+y).collectAsMap()
 print wc
```

This scripts can run locally or on Mesos cluster without
any modification, just with different command arguments:

``` bash
$ python wc.py
$ python wc.py -m process
$ python wc.py -m host[:port]
```

See examples/ for more examples.

Some Chinese docs: https://github.com/jackfengji/test_pro/wiki

DPark can run with Mesos (0.9 or latest).

If $MESOS_MASTER was configured, then you can run it with mesos just typing
``` bash
$ python wc.py -m mesos
```
for shutcut. $MESOS_MASTER can be any scheme of mesos master, such as
``` bash
$ export MESOS_MASTER=zk://zk1:2181,zk2:2181,zk3:2181/mesos_master
```

In order to speed up shuffing, should deploy Nginx at port 5055
for accessing data in DPARK_WORK_DIR (default is /tmp/dpark), such as:

``` bash
        server {
                listen 5055;
                server_name localhost;
                root /tmp/dpark/;
        }
```

Mailing list: dpark-users@googlegroups.com (http://groups.google.com/group/dpark-users)

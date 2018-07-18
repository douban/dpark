# -*- coding: utf-8 -*-

import dpark.conf

try:
    conf = dpark.conf.rddconf(disk_merg=True)
except Exception as e:
    # except AttributeError  as e:
    print(e)

conf = dpark.conf.rddconf(disk_merge=True)

try:
    conf.aa = 1
except Exception as e:
    print(e)

print("%r" % (conf,))
print(conf)

#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from dpark import DparkContext
import pickle
dpark = DparkContext()

webservers = ['theoden', 'balin']
log_path = ["/mfs/log/access-log/current/weblog/%s/%s",]

today = date.today()
for i in range(1, 2):
    day = today - timedelta(days=i)
    path = '/mfs/log/weblog/%s/' % day.strftime("%Y/%m/%d")
    #print 'target', path
    if not os.path.exists(path):
        os.makedirs(path)
    target = dpark.textFile(path)
#    if len(target) > 80:
#        continue
    try:
        logs = [dpark.textFile(p % (h,day.strftime("%Y/%m/%d")), splitSize=512<<20)
                 for p in log_path
                 for h in webservers]
        rawlog = dpark.union(logs)
        #rawlog = rawlog.glom().flatMap(peek(day))
#        print rawlog.take(10)
        weblog = rawlog.pipe('/mfs/log/nginx-log/format_access_log --stream', quiet=True)
        weblog.saveAsTextFile(path)
    except IOError:
        pass

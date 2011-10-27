#!/usr/bin/env python
import sys, os, os.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from dpark import DparkContext

dpark = DparkContext()

today = date.today()
day = date.today().strftime("%Y%m%d")
for i in range(60):
    day = today - timedelta(days=i)
    theday = day - timedelta(days=1)
    path = '/mfs/log/weblog/%s/' % theday.strftime("%Y/%m/%d")
    print 'target', path
    if not os.path.exists(path):
        os.makedirs(path)
    target = dpark.textFile(path)
    if len(target) > 500:
        continue
    try:
        alllog = dpark.union([
            dpark.textFile("/mfs/log/nginx-log/current/%s/access_log-%s" % (h,day.strftime("%Y%m%d")))
            for h in ['bifur', 'bofur', 'faramir']])
        print alllog, len(alllog.splits)
        if len(alllog) < 500:
            continue
        weblog = alllog.pipe('/mfs/log/nginx-log/format_access_log --stream')
        weblog.saveAsTextFile(path).collect()
    except IOError:
        pass

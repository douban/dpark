#!/usr/bin/env python
import sys, os, os.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from dpark import DparkContext
import pickle
dpark = DparkContext()

webservers = ['bifur', 'bofur', 'faramir']
log_path = ["/mfs/log/nginx-log/current/%s/access_log-%s",
    "/mfs/log/nginx-log/current/%s/mobile_access_log-%s"]

def peek(day):
    before = (day-timedelta(days=1)).strftime('%d/%b/%Y')
    after = (day+timedelta(days=1)).strftime('%d/%b/%Y')
    day = day.strftime('%d/%b/%Y')
    def func(lines):
        for line in lines:
            t = line.split(' ', 3)[2]
            d = t[1:12]
            if d == day:
                yield line 
                continue
            t = t[13:18]
            if d == before and t < "23:50":
                return
            if d == after:
                return
    return func

today = date.today()
for i in range(0, 10):
    day = today - timedelta(days=i)
    yesterday = day - timedelta(days=1)
    #path = '/mfs/log/weblog/%s/' % yesterday.strftime("%Y/%m/%d")
    path = '/mfs/log/weblog/%s/' % yesterday.strftime("%Y/%m/%d")
    print 'target', path
    if not os.path.exists(path):
        os.makedirs(path)
    target = dpark.textFile(path)
    if len(target) > 600:
        continue
    try:
        logs = [dpark.textFile(p % (h,d.strftime("%Y%m%d")))
                 for p in log_path
                 for d in (yesterday, day) 
                 for h in webservers]
        rawlog = dpark.union(logs)
        if len(rawlog) < 1000:
            continue
        rawlog = rawlog.glom().flatMap(peek(yesterday))
#        print rawlog.take(10)
        weblog = rawlog.pipe('/mfs/log/nginx-log/format_access_log --stream')
        weblog.saveAsTextFile(path).collect()
    except IOError:
        pass

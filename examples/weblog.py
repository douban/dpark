#!/usr/bin/env python
import sys, os, os.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from dpark import DparkContext

dpark = DparkContext()

webservers = ['bifur', 'bofur', 'faramir']

def peek(d1, d2, d3):
    d1 = d1.strftime('%d/%b/%Y')
    d2 = d2.strftime('%d/%b/%Y')
    d3 = d3.strftime('%d/%b/%Y')
    def func(lines):
        for line in lines:
            t = line.split(' ', 3)[2]
            d = t[1:12]
            if d == d2:
                yield line 
                continue
            t = t[13:18]
            if d == d1 and t < "23:50":
                return
            if d == d3:
                return
    return func

today = date.today()
for i in range(30):
    day = today - timedelta(days=i)
    yesterday = day - timedelta(days=1)
    dby = day - timedelta(days=2)
    path = '/mfs/log/weblog/%s/' % yesterday.strftime("%Y/%m/%d")
    print 'target', path
    if not os.path.exists(path):
        os.makedirs(path)
    target = dpark.textFile(path)
    if len(target) > 530:
        continue
    try:
        rawlog = dpark.union([
            dpark.textFile("/mfs/log/nginx-log/current/%s/access_log-%s" 
                % (h,yesterday.strftime("%Y%m%d"))) for h in webservers])
        rawlog = rawlog.union(dpark.union([
            dpark.textFile("/mfs/log/nginx-log/current/%s/access_log-%s" 
                % (h,day.strftime("%Y%m%d"))) for h in webservers]))
        #print 'rawlog', len(rawlog)
        if len(rawlog) < 1000:
            continue
        rawlog = rawlog.glom().flatMap(peek(dby, yesterday, day))
        #print rawlog.take(10)
        weblog = rawlog.pipe('/mfs/log/nginx-log/format_access_log --stream')
        weblog.saveAsTextFile(path).collect()
    except IOError:
        pass

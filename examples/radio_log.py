#!/usr/bin/env python
import sys, os, os.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from dpark import DparkContext

dpark = DparkContext()

servers = ['theoden','balin']
log_path = "/mfs/log/access-log/current/radiolog/%s"
radiolog = dpark.union(
        [dpark.textFile(log_path % h, followLink=False, splitSize=256*1024*1024)
        for h in servers])
#radiolog = dpark.csvFile(log_path%'balin'+'/radiolog_current')[:2]
print len(radiolog)

cnt = radiolog.map(
        lambda line:line.split(',')
    ).filter(
        lambda line:len(line) > 4 and line[0] in 'UP'
    ).map(
        lambda line:("%s:%s"%(line[1],line[3]), 1)
    ).reduceByKey(lambda x,y:(x+y), 256)

rcnt = cnt.map(lambda (k,v):(v,1)).reduceByKey(
        lambda x,y:x+y
    ).collectAsMap()

for n in rcnt:
    print n, rcnt[n]

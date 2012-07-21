#!/usr/bin/env python
import sys, os.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from dpark import DparkContext

dpark = DparkContext()

today = date.today()
day = date.today().strftime("%Y%m%d")
for i in range(30):
    day = today - timedelta(days=i)
    theday = day - timedelta(days=1)
    tpath = '/mfs/log/weblog/%s/' % theday.strftime("%Y/%m/%d")
    target = dpark.textFile(tpath)
    if len(target) > 500:
        continue
    alllog = dpark.union([
        dpark.textFile("/mfs/log/nginx-log/current/%s/access_log-%s" % (h,day.strftime("%Y%m%d")))
        for h in ['bifur', 'bofur', 'faramir']])
    if len(alllog) < 400:
        continue
    print alllog, len(alllog.splits)
    weblog = alllog.pipe('/mfs/log/nginx-log/format_access_log --stream', quiet=True)
    alllog.saveAsTextFile(tpath)

#weblog = dpark.textFile('/mfs/log/weblog/%s/' % dpath)

#weblog = weblog.filter(lambda line:line.split(',')[6].startswith('/site/-/zuoxiao'))
#print weblog.collect()

#nurls = weblog.map(lambda line:(line.split(',',7)[6],1)).reduceByKey(lambda x,y:x+y).collectAsMap()

#from operator import itemgetter
#for u,c in sorted(nurls.items(), key=itemgetter(1), reverse=True):
#for u in sorted(nurls):
#    print u,nurls[u]

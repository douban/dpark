#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from operator import itemgetter, add
import subprocess
from dpark import DparkContext
import pickle

sinkservers = ['balin', 'theoden']
log_path = "/mfs/log/access-log/current/weblog/%s/%s"
DATE,TIME,UID,IP,BID,METHOD,NURL,URL,CODE,LENGTH,PT,NREFERER,REFERER = range(13)
LIMITS = [10,8,11,12,20,8,80,255,3,8,5,80,255] 

def drop_args(url):
    return url[:url.index('?')] if '?' in url else url

def clean(dpark, topr):
    topr = dpark.broadcast(topr)
    def _(line):
        line = [v.replace(',','%2C')[:LIMITS[i]] for i,v in enumerate(line)]
#        line[NURL] = drop_args(line[NURL]) 
#        line[NREFERER] = drop_args(line[NREFERER]) 
        if line[NREFERER] not in topr.value:
            line[NREFERER] = ''
        return (line[NURL], ','.join(line))
    return _

def drop_nurl(l):
    l = l.split(',')
    l[NURL] = ''
    return ','.join(l)

def load_weblog(day):
    path = '/mfs/tmp/daily_weblog/%s' % day.strftime("%Y%m%d")
    if not os.path.exists(path) or len([1 for f in os.listdir(path) if f.endswith('.csv')]) < 16:
        webpath = '/mfs/log/weblog/%s' % day.strftime("%Y/%m/%d")
        total = sum([os.path.getsize(os.path.join(webpath,f)) for f in os.listdir(webpath) if os.path.isfile(os.path.join(webpath,f))])/1024/1024/1024
        if total < 26:
            return
        dpark = DparkContext()
        weblog = dpark.csvFile('/mfs/log/weblog/%s' % day.strftime("%Y/%m/%d"))
        topreferers = weblog.map(
                lambda l:(l[NREFERER], 1)).reduceByKey(add).filter(lambda (x,y): y>1000).collectAsMap()
        g = weblog.map(clean(dpark, topreferers)).groupByKey()
        s = g.flatMap(
                lambda (u,ls): len(ls) > 1000 and ls or [drop_nurl(l) for l in ls]
            ).saveAsTextFile(path, ext='csv', overwrite=False)
        dpark.stop()

    for name in sorted(os.listdir(path)):
        if name.startswith('.'):
            continue
        if name.endswith('done'):
            continue
        if not name.endswith("csv"):
            continue
        flag = os.path.join(path, name+".done")
        if os.path.exists(flag):
            continue
        cmd = "mysql -hbalin -uluzong -pfulllink -P4406 rivendell -e".split(' ')
        cmd.append(r"LOAD DATA INFILE '%s' INTO TABLE weblog FIELDS TERMINATED by ',' ENCLOSED BY '\"'"
                  % os.path.join(path,name))
        try:
            open(flag, 'w').write('OK')
            #print ' '.join(cmd)
            p = subprocess.Popen(cmd)
            p.wait()
            if p.returncode != 0:
                print 'load failed', os.path.join(path,name)
                os.remove(flag)
        except :
            os.remove(flag)

    open('/mfs/mysql-ib-eye/flags/done','w').write('OK')

if __name__ == '__main__':
    today = date.today() 
    for i in range(1, 5):
        day = today - timedelta(days=i)
        load_weblog(day)

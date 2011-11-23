#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime,timedelta
from operator import itemgetter
import csv
from cStringIO import StringIO
import subprocess
from dpark import DparkContext
import pickle
dpark = DparkContext()

sinkservers = ['balin', 'theoden']
log_path = "/mfs/log/access-log/current/weblog/%s/%s"
DATE,TIME,UID,IP,BID,METHOD,NURL,URL,CODE,LENGTH,PT,NREFERER,REFERER = range(13)
LIMITS = [10,8,11,12,20,8,255,255,3,8,5,255,255]

def format_weblog(hour):
    logs = [dpark.textFile(log_path % (h,hour.strftime("%Y/%m/%d/%H/")), splitSize=10<<20)
             for h in sinkservers]
    rawlog = dpark.union(logs)
    return rawlog.pipe('/mfs/log/nginx-log/format_access_log --stream')

def parse(line):
    return csv.reader(StringIO(line)).next()

def drop_args(url):
    return url[:url.index('?')] if '?' in url else url

def clean(line):
    line = [v.replace(',','%2C')[:LIMITS[i]] for i,v in enumerate(line)]
    line[NURL] = drop_args(line[NURL])
    line[NREFERER] = drop_args(line[NREFERER])
    return (line[NURL], ','.join(line))

def drop_nurl(line):
    ps = line.split(',')
    ps[NURL] = ''
    return ','.join(ps)

def load_weblog(hour):
    path = '/mfs/tmp/hourly_weblog/%s' % hour.strftime("%Y%m%d%H")
    if not os.path.exists(path):
        weblog = format_weblog(hour)
        g = weblog.map(parse).map(clean).groupByKey()
        s = g.flatMap(
                lambda (u,ls): len(ls) > 10 and ls or [drop_nurl(l) for l in ls]
            ).saveAsTextFile(path, ext='csv')
    
    for name in os.listdir(path):
        if name.startswith('.'): continue
        if not name.endswith("csv"):
            continue
        flag = os.path.join(path, name+".done")
        if os.path.exists(flag):
            continue
        cmd = "mysql -hbalin -uluzong -pfulllink -P4406 rivendell -e".split(' ')
        cmd.append("LOAD DATA INFILE '%s' INTO TABLE hourly_weblog FIELDS TERMINATED by ',' ENCLOSED BY 'NULL'"
                  % os.path.join(path,name))
        #print ' '.join(cmd)
        p = subprocess.Popen(cmd)
        p.wait()
        if p.returncode == 0:
            open(flag, 'w').write('OK')
        else:
            print 'load failed', os.path.join(path,name)

if __name__ == '__main__':
    now = datetime.now() 
    for i in range(1, 24):
        hour = now - timedelta(seconds=3600*i)
        load_weblog(hour)

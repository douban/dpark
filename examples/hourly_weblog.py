#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime,timedelta
from operator import itemgetter
import subprocess
from dpark import DparkContext
import pickle
dpark = DparkContext()

sinkservers = ['balin', 'theoden']
log_path = "/mfs/log/access-log/current/weblog/%s/%s"
DATE,TIME,UID,IP,BID,METHOD,NURL,URL,CODE,LENGTH,PT,NREFERER,REFERER = range(13)

def format_weblog(hour):
    logs = [dpark.textFile(log_path % (h,hour.strftime("%Y/%m/%d/%H/")), splitSize=10<<20)
             for h in sinkservers]
    rawlog = dpark.union(logs)
    return rawlog.pipe('/mfs/log/nginx-log/format_access_log --stream')

def drop_nurl(line):
    ps = line.split(',')
    ps[NURL] = ''
    return ','.join(ps)

def load_weblog(hour):
    path = '/mfs/tmp/hourly_weblog/%s' % hour.strftime("%Y%m%d%H")
    if not os.path.exists(path):
        weblog = format_weblog(hour)
        g = weblog.map(lambda line:line.split(",")).filter(
                lambda line:len(line) == 13
            ).map(
                lambda l:(l[NURL], ",".join(i[:255] for i in l))
            ).groupByKey()
        #    cnt = g.mapValue(len).collectAsMap()
        #    topnurl = sorted(cnt.iteritems(), key=itemgetter(1), reverse=True)[:5000]
        #    print len(cnt), topnurl[-10:]
        #    topnurl = dict(topnurl)
        s = g.flatMap(
                lambda (u,ls): len(ls) > 10 and ls or [drop_nurl(l) for l in ls]
            ).saveAsTextFile(path, ext='csv')

    # ib
    for name in os.listdir(path):
        if not name.endswith("csv"):
            continue
        flag = os.path.join(path, name+".done")
        if os.path.exists(flag):
            continue
        cmd = "mysql -hbalin -uluzong -pfulllink -P4406 rivendell -e".split(' ')
        cmd.append("LOAD DATA INFILE '%s' INTO TABLE hourly_weblog FIELDS TERMINATED by ',' ENCLOSED BY 'NULL'"
                  % os.path.join(path,name))
        print ' '.join(cmd)
        p = subprocess.Popen(cmd)
        p.wait()
        if p.returncode == 0:
            open(flag, 'w').write('OK')
        
if __name__ == '__main__':
    now = datetime.now() 
    for i in range(1, 10):
        hour = now - timedelta(seconds=3600*i)
        load_weblog(hour)

#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
import cPickle
from dpark import DparkContext
from douban.sqlstore import store_from_config

DATE,TIME,UID,IP,BID,METHOD,NURL,URL,CODE,LENGTH,PT,NREFERER,REFERER = range(13)
dpark = DparkContext()

def parse_city(line):
    u = line[URL]
    if not u.startswith('/movie/subject/'):
        return []
    us = u.split('/')
    if len(us) < 7: return []
    if us[4] != 'cinema': return []
    return [(us[5], line[BID])]

def cinema_stat(day):
    path = '/mfs/log/weblog/%s' % day.strftime("%Y/%m/%d")
    cinema_log = dpark.csvFile(path).flatMap(parse_city)
    logs = cinema_log.groupByKey().collectAsMap()
    return [(loc, len(bids), len(set(bids))) for loc,bids in logs.iteritems()]

sys.path.append('/var/shire')
store = store_from_config("shire-offline")

def save_report(day, stats):
    for loc,pv,uv in sorted(stats):
        if not loc.isdigit():
            r = store.execute('select id from loc where uid=%s',loc)
            if r:
                loc = r[0][0]
            else:
                continue
        store.execute('replace into cinema_report (date, loc_id, pv, uv) '
                'values (%s,%s,%s,%s)', (day, loc, pv, uv))
    store.commit()
    #print 'completed', day, len(stats)

if __name__ == '__main__':
    for i in range(1, 10):
        day = date.today() - timedelta(days=i)
        path = '/mfs/tmp/cinema-report-%s' % day.strftime("%Y-%m-%d")
        if not os.path.exists(path):
            stats = cinema_stat(day)
            if len(stats) > 100:
                cPickle.dump(stats, open(path,'w'))
                save_report(day, stats)
#        else:
#            stats = cPickle.load(open(path))
#            save_report(day, stats)

#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
import cPickle
from dpark import DparkContext
from douban.sqlstore import store_from_config

DATE,TIME,UID,IP,BID,METHOD,NURL,URL,CODE,LENGTH,PT,NREFERER,REFERER = range(13)
dpark = DparkContext()

def parse_time(ss):
    h,m,s = ss.split(':')
    return int(h)*3600 + int(m)*60 + int(s)

def site_log(day):
    path = '/mfs/log/weblog/%s' % day.strftime("%Y/%m/%d")
    sitelog = dpark.csvFile(path).filter(
            lambda l:l[URL].startswith('/site/')
        ).map(
            lambda line:(line[URL][5:],(line[BID],parse_time(line[TIME])))
        ).groupByKey()

    black_dir = ('widget','censor', 'j', 'cart', 'invite', 'about', 'tos', 'blank', 'apply', 'site')

    sites = sitelog.map(
            lambda (u,bids):(u.split('/'),bids)
        ).filter(
            lambda (us,bids):len(us)>1 and us[1] not in black_dir
        ).map(
            lambda (us,bids):(us[1],bids)
        ).reduceByKey(lambda x,y:x+y).collectAsMap()

    widgets = sitelog.filter(
                lambda (u,c):u.startswith('/widget/')
            ).map(
                lambda (u,c):(':'.join(u.split('/')[3:4]),c)
            ).reduceByKey(lambda x,y:x+y).collectAsMap()
    return sites, widgets 

def merge_by_site(sites, widgets, site_widgets):
    for uid in site_widgets:
        bids = sites.setdefault(uid, [])
        for wid in site_widgets[uid]:
            bids.extend(widgets.get(wid, []))
    return dict((uid, (len(bids),len(set(bids))))
        for uid,bids in sites.iteritems())

VISIT_PERIOD = 30 * 60
MIN_PERIOD = 10

def calcu(logs):
    logs.sort()
    visits = {}
    for bid, t in logs:
        if bid not in visits:
            visits[bid] = [[t,t]]
        else:
            last = visits[bid][-1][1]
            if t < last + VISIT_PERIOD:
                visits[bid][-1][1] = t
            else:
                visits[bid].append([t,t])
    pv = len(logs)
    uv = len(visits)
    n_visit = sum(len(vs) for vs in visits.itervalues())
    avg_time = sum(max(MIN_PERIOD, b-a) 
            for vs in visits.itervalues() for a,b in vs) / n_visit
    return pv, uv, n_visit, avg_time

sys.path.append('/var/shire')
store = store_from_config("shire-offline")

def save_report(day, sites, widgets):
    from luzong.site import Site
    for uid in sites:
        site = Site.get(uid)
        if not site:
            continue
        logs = sites[uid]
        for rid, in store.execute("select id from room where site_id=%s", site.id):
            for wid, in store.execute("select id from widget where container_id=%s", rid):
                logs.extend(widgets.get(str(wid),[]))
        pv, uv, visits, avg_time = calcu(logs)
        store.execute("replace into site_report (date,site_id,pv,uv,visits,avg_time) "
            "values (%s,%s,%s,%s,%s,%s)", 
            (day, site.id, pv, uv, visits, avg_time))
    for wid in widgets:
        if wid.isdigit():
            uv = len(dict((widgets[wid])))
            store.execute("replace into widget_report (date, widget_id, pv,uv)"
                " values (%s,%s,%s,%s)", 
                (day,wid,len(widgets[wid]),uv))
    store.commit()
    print 'completed', len(sites)

if __name__ == '__main__':
    for i in range(1, 10):
        day = date.today() - timedelta(days=i)
        path = '/tmp/site-report-%s' % day.strftime("%Y-%m-%d")
#        print day, path
        if not os.path.exists(path):
            sites, widgets = site_log(day)
            if len(sites) > 1000 and len(widgets) > 1000:
                cPickle.dump((len(sites), len(widgets)), open(path,'w'))
                save_report(day, sites, widgets)
#        else:
#            sites, widgets = cPickle.load(open(path))

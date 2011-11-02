#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
import cPickle
from dpark import DparkContext
from douban.sqlstore import store_from_config

DATE,TIME,UID,IP,BID,METHOD,NURL,URL,CODE,LENGTH,PT,NREFERER,REFERER = range(13)
dpark = DparkContext()

def site_log(day):
    path = '/mfs/log/weblog/%s' % day.strftime("%Y/%m/%d")
    alllog = dpark.textFile(path).map(lambda line:line.split(',')).map(lambda line:(line[URL],line[BID]))
    sitelog = alllog.filter(lambda (u,bid): u.startswith('/site/')).map(lambda (u,bid):(u[5:],bid)).groupByKey()

    black_dir = ('widget','censor', 'j', 'cart', 'invite', 'about', 'tos', 'blank', 'apply')

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

sys.path.append('/var/shire')
store = store_from_config("shire-offline")

def save_report(day, sites, widgets):
    from luzong.site import Site
    for uid in sites:
        bids = sites[uid]
        site = Site.get(uid)
        if not site:
            continue
        for rid, in store.execute("select id from room where site_id=%s", site.id):
            for wid, in store.execute("select id from widget where container_id=%s", rid):
                bids.extend(widgets.get(str(wid),[]))
        pv = len(bids)
        uv = len(set(bids))
        store.execute("replace into site_report (date,site_id,pv,uv) "
            "values (%s, %s, %s, %s)", (day, site.id, pv, uv))
    for wid in widgets:
        if wid.isdigit():
            store.execute("replace into widget_report (date, widget_id, pv,uv)"
                " values (%s,%s,%s,%s)", 
                (day,wid,len(widgets[wid]),len(set(widgets[wid]))))
    store.commit()
    print 'completed', len(sites)

if __name__ == '__main__':
    for i in range(1, 10):
        day = date.today() - timedelta(days=i)
        path = '/tmp/site-report-%s' % day.strftime("%Y-%m-%d")
        print day, path
        if not os.path.exists(path):
            sites, widgets = site_log(day)
            if len(sites) > 1000 and len(widgets) > 1000:
                cPickle.dump((len(sites), len(widgets)), open(path,'w'))
                save_report(day, sites, widgets)
#        else:
#            sites, widgets = cPickle.load(open(path))

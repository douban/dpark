#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
import cPickle
from operator import add
from dpark import DparkContext
from douban.sqlstore import store_from_config

DATE,TIME,UID,IP,BID,METHOD,NURL,URL,CODE,LENGTH,PT,NREFERER,REFERER = range(13)
dpark = DparkContext()

def parse_time(ss):
    h,m,s = ss.split(':')
    return int(h)*3600 + int(m)*60 + int(s)

def site_log(day):
    path = '/mfs/log/weblog/%s' % day.strftime("%Y/%m/%d")
    weblog = dpark.csvFile(path)
    sitelog = weblog.filter(
            lambda l:l[BID] and l[URL].startswith('/site/')
        ).map(
            lambda line:(line[URL][5:],(line[BID],parse_time(line[TIME])))
        ).groupByKey()
    
    # pv,uv,visits
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

    # pv,uv of rooms
    front_pages = sitelog.map(
            lambda (u,bids):(u.split('/'),bids)
        ).filter(
            lambda (us,bids):len(us)==3
        ).map(
            lambda (us, bids):(us[1],bids)
        ).reduceByKey(lambda x,y:x+y).collectAsMap()

    rooms = sitelog.map(
            lambda (u,bids):(u.split('/'),bids)
        ).filter(
            lambda (us,bids):len(us)>3 and us[2] == 'room'
        ).map(
            lambda (us,bids):(us[3],bids)
        ).reduceByKey(lambda x,y:x+y).collectAsMap()

    # share
    sharelog = weblog.filter(
            lambda l:l[BID] and l[URL].startswith("/shuo/!service/share") and l[REFERER].startswith("/site/")
        ).map(
            lambda line:(line[REFERER][5:], 1)
        ).reduceByKey(add)

    site_share = sharelog.map(
            lambda (u,c):(u.split('/'),c)
        ).filter(
            lambda (us,c):len(us)>1 and us[1] not in black_dir
        ).map(
            lambda (us,c):(us[1],c)
        ).reduceByKey(add).collectAsMap()

    widget_share = sharelog.filter(
                lambda (u,c):u.startswith('/widget/')
            ).map(
                lambda (u,c):(':'.join(u.split('/')[3:4]),c)
            ).reduceByKey(add).collectAsMap()

    return sites, widgets, front_pages, rooms, site_share, widget_share

#def merge_by_site(sites, widgets, site_widgets):
#    for uid in site_widgets:
#        bids = sites.setdefault(uid, [])
#        for wid in site_widgets[uid]:
#            bids.extend(widgets.get(wid, []))
#    return dict((uid, (len(bids),len(set(bids))))
#        for uid,bids in sites.iteritems())

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

def save_report(day, sites, widgets, front_pages, rooms, site_shares, widget_shares):
    from luzong.site import Site, Room
    for uid in sites:
        site = Site.get(uid)
        if not site:
            continue
        logs = sites[uid]
        share = site_shares.get(uid, 0)
        for rid, in store.execute("select id from room where site_id=%s", site.id):
            for wid, in store.execute("select id from widget where container_id=%s", rid):
                logs.extend(widgets.get(str(wid),[]))
                share += widget_shares.get(str(wid), 0)
        pv, uv, visits, avg_time = calcu(logs)
        store.execute("replace into site_report (date,site_id,pv,uv,visits,avg_time,share) "
            "values (%s,%s,%s,%s,%s,%s,%s)", 
            (day, site.id, pv, uv, visits, avg_time,share))

        # front page is default room
        rooms.setdefault(site.default_room.uid, []).extend(front_pages.get(site.default_room.uid, []))

    for wid in widgets:
        if wid.isdigit():
            uv = len(dict((widgets[wid])))
            share = widget_shares.get(wid, 0)
            store.execute("replace into widget_report (date, widget_id, pv, uv, share)"
                " values (%s,%s,%s,%s, %s)", 
                (day,wid,len(widgets[wid]), uv, share))

    for id,bids in rooms.iteritems():
        if id and id.isdigit():
            store.execute("replace into room_report (date, room_id, pv, uv)"
                " values (%s, %s, %s, %s)", (day, id, len(bids), len(dict(bids))))

    store.commit()
    print 'completed', len(sites)

if __name__ == '__main__':
    for i in range(1, 30):
        day = date.today() - timedelta(days=i)
        path = '/tmp/site-report-%s' % day.strftime("%Y-%m-%d")
        if not os.path.exists(path):
            print day
            sites, widgets, fronts, rooms, site_shares, widget_shares = site_log(day)
            if len(sites) > 1000 and len(widgets) > 1000:
                save_report(day, sites, widgets, fronts, rooms, site_shares, widget_shares)
                cPickle.dump((len(sites), len(rooms), len(widgets)), open(path,'w'))

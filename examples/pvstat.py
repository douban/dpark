#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
import cPickle
from dpark import DparkContext
from operator import itemgetter
from dpark.dependency import Aggregator

dpark = DparkContext()

DATE,TIME,UID,IP,BID,METHOD,NURL,URL,CODE,LENGTH,PT,NREFERER,REFERER = range(13)

def spammer_map(line):
    if len(line) < NURL: return
    uid,ip,bid,_,nurl = line[UID:URL]
    if nurl.startswith("/service/api") or nurl.startswith("/fm/"):
        return
    upv = uid and 1 or 0
    apv = not uid and not bid and 1 or 0
    yield ip,(upv,apv)

def gen_map(crawler_ip):
    crawler_ip = dpark.broadcast(crawler_ip)
    def gen_data(line):
        try:
            uid,ip,bid,_,nurl,_,code,_,pt = line[UID:11]
            if not uid and not bid and ip in crawler_ip.value:
                return
            if int(code) >= 400:
                return
            upv, apv = (1, 0) if uid else (0, 1)
            pt = pt and float(pt) or 0.1
            if pt > 3: pt = 0.1
            bid = '' if uid else bid
            v = (upv,apv,pt,uid,bid,ip)
            yield (0,nurl),v
            # root url
            nurls = nurl.split('?',1)[0].split('#')[0].split('/')
            for i in range(len(nurls)):
                u = '/'.join(nurls[:i+1])
                yield (1, u), v
        except Exception:
            print line 
    return gen_data 

def create((upv, apv, pt, uid, bid, ip)):
    return (upv, apv, pt, set([uid]), set([bid]), set([ip]))
def mergeData((upv, apv, pt, uid, bid, ip), 
        (upv1, apv1, pt1, uid1, bid1, ip1)):
    uid |= uid1
    bid |= bid1
    ip |= ip1
    return (upv+upv1, apv+apv1, pt+pt1, uid, bid, ip)
def addData(all, new):
    return mergeData(all, create(new))
agg = Aggregator(create, addData, mergeData)

def get_pvstat(theday):
    path = '/mfs/log/weblog/%s/' % theday.strftime("%Y/%m/%d")
    weblog = dpark.csvFile(path)

    crawler_ip = weblog.flatMap(spammer_map).reduceByKey(
            lambda (x1,x2),(y1,y2): (x1+y1,x2+y2)
        ).filter(
            lambda (ip,(upv,apv)): apv > 60 and apv > upv*10
        ).collectAsMap()
    #print sorted(bad_ip.iteritems(), key=itemgetter(1), reverse=True)[:10]
    #print len(bad_ip)
    #user_bids = set(weblog.flatMap(
    #    lambda line:line[UID] and [(line[BID],1)] or []
    #    ).reduceByKey(lambda x,y:x).collectAsMap().keys())
    #user_bids = dpark.broadcast(user_bids)

    weblog = weblog.flatMap(gen_map(crawler_ip))

    pv = weblog.combineByKey(agg, 32).mapValue(
            lambda (upv,apv,pt,uid,bid,ip):
                (upv,apv,pt/(upv+apv),len(uid),len(bid),len(ip))
        ).collectAsMap()
    return pv

def get_parent(nurl):
    if  nurl.endswith("/"):
        return nurl and "/".join(nurl.rstrip("/").split("/")) or ""
    else:
        return nurl and "/".join(nurl.rstrip("/").split("/")[:-1]) or ""

def save(day, pvstats):
    from douban.sqlstore import store_from_config
    store = store_from_config('rohan-online')
    store.execute('delete from pvstat2 where day=%s', day)
    store.execute('delete from rooturl3 where day=%s', day)
    for ((flag, nurl),(upv,apv,pt,uid,bid,ip)) in pvstats.iteritems():
        pv = upv + apv
        if flag == 0:
            try:
                store.execute('insert into pvstat2 (day,nurl,pv,upv,apv,ip,uid,bid,pt) '
                'values (%s,%s,%s,%s,%s,%s,%s,%s,%s)', 
                (day, nurl[:120], pv,upv,apv,ip,uid,bid,pt*1000))
            except Exception:
                #print nurl
                pass
        else:
            #rooturl
            #parent = '/'.join(nurl.split('/')[:-1])
            parent = get_parent(nurl)
            try:
                store.execute('insert into rooturl3 (day,parent,nurl,pv,upv,apv,ip,uid,bid,pt)' 
                    ' values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                    (day,parent,nurl[:120],pv,upv,apv,ip,uid,bid,pt*1000))
            except Exception:
                #print parent, nurl
                pass
    store.commit()

if __name__ == '__main__':
    for i in range(1,60):
        day = date.today() - timedelta(days=i)
        print day
        name = '/mfs/tmp/pvstat-%s' % day
        if os.path.exists(name):
            #pv = cPickle.load(open(name))
            pass
        else:
            try:
                pv = get_pvstat(day)
            except IOError:
                continue
            cPickle.dump(pv, open(name,'w'))
            print len(pv)
            save(day, pv)

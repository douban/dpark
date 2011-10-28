#!/usr/bin/env python
import sys, os, os.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from dpark import DparkContext
from operator import itemgetter
from dpark.dependency import Aggregator

dpark = DparkContext()

theday = date.today() - timedelta(days=1)
path = '/mfs/log/weblog/%s/' % theday.strftime("%Y/%m/%d")

DATE,TIME,UID,IP,BID,METHOD,NURL,URL,CODE,LENGTH,PT,NREFERER,REFERER = range(13)

weblog = dpark.csvFile(path)
bad_ip = weblog.filter(
        lambda line:len(line) > BID and not line[BID]
    ).map(
        lambda line:(line[IP], 1)
    ).reduceByKey(
        lambda x,y:x+y
    ).filter(
        lambda (ip,c): c > 600
    ).collectAsMap()

#print sorted(bad_ip.iteritems(), key=itemgetter(1), reverse=True)[:10]
print len(bad_ip)

def gen_data(line):
    try:
        uid,ip,bid,_,nurl,_,_,_,pt = line[UID:11]
        if not uid and not bid and ip in bad_ip:
            return
        upv, apv = (1, 0) if uid else (0, 1)
        pt = pt and float(pt) or 0.1
        if pt > 3: pt = 0.1
        bid = '' if uid else bid
        aip = '' if uid else ip
        v = (upv,apv,pt,uid,bid,ip,aip)
        yield (0,nurl),v
        # root url
        nurls = nurl.split('/', 7)
        for i in range(1, len(nurls)):
            u = '/'.join(nurls[:i])
            yield (1, u), v
    except Exception:
        print line 

def create((upv, apv, pt, uid, bid, ip, aip)):
    return (upv, apv, pt, set([uid]), 
        set([bid]), set([ip]), set([aip]))
def mergeData((upv, apv, pt, uid, bid, ip, aip), 
        (upv1, apv1, pt1, uid1, bid1, ip1, aip1)):
    uid |= uid1
    bid |= bid1
    ip |= ip1
    aip |= aip1
    return (upv+upv1, apv+apv1, pt+pt1, uid, bid, ip, aip)
def addData(all, new):
    return mergeData(all, create(new))
agg = Aggregator(create, addData, mergeData)

weblog = weblog.flatMap(gen_data)
print weblog.first()

pvstat = weblog.combineByKey(agg).filter(
        lambda (_,vs): vs[0] > 1000000
    ).mapValue(
        lambda (upv,apv,pt,uid,bid,ip,aip):
            (upv,apv,pt/(upv+apv),len(uid),len(bid),len(ip),len(aip))
    ).collectAsMap()

print pvstat#[(1,'')] # all pv

#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from urllib import unquote
from dpark import DparkContext
import cPickle
from urlformat import *
from dpark.dependency import Aggregator

dpark = DparkContext()

webservers = ['bifur', 'bofur', 'faramir']
log_path = ["/mfs/log/nginx-log/current/%s/beacon_log-%s",]

def peek(day):
    after = (day+timedelta(days=1)).strftime('%d/%b/%Y')
    day = day.strftime('%d/%b/%Y')
    def func(lines):
        for line in lines:
            t = line.split(' ', 3)[2]
            d = t[1:12]
            if d == day:
                yield line 
            elif d == after:
                return
    return func

def urldecode(url):
    result={}
    url=url.split("?",1)
    if len(url)==2:
        for i in url[1].split("&"):
            i=i.split("=",1)
            if len(i)==2:
                result[(i[0])]= len(i[1]) >= 3 and unquote(i[1]) or i[1]
    return result

def format_line(line):
    try:
        host,_,url = line.split(' ')[4:7]
        i = urldecode(url)
        done = i.get('t_done', '0')
        if 'u' not in i or '"' in i['u']:
            return
        url = i['u']
        if url.startswith('http') and 'douban.com/' in url:
            url = url[url.index('douban.com/') + 10:]
        url = hosts.get(host, '') + url
        while url.startswith('//'): url=url[1:]
        yield ourl(url), int(done)
    except Exception:
        print line
    
def gen_tti((nurl, tti)):
    if tti > 10000 or tti < 0:
        tti = 2000
    yield (0, nurl), (1, tti)
    # root url
    nurls = nurl.split('?',1)[0].split('#')[0].split('/')
    for i in range(len(nurls)):
        u = '/'.join(nurls[:i+1])
        yield (1, u), (1, tti) 

def calc_tti(day):
    logs = [dpark.textFile(p % (h,d.strftime("%Y%m%d")))
             for p in log_path
             for d in (day, day + timedelta(days=1)) 
             for h in webservers
             if os.path.exists(p % (h,d.strftime("%Y%m%d")))]
    rawlog = dpark.union(logs)
    beacon = rawlog.glom().flatMap(peek(day)).flatMap(format_line)
    tti = beacon.flatMap(gen_tti).reduceByKey(
            lambda (a1,b1),(a2,b2):((a1+a2),(b1+b2))
        ).mapValue(
            lambda (pv,tti):tti/pv
        ).collectAsMap()
    return tti    

def get_parent(nurl):
    if  nurl.endswith("/"):
        return nurl and "/".join(nurl.rstrip("/").split("/")) or ""
    else:
        return nurl and "/".join(nurl.rstrip("/").split("/")[:-1]) or ""

def save(day, tti_stats):
    from douban.sqlstore import store_from_config
    store = store_from_config('rohan-online')
    for (flag, nurl), tti in tti_stats.iteritems():
        try:
            nurl.decode('utf8').encode('latin1')
            if flag == 0:
                store.execute('update pvstat2 set tti=%s where day=%s and nurl=%s',
                     (tti, day, nurl[:120]))
            else:
                store.execute('update rooturl3 set tti=%s where day=%s and parent=%s '
                    ' and nurl=%s', (tti, day, get_parent(nurl)[:120], nurl[:120]))
        except Exception:
            pass
    store.commit()
    #print 'completed', len(tti_stats)

if __name__ == "__main__":
    today = date.today()
    for i in range(1, 3):
        day = today - timedelta(days=i)
        name = '/tmp/beacon-%s' % day
        if os.path.exists(name):
            tti = cPickle.load(open(name))
            save(day, tti)
        else:
            tti = calc_tti(day)
            if len(tti) > 1000:
                cPickle.dump(tti, open(name, 'w'), -1)
                save(day, tti)

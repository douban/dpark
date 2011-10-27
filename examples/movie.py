#!/usr/bin/env python
import sys, os.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from dpark import DparkContext

dpark = DparkContext()

path = '/mfs/tmp/weblog-20111019.csv.small'
path = '/mfs/tmp/weblog-20111019.csv.medium'
path = '/mfs/tmp/weblog-20111019.csv.medium'

alllog = dpark.textFile(path)
yesterday = date.today().strftime("%Y%m%d")
alllog = dpark.union([
    dpark.textFile("/mfs/log/nginx-log/current/%s/access_log-%s.csv" % (h,yesterday))
    for h in ['bifur', 'bofur', 'faramir']])
print alllog, len(alllog.splits)

alllog = alllog.map(lambda line:line.split(',')[7])
sitelog = alllog.filter(lambda u: u.startswith('/site/')).map(lambda u:(u[5:],1)).reduceByKey(lambda x,y:x+y)

black_dir = ('widget','censor', 'j', 'cart', 'invite', 'about', 'tos', 'blank', 'apply')
sites = sitelog.map(lambda (u,pv):(u.split('/'),pv)).filter(lambda (us,pv):len(us)>1 and us[1] not in black_dir)
sites_pv = sites.map(lambda (us,pv):(us[1],pv)).reduceByKey(lambda x,y:x+y).collectAsMap()

widgets = sitelog.filter(lambda (u,c):u.startswith('/widget/'))
widgets_pv = widgets.map(lambda (u,c):(':'.join(u.split('/')[3:4]),c)).reduceByKey(lambda x,y:x+y).collectAsMap()

def parse_city(u):
    if not u.startswith('/movie/subject/'):
        return []
    us = u.split('/')
    if len(us) < 7: return []
    if us[4] != 'cinema': return []
    return [(us[5], 1)]

cinema_pv = alllog.flatMap(parse_city).reduceByKey(lambda x,y:x+y).collectAsMap()
#print cinema_pv

bid_sites = ['111097']
widget_map = {
'111097':['2364034',
     '346183',
  '346167',
   '2122545',
    '2637708',
     '346320',
      '3721513',
       '346173'],
'Microsoft': [
 '5300949',
 '5267684',
 '5264606',
 '5260479',
 '5260463',
 '5260448',
 '5260430',
 '5260417',
 '5165447',
 '5270612',
 '5270594',
 '5265086',
 '5265028',
 '5265006'],
}

for uid in bid_sites:
    pv = sites_pv.get(uid, 0)
    print uid, 'pv', pv 

    for wid in widget_map.get(uid, []):
        wpv = widgets_pv.get(wid, 0)
        pv += wpv
        print uid, wid, 'pv', wpv

    print 'city', uid, cinema_pv.get(uid)

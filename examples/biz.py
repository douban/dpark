
from dpark import DparkContext

dpark = DparkContext()

#path = '/tmp/weblog-20111019.csv.small'
path = '/tmp/weblog-20111019.csv.medium'

def format_url(u):
    return u

alllog = dpark.textFile(path).map(lambda line:line.split(',')).map(lambda line:(line[7],line[4]))
sitelog = alllog.filter(lambda (u,bid): u.startswith('/site/')).map(lambda (u,bid):(u[5:],bid)).groupByKey()

black_dir = ('widget','censor', 'j', 'cart', 'invite', 'about', 'tos', 'blank', 'apply')

sites = sitelog.map(lambda (u,bids):(u.split('/'),bids)).filter(lambda (us,bids):len(us)>1 and us[1] not in black_dir)
sites = sites.map(lambda (us,bids):(us[1],bids)).reduceByKey(lambda x,y:x+y).collectAsMap()
#for s in sites:
#    print s, len(sites)

widgets = sitelog.filter(lambda (u,c):u.startswith('/widget/'))
widgets = widgets.map(lambda (u,c):(':'.join(u.split('/')[3:4]),c)).reduceByKey(lambda x,y:x+y).collectAsMap()
#for w in widgets:
#    print w, len(widgets[w])

bid_sites = ['Microsoft']
widget_map = {
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
    pv = len(sites.get(uid, []))
    bids = set(sites.get(uid, []))
    print uid, 'pv', pv, 'uv', len(bids) 
    for wid in widget_map.get(uid, []):
        bid = widgets.get(wid, [])
        pv += len(bid)
        bids |= set(bid)
    print uid, 'pv', pv, 'uv', len(bids) 

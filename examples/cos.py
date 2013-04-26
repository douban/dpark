import sys
sys.path.append('../')
import logging
import dpark

name = 'rating.txt'

def parse(line):
    sid, uid, r, f = line.strip().split('\t')
    defaults = {'F':4.5, 'P':3.7, 'N':4.0}
    if r == 'None':
        r = defaults[f]
    return (sid, (uid, float(r)))
rating = dpark.textFile(name, numSplits=2).map(parse).groupByKey(2)#.cache()
#print 'us', rating.first()
print rating.count()

def reverse(it):
    s = {}
    for k, us in it:
        for u,r in us:
            s.setdefault(u, {})[k] = r
    return s

def vsum(a, b):
#    return 1
    if len(a) < len(b):
        a, b = b, a
    d = dict(a)
    s = 0
    for u,r in b:
        s += r * d.get(u, 0)
    return s

# should replace this function with c extension for best performance.
def cos((l1, l2)):
    l1 = list(l1)
    l2 = list(l2)
    d2 = dict(l2)
    u2 = reverse(l2)
    for sid1, us1 in l1:
        s = {}
        for u,r in us1:
            if u in u2:
                for sid2 in u2[u]:
                    if sid2 not in s:
                        s[sid2] = (vsum(us1, d2[sid2]),sid2)
        sim = sorted(s.values(), reverse=True)[:5]
#        sim = sorted([(vsum(us1, us2),sid2) for sid2,us2 in l2
#            if sid2!=sid1], reverse=True)[:5]
        yield sid1, sim

final = rating.glom().cartesian(rating.glom())
print final.count()
final = final.flatMap(cos)
#print 'sim', final.first()
final = final.reduceByKey(lambda x,y:x+y).mapValue(lambda x:sorted(x,reverse=True)[:5])
print 'final',final.count()

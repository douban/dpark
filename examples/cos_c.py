from __future__ import absolute_import
from __future__ import print_function
import logging
from context import SparkContext
import gc

gc.disable()

spark = SparkContext()

name = 'rating.txt'
name = 'rating.txt.medium'
name = 'rating.txt.large'


def parse(line):
    try:
        sid, uid, r, f = line.split('\t')
        defaults = {'F': 4.5, 'P': 3.7, 'N': 4.0}
        if r == 'None':
            r = defaults.get(f, 0)
        return (int(sid), (int(uid), float(r)))
    except Exception:
        return (int(sid), (int(uid), 0))


rating = spark.textFile(name, numSplits=32).map(parse).groupByKey(16).filter(lambda x_y: len(x_y[1]) > 10)  # .cache()


# print 'us', rating.first()

def convert(it):
    s = {}
    for k, us in it:
        for u, r in us:
            s.setdefault(k, {})[u] = r
    return s


def cos(l1_l2):
    (l1, l2) = l1_l2
    import map_sim
    r = map_sim.map_sim([], convert(l1), convert(l2), 10)
    for k in r:
        yield k, r[k]


final = rating.glom().cartesion(rating.glom()).flatMap(cos)
# print 'sim', final.first()
final = final.reduceByKey(lambda x, y: x + y).mapValue(lambda x: sorted(x, reverse=True)[:5])
print('final', final.count())

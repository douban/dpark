import os, sys
import re
import logging
from collections import namedtuple
import itertools

import msgpack

from rdd import RDD
from dependency import OneToOneDependency

class DerivedRDD(RDD):
    def __init__(self, rdd):
        RDD.__init__(self, rdd.ctx)
        self.prev = rdd
        self.dependencies = [OneToOneDependency(rdd)]

    def __len__(self):
        return len(self.prev)

    @property
    def splits(self):
        return self.prev.splits

    def _preferredLocations(self, split):
        return self.prev.preferredLocations(split)


class TableRDD(DerivedRDD):
    def __init__(self, rdd, fields):
        DerivedRDD.__init__(self, rdd)
        self.name = 'Row%d' % self.id
        self.fields = fields

    def __str__(self):
        return '<Table(%s) from %s>' % (','.join(self.fields), self.prev)

    def iterator(self, split):
        cls = namedtuple(self.name, self.fields)
        def f(v):
            return cls(*v)
        return itertools.imap(f, RDD.iterator(self, split))

    def compute(self, split):
        return self.prev.iterator(split)

    def _create_selector(self, e):
        if callable(e): 
            return e
        for i,n in enumerate(self.fields):
            e = re.sub(r'%s'%n, '_v[%d]' % i, e)
        return eval('lambda _v:' + e, globals())

    def select(self, *fields, **named_fields):
        def parser_name(e):
            return re.sub(r'[(, )+*/\-]', '_', e).strip('_') 
        new_fields = [parser_name(e) for e in fields] + named_fields.keys()
        if len(set(new_fields)) != len(new_fields):
            raise Exception("dupicated fields: " + (','.join(new_fields)))
        
        selector = [self._create_selector(e) for e in fields] + \
            [self._create_selector(named_fields[n]) for n in new_fields[len(fields):]]
        def _select(v):
            return tuple(s(v) for s in selector)

        need_attr = any(callable(f) for f in named_fields.values())
        return (need_attr and self or self.prev).map(_select).asTable(new_fields)

    def where(self, *conditions):
        need_attr = any(callable(f) for f in conditions)
        conditions = [self._create_selector(c) for c in conditions]
        def _filter(v):
            return all(c(v) for c in conditions)
        return (need_attr and self or self.prev).filter(_filter).asTable(self.fields)

    def groupBy(self, *fields, **kw):
        keys = [self.fields.index(n) for n in fields]
        others = [i for i in range(len(self.fields)) 
            if self.fields[i] not in fields]
        def gen_key(v):
            return tuple(v[k] for k in keys)
        def gen_others(v):
            return tuple(v[k] for k in others)
        
        numSplits = kw.get('numSplits', None)
        g = self.prev.map(lambda v:(gen_key(v), gen_others(v))).groupByKey(numSplits)

        def swap(ll):
            r = [[] for i in range(len(ll[0]))]
            for l in ll:
                for i,v in enumerate(l):
                    r[i].append(v)
            return r

        new_fields = [self.fields[i] for i in keys+others]
        return g.map(lambda (k,v): tuple(list(k)+swap(v))).asTable(new_fields)

    def sort(self, fields, reverse=False, numSplits=None):
        if isinstance(fields, str):
            keys = [self.fields.index(fields)]
        else:
            keys = [self.fields.index(n) for n in fields]
        def key(v):
            return tuple(v[i] for i in keys)
        return self.prev.sort(key, reverse, numSplits).asTable(self.fields)

    def top(self, n, fields, reverse=False):
        keys = [self.fields.index(i) for i in fields]
        def key(v):
            return tuple(v[i] for i in keys)
        cls = namedtuple(self.name, self.fields)
        return [cls(*i) for i in self.prev.top(n, key=key, reverse=reverse)]

    def collect(self):
        cls = namedtuple(self.name, self.fields)
        return [cls(*i) for i in self.prev.collect()]

    def take(self, n):
        cls = namedtuple(self.name, self.fields)
        return [cls(*i) for i in self.prev.take(n)]

    def execute(self, sql):
        parts = [i.strip() for i in re.compile(r'(select|from|where|group by|order by|limit) ', re.I).split(sql)[1:]]
        kw = dict(zip([i.lower() for i in parts[::2]], parts[1::2]))
        
        # select needed cols 
        r = self.select(*[n for n in self.fields if n in sql])

        if 'where' in kw:
            r = r.where(kw['where'])

        if 'group by' in kw:
            r = r.groupBy(*[n.strip() for n in kw['group by'].split(',')])

        if 'select' in kw:
            r = r.select(*[n.strip() for n in kw['select'].split(',')])

        if 'order by' in kw:
            keys = kw['order by']
            reverse = False
            if keys.endswith(' desc'):
                reverse = True
                keys = keys[:-5].strip()
            keys = [n.strip() for n in keys.split(',')]
            if 'limit' in kw:
                return r.top(int(kw['limit']), keys, reverse)
            r = r.sort(keys, reverse)

        if 'limit' in kw:
            return r.take(int(kw['limit']))
        else:
            return r.collect()

def test():
    from context import DparkContext
    ctx = DparkContext()
    rdd = ctx.makeRDD(zip(range(1000), range(1000)))
    table = rdd.asTable(['f1', 'f2']) 
    print table.select('f1', 'f2').where('f1>10', 'f2<80', 'f1+f2>30 or f1*f2>200').groupBy('f1').select("-f1", f2="sum(f2)").sort('f1', reverse=True).take(5)
    print table.execute('select -f1, sum(f2) from me where f1>10 and f2<80 and (f1+f2>30 or f1*f2>200) group by f1 order by f1 limit 5')
    #where(lambda x:x.f1>10, lambda v:v.f2<80
    #    ).groupBy('f1').select(f1=lambda x:-x, f2=sum).sort('f1').take(5)

if __name__ == '__main__':
    test()

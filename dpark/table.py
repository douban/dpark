import os, sys
import re
import logging
from collections import namedtuple
import itertools

import msgpack

from rdd import DerivedRDD
from dependency import Aggregator, OneToOneDependency

Aggs = {
    'sum': lambda x,y: x+y,
}

def table_join(f):
    def _join(self, other, left_keys=None, right_keys=None):
        if not left_keys:
            left_keys = [n for n in self.fields if n in other.fields]
        assert left_keys, 'need field names to join'
        if right_keys is None:
            right_keys = left_keys
        
        joined = f(self, other, left_keys, right_keys)

        ln = [n for n in self.fields if n not in left_keys]
        rn = [n for n in other.fields if n not in left_keys]
        return joined.map(lambda (k,(v1,v2)): list(k)+v1+v2
            ).asTable(left_keys + ln + rn)

    return _join

class TableRDD(DerivedRDD):
    def __init__(self, rdd, fields):
        DerivedRDD.__init__(self, rdd)
        self.name = 'Row%d' % self.id
        if isinstance(fields, str):
            fields = [n.strip() for n in fields.split(',')]
        self.fields = fields

    def __str__(self):
        return '<Table(%s) from %s>' % (','.join(self.fields), self.prev)

    def iterator(self, split):
        cls = namedtuple(self.name, self.fields)
        return itertools.imap(lambda x:cls(*x), super(TableRDD, self).iterator(split))

    def compute(self, split):
        return self.prev.iterator(split)

    def _create_expression(self, e):
        if callable(e): 
            return e
        e = re.compile(r' as (\w+)', re.I).split(e)[0]
        for i,n in enumerate(self.fields):
            e = re.sub(r'%s'%n, '_v[%d]' % i, e)
        return e

    def _create_field_name(self, e):
        asp = re.compile(r' as (\w+)', re.I)
        m = asp.search(e)
        if m: return m.group(1)
        return re.sub(r'[(, )+*/\-]', '_', e).strip('_') 

    def select(self, *fields, **named_fields):
        new_fields = [self._create_field_name(e) for e in fields] + named_fields.keys()
        if len(set(new_fields)) != len(new_fields):
            raise Exception("dupicated fields: " + (','.join(new_fields)))
        
        selector = [self._create_expression(e) for e in fields] + \
            [self._create_expression(named_fields[n]) for n in new_fields[len(fields):]]
        _select = eval('lambda _v:(%s,)' % (','.join(e for e in selector)), globals())    

        need_attr = any(callable(f) for f in named_fields.values())
        return (need_attr and self or self.prev).map(_select).asTable(new_fields)

    def _create_reducer(self, index, e):
        """ -> creater, merger, combiner"""
        if '(' not in e:
            e = '(%s)' % e

        func_name, args = e.split('(', 1)
        if func_name == 'count':
            return '1', '_x[%d] + 1' % index, '_x[%d] + _y[%d]' % (index, index)

        args = args.rsplit(')', 1)[0]
        for i,n in enumerate(self.fields):
            args = re.sub(n, '_v[%d]' % i, args)
        
        if func_name:
            return (args, '%s(_x[%d],%s)' % (func_name, index, args),
                '%s(_x[%d],_y[%d])' % (func_name, index, index))
        else: # group by
            return ('[%s]' % args, '_x[%d].append(%s) or _x[%d]' % (index, args, index), 
                    '_x[%d] + _y[%d]' % (index, index))

    def selectOne(self, *fields, **named_fields):
        new_fields = [self._create_field_name(e) for e in fields] + named_fields.keys()
        if len(set(new_fields)) != len(new_fields):
            raise Exception("dupicated fields: " + (','.join(new_fields)))
        
        codes = ([self._create_reducer(i, e) for i,e in enumerate(fields)]
              + [self._create_reducer(i + len(fields), named_fields[n]) 
                    for i,n in enumerate(new_fields[len(fields):])])
        
        d = dict(globals())
        d.update(Aggs)
        creater = eval('lambda _v:(%s,)' % (','.join(c[0] for c in codes)), d)
        merger = eval('lambda _x, _v:(%s,)' % (','.join(c[1] for c in codes)), d)
        combiner = eval('lambda _x, _y:(%s,)' % (','.join(c[2] for c in codes)), d)

        def reducePartition(it):
            r = None
            iter(it)
            for i in it:
                if r is None:
                    r = creater(i)
                else:
                    r = merger(r, i)
            return r
        rs = self.ctx.runJob(self.prev, reducePartition)
        return reduce(combiner, (x for x in rs if x is not None)) 

    def where(self, *conditions):
        need_attr = any(callable(f) for f in conditions)
        conditions = ' and '.join(['(%s)' % self._create_expression(c) for c in conditions])
        _filter = eval('lambda _v: %s' % conditions, globals())
        return (need_attr and self or self.prev).filter(_filter).asTable(self.fields)

    def groupBy(self, *keys, **kw):
        numSplits = kw.pop('numSplits', None)
        
        key_names = [self._create_field_name(e) for e in keys] 
        expr = ','.join(self._create_expression(e) for e in keys)
        gen_key = eval('lambda _v:(%s,)' % expr, globals())

        if not kw:
            kw.update((k,k) for k in self.fields if k not in keys)
        values = kw.keys()
        codes = [self._create_reducer(i, kw[n]) for i,n in enumerate(values)]
        d = dict(globals())
        d.update(Aggs)
        creater = eval('lambda _v:(%s,)' % (','.join(c[0] for c in codes)), d)
        merger = eval('lambda _x, _v:(%s,)' % (','.join(c[1] for c in codes)), d)
        combiner = eval('lambda _x, _y:(%s,)' % (','.join(c[2] for c in codes)), d)

        agg = Aggregator(creater, merger, combiner) 
        g = self.prev.map(lambda v:(gen_key(v), v)).combineByKey(agg, numSplits)
        return g.map(lambda (k,v): list(k)+list(v)).asTable(key_names + values)

    def indexBy(self, keys=None):
        if keys is None:
            keys = self.fields[:1]
        if not isinstance(keys, (list, tuple)):
            keys = (keys,)
        
        def pick(keys, fields):
            ki = [fields.index(n) for n in keys]
            vi = [i for i in range(len(fields)) if fields[i] not in keys]
            def _(v):
                return tuple(v[i] for i in ki), [v[i] for i in vi]
            return _
        return self.prev.map(pick(keys, self.fields))

    @table_join
    def join(self, other, left_keys=None, right_keys=None):
        return self.indexBy(left_keys).join(other.indexBy(right_keys))

    @table_join
    def innerJoin(self, other, left_keys=None, right_keys=None):
        return self.indexBy(left_keys).innerJoin(other.indexBy(right_keys))

    @table_join
    def outerJoin(self, other, left_keys=None, right_keys=None):
        return self.indexBy(left_keys).outerJoin(other.indexBy(right_keys))

    @table_join
    def leftOutJoin(self, other, left_keys=None, right_keys=None):
        return self.indexBy(left_keys).leftOuterJoin(other.indexBy(right_keys))

    @table_join
    def rightOuterJoin(self, other, left_keys=None, right_keys=None):
        return self.indexBy(left_keys).rightOuterJoin(other.indexBy(right_keys))

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
            keys = [n.strip() for n in kw['group by'].split(',')]
            values = [n.strip() for n in kw['select'].split(',')]
            values = dict([(self._create_field_name(n), n) for n in values if n not in keys])
            r = r.groupBy(*keys, **values)

        elif 'select' in kw:
            cols = kw['select'].split(',')
            r = r.select(*cols)

        if 'order by' in kw:
            keys = kw['order by']
            reverse = False
            if keys.endswith(' desc'):
                reverse = True
                keys = keys[:-5].strip()
            keys = [n.strip() for n in keys.split(',')]
            if 'limit' in kw:
                return r.top(int(kw['limit']), keys, not reverse)
            r = r.sort(keys, reverse)

        if 'limit' in kw:
            return r.take(int(kw['limit']))
        return r


def test():
    from context import DparkContext
    ctx = DparkContext()
    rdd = ctx.makeRDD(zip(range(1000), range(1000)))
    table = rdd.asTable(['f1', 'f2']) 
    print table.select('f1', 'f2').where('f1>10', 'f2<80', 'f1+f2>30 or f1*f2>200').groupBy('f1').select("-f1", f2="sum(f2)").sort('f1', reverse=True).take(5)
    print table.selectOne('count(*)', 'max(f1)', 'min(f2+f1)', 'sum(f1*f2+f1)')
    print table.groupBy('f1/20', f2s='sum(f2)', fcnt='count(*)').take(5)
    print table.execute('select f1, sum(f2), count(*) as cnt from me where f1>10 and f2<80 and (f1+f2>30 or f1*f2>200) group by f1 order by cnt limit 5')
    table2 = rdd.asTable(['f1', 'f3'])
    print table.innerJoin(table2).take(10)
    print table.join(table2).sort('f1').take(10)

if __name__ == '__main__':
    test()

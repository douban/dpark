import os, sys
import re
import logging
from collections import namedtuple
import itertools

import msgpack

from rdd import DerivedRDD, OutputTableFileRDD
from dependency import Aggregator, OneToOneDependency

try:
    from pyhll import HyperLogLog
except ImportError:    
    from hyperloglog import HyperLogLog

from hotcounter import HotCounter

SimpleAggs = {
    'sum': lambda x,y: x+y,
    'last': lambda x,y: y,
    'min': lambda x,y: x if x < y else y,
    'max': lambda x,y: x if x > y else y,
}

FullAggs = {
        'avg': (
            lambda v:(v, 1), 
            lambda (s,c), v: (s+v, c+1), 
            lambda (s1,c1), (s2,c2):(s1+s2, c1+c2),
            lambda (s,c): float(s)/c,
         ),
        'count': (
            lambda v: 1 if v is not None else 0,
            lambda s,v: s + (1 if v is not None else 0),
            lambda s1,s2: s1+s2,
            lambda s: s
         ),
        'adcount': (
            lambda v: HyperLogLog([v]),
            lambda s,v: s.add(v) or s,
            lambda s1,s2: s1.update(s2) or s1,
            lambda s: len(s)
        ),
        'group_concat': (
            lambda v: [v],
            lambda s,v: s.append(v) or s,
            lambda s1,s2: s1.extend(s2) or s1,
            lambda s: ','.join(s),
        ),
        'top': (
            lambda v: HotCounter([v], 20),
            lambda s,v: s.add(v) or s,
            lambda s1,s2: s1.update(s2) or s1,
            lambda s: s.top(20)
        ),
}

Aggs = dict(SimpleAggs)
Aggs.update(FullAggs)

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

    def _replace_attr(self, e):
        es = re.split(r'([()\-+ */,]+)', e)
        for i in range(len(es)):
            if es[i] in self.fields:
                es[i] = '_v[%d]' % self.fields.index(es[i])
        return ''.join(es) 

    def _create_expression(self, e):
        if callable(e): 
            return e
        e = re.compile(r' as (\w+)', re.I).split(e)[0]
        return self._replace_attr(e)

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
        func_name = func_name.strip()
        args = self._replace_attr(args.rsplit(')', 1)[0])
        if args == '*':
            args = '1'
        ag = '_x[%d]' % index
        if func_name in SimpleAggs:
            return (args, '%s(%s,%s)' % (func_name, ag, args),
                '%s(_x[%d],_y[%d])' % (func_name, index, index), ag)
        elif func_name in FullAggs:
            return ('%s[0](%s)' % (func_name, args),
                    '%s[1](%s, %s)' % (func_name, ag, args),
                    '%s[2](_x[%d], _y[%d])' % (func_name, index, index),
                    '%s[3](_x[%d])' % (func_name, index),)
        elif func_name:    
            raise Exception("invalid aggregator function: %s" % func_name)
        else: # group by
            return ('[%s]' % args, '_x[%d].append(%s) or _x[%d]' % (index, args, index), 
                    '_x[%d] + _y[%d]' % (index, index), ag)

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
        mapper = eval('lambda _x:(%s,)' % ','.join(c[3] for c in codes), d)

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
        return mapper(reduce(combiner, (x for x in rs if x is not None)))

    def atop(self, field):
        return self.selectOne('top(%s)' % field)[0]

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
        mapper = eval('lambda _x:(%s,)' % ','.join(c[3] for c in codes), d)

        agg = Aggregator(creater, merger, combiner) 
        g = self.prev.map(lambda v:(gen_key(v), v)).combineByKey(agg, numSplits)
        return g.map(lambda (k,v): k + mapper(v)).asTable(key_names + values)

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
        return self.prev.top(n, key=key, reverse=reverse)

    def collect(self):
        return self.prev.collect()

    def take(self, n):
        return self.prev.take(n)

    def execute(self, sql):
        parts = [i.strip() for i in re.compile(r'(select|from|(?:inner|left outer)? join|where|group by|having|order by|limit) ', re.I).split(sql)[1:]]
        kw = dict(zip([i.lower() for i in parts[::2]], parts[1::2]))
        
        # select needed cols 
        r = self.select(*[n for n in self.fields if n in sql])

        if 'where' in kw:
            r = r.where(kw['where'])

        if 'top(' in kw['select']:
            field = re.match(r'top\((.*?)\)', kw['select']).group(1)
            r = r.atop(field)
            values = [n.strip() for n in kw['select'].split(',')]
            if len(values) == 1:
                return [k for k,c in r]
            elif values[0].startswith('top'):
                return r
            else:
                return [(c,k) for k,c in r]
            
        elif 'group by' in kw:
            keys = [n.strip() for n in kw['group by'].split(',')]
            values = [n.strip() for n in kw['select'].split(',')]
            values = dict([(self._create_field_name(n), n) for n in values if n not in keys])
            r = r.groupBy(*keys, **values)

        elif 'select' in kw:
            cols = kw['select'].split(',')
            for name in Aggs:
                if (name + '(') in kw['select']:
                    return r.selectOne(*cols)
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

    def save(self, path, overwrite=True, compress=True):
        r = OutputTableFileRDD(self.prev, path,
            overwrite=overwrite, compress=compress).collect()
        open(os.path.join(path, '.field_names'), 'w').write('\t'.join(self.fields))
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
    print table.selectOne('adcount(f1)', 'adcount(f2*10)')

if __name__ == '__main__':
    test()

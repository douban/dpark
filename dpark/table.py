import os, sys
import re
import logging
from collections import namedtuple
import itertools

import msgpack

from dpark.rdd import DerivedRDD, OutputTableFileRDD
from dpark.dependency import Aggregator, OneToOneDependency

try:
    from pyhll import HyperLogLog
except ImportError:
    from dpark.hyperloglog import HyperLogLog

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

Globals = {}
Globals.update(Aggs)
import math
Globals.update(math.__dict__)

__eval = eval
def eval(code, g={}, l={}):
    return __eval(code, g or Globals, l)

def table_join(f):
    def _join(self, other, left_keys=None, right_keys=None):
        if not left_keys:
            left_keys = [n for n in self.fields if n in other.fields]
        assert left_keys, 'need field names to join'
        if right_keys is None:
            right_keys = left_keys

        joined = f(self, other, left_keys, right_keys)

        ln = [n for n in self.fields if n not in left_keys]
        rn = [n for n in other.fields if n not in right_keys]
        def conv((k, (v1, v2))):
            return list(k) + (v1 or [None]*len(ln)) + (v2 or [None]*len(rn))
        return joined.map(conv).asTable(left_keys + ln + rn, self.name)

    return _join

class TableRDD(DerivedRDD):
    def __init__(self, rdd, fields, name='', field_types=None):
        DerivedRDD.__init__(self, rdd)
        self.name = name
        if isinstance(fields, str):
            fields = [n.strip() for n in fields.split(',')]
        self.fields = fields
        self.field_types = field_types
        self._repr = '<Table(%s) from %s>' % (','.join(fields), rdd)

    def iterator(self, split):
        cls = namedtuple(self.name or ('Row%d' % self.id), self.fields)
        return itertools.imap(lambda x:cls(*x), super(TableRDD, self).iterator(split))

    def compute(self, split):
        return self.prev.iterator(split)

    def _split_expr(self, expr):
        rs = []
        last, n = [], 0
        for part in expr.split(','):
            last.append(part)
            n += part.count('(') - part.count(')')
            if n == 0:
                rs.append(','.join(last))
                last = []
                n = 0
        assert not last, '() not match in expr'
        return rs

    def _replace_attr(self, e):
        es = re.split(r'([()\-+ */%,><=.\[\]]+)', e)
        named_fields = ['%s.%s' % (self.name, f) for f in self.fields]
        for i in range(len(es)):
            if es[i] in self.fields:
                es[i] = '_v[%d]' % self.fields.index(es[i])
            elif es[i] in named_fields:
                es[i] = '_v[%d]' % named_fields.index(es[i])
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
        if len(fields) == 1 and not named_fields and fields[0] == '*':
            fields = self.fields
        new_fields = [self._create_field_name(e) for e in fields] + named_fields.keys()
        if len(set(new_fields)) != len(new_fields):
            raise Exception("dupicated fields: " + (','.join(new_fields)))

        selector = [self._create_expression(e) for e in fields] + \
            [self._create_expression(named_fields[n]) for n in new_fields[len(fields):]]
        _select = eval('lambda _v:(%s,)' % (','.join(e for e in selector)))

        need_attr = any(callable(f) for f in named_fields.values())
        return (need_attr and self or self.prev).map(_select).asTable(new_fields, self.name)

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

        creater = eval('lambda _v:(%s,)' % (','.join(c[0] for c in codes)))
        merger = eval('lambda _x, _v:(%s,)' % (','.join(c[1] for c in codes)))
        combiner = eval('lambda _x, _y:(%s,)' % (','.join(c[2] for c in codes)))
        mapper = eval('lambda _x:(%s,)' % ','.join(c[3] for c in codes))

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
        return [mapper(reduce(combiner, (x for x in rs if x is not None)))]

    def atop(self, field):
        return self.selectOne('top(%s)' % field)[0][0]

    def where(self, *conditions):
        need_attr = any(callable(f) for f in conditions)
        conditions = ' and '.join(['(%s)' % self._create_expression(c) for c in conditions])
        _filter = eval('lambda _v: %s' % conditions)
        return (need_attr and self or self.prev).filter(_filter).asTable(self.fields, self.name)

    def groupBy(self, keys, *fields, **kw):
        numSplits = kw.pop('numSplits', None)

        if not isinstance(keys, (list, tuple)):
            keys = [keys]
        key_names = [self._create_field_name(e) for e in keys]
        expr = ','.join(self._create_expression(e) for e in keys)
        gen_key = eval('lambda _v:(%s,)' % expr)

        values = [self._create_field_name(e) for e in fields] + kw.keys()
        kw.update((values[i], fields[i]) for i in range(len(fields)))
        codes = [self._create_reducer(i, kw[n]) for i,n in enumerate(values)]
        creater = eval('lambda _v:(%s,)' % (','.join(c[0] for c in codes)))
        merger = eval('lambda _x, _v:(%s,)' % (','.join(c[1] for c in codes)))
        combiner = eval('lambda _x, _y:(%s,)' % (','.join(c[2] for c in codes)))
        mapper = eval('lambda _x:(%s,)' % ','.join(c[3] for c in codes))

        agg = Aggregator(creater, merger, combiner)
        g = self.prev.map(lambda v:(gen_key(v), v)).combineByKey(agg, numSplits)
        return g.map(lambda (k,v): k + mapper(v)).asTable(key_names + values, self.name)

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
    def leftOuterJoin(self, other, left_keys=None, right_keys=None):
        o = other.indexBy(right_keys).collectAsMap()
        r = self.indexBy(left_keys).map(lambda (k,v):(k,(v,o.get(k))))
        r.mem += (sys.getsizeof(o) * 10) >> 20 # memory used by broadcast obj
        return r

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

        if len(self) <= 16: # maybe grouped
            data = sorted(self.prev.collect(), key=key, reverse=reverse)
            return self.ctx.makeRDD(data).asTable(self.fields, self.name)

        return self.prev.sort(key, reverse, numSplits).asTable(self.fields, self.name)

    def top(self, n, fields, reverse=False):
        keys = [self.fields.index(i) for i in fields]
        def key(v):
            return tuple(v[i] for i in keys)
        return self.prev.top(n, key=key, reverse=reverse)

    def collect(self):
        return self.prev.collect()

    def take(self, n):
        return self.prev.take(n)

    def execute(self, sql, asTable=False):
        sql_p = re.compile(r'(select|from|(?:inner|left outer)? join(?: each)?|where|group by|having|order by|limit) ', re.I)
        parts = [i.strip() for i in sql_p.split(sql)[1:]]
        kw = dict(zip([i.lower() for i in parts[::2]], parts[1::2]))

        for type in kw:
            if 'join' not in type:
                continue

            table, cond = re.split(r' on ', kw[type], re.I)
            if '(' in table:
                last_index = table.rindex(')')
                name = table[last_index + 1:].split(' ')[-1]
                expr = table[:last_index + 1]
            else:
                name = table.strip()
                expr = ''
            other = create_table(self.ctx, name, expr)

            left_keys, right_keys = [], []
            for expr in re.split(r' and ', cond, re.I):
                lf, rf = re.split(r'=+', expr)
                if lf.startswith(name + '.'):
                    lf, rf = rf, lf
                left_keys.append(lf[lf.rindex('.') + 1:].strip())
                right_keys.append(rf[rf.rindex('.') + 1:].strip())
            if 'left outer' in type:
                r = self.leftOuterJoin(other, left_keys, right_keys)
            else:
                r = self.innerJoin(other, left_keys, right_keys)
            # remove table prefix in colnames
            for n in (self.name, name):
                if not n: continue
                p = re.compile(r'(%s\.)(?=[\w])' % n)
                for k in ('select', 'where', 'group by', 'having', 'order by'):
                    if k in kw:
                        kw[k] = p.sub('', kw[k])
            break
        else:
            r = self

        # select needed cols
        #r = self.select(*[n for n in self.fields if n in sql])
        cols = [n.strip() for n in self._split_expr(kw['select'])]

        if 'where' in kw:
            r = r.where(kw['where'])

        if 'top(' in kw['select']:
            field = re.match(r'top\((.*?)\)', kw['select']).group(1)
            rs = r.atop(field)
            if asTable:
                rs = self.ctx.makeRDD(rs).asTable([field, 'count'], self.name)
            return rs

        elif 'group by' in kw:
            keys = [n.strip() for n in kw['group by'].split(',')]
            values = dict([(r._create_field_name(n), n) for n in cols if n not in keys])
            r = r.groupBy(keys, *cols)

            if 'having' in kw:
                r = r.where(kw['having'])

        elif 'select' in kw:
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
        if asTable:
            return r
        else:
            return r.collect()

    def save(self, path, overwrite=True, compress=True):
        r = OutputTableFileRDD(self.prev, path,
            overwrite=overwrite, compress=compress).collect()
        open(os.path.join(path, '.field_names'), 'w').write('\t'.join(self.fields))
        return r


CachedTables = {
}

def create_table(ctx, name, expr):
    if name in CachedTables:
        return CachedTables[name]

    assert expr, 'table %s is not defined' % name
    t = eval('ctx.' + expr, Globals, locals())
    if isinstance(t, TableRDD):
        t.name = name
        CachedTables[name] = t
        return t

    # TODO: try to find .fields

    # guess format
    row = t.first()
    if isinstance(row, str):
        if '\t' in row:
            t = t.fromCsv(dialet='excel-tab')
        elif ',' in row:
            t = t.fromCsv(dialet='excel')
        else:
            t = t.map(lambda line:line.strip().split(' '))

    # fake fields names
    row = t.first()
    fields = ['f%d' % i for i in range(len(row))]
    t = t.asTable(fields, name)
    CachedTables[name] = t
    return t


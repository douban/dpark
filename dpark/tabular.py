import os
import zlib
import types
import socket
import struct
import marshal
import cPickle
from lz4 import compress, decompress
from dpark.rdd import RDD, MultiSplit, TextFileRDD, Split, ParallelCollection, cached
from dpark.util import chain, atomic_file
from dpark.moosefs import walk
from dpark.bitindex import Bloomfilter, BitIndex
from dpark.serialize import dumps, loads
from dpark.dependency import OneToOneDependency, OneToRangeDependency

'''
Strip Format:
+---------------+-----------------+---------------+-------+---------------+------------------+
|  Header_Size  |     *Header     |   *Column_0   |  ...  |   *Column_n   |        \0        |
|      (4)      |  (Header_Size)  |  (Header[0])  |       |  (Header[n])  |  (Padding_Size)  |
+---------------+-----------------+---------------+-------+---------------+------------------+

Padding_Size = STRIPE_SIZE - Header_Size - sum(Header)

Footer Format:
+------------------+-----------------+---------------+----------------+
|     +Indices     |     +Fields     |  Fields_Size  |  Indices_Size  |
|  (Indices_Size)  |  (Fields_Size)  |      (4)      |      (4)       |
+------------------+-----------------+---------------+----------------+

*: lz4 + marshal
+: zlib + cPickle
'''

STRIPE_SIZE = 1 << 26
STRIPE_HEADER_SIZE = 1 << 15
STRIPE_DATA_SIZE = STRIPE_SIZE - STRIPE_HEADER_SIZE

BF_SIZE = 1 << 27
BF_HASHES = 3

MAX_BIT_INDEX_SIZE = 1 << 14

BITMAP_INDEX = 0
BLOOMFILTER_INDEX = 1

class NamedTuple(object):
    _fields = []
    _values = ()
    def __init__(self, fields, values):
        if isinstance(fields, types.StringTypes):
            fields = fields.replace(',', ' ').split()

        fields = list(fields)
        values = tuple(values)
        if len(set(fields)) != len(fields):
            raise ValueError('Duplicated field names!')

        if len(fields) != len(values):
            raise ValueError('Key/value length not match!')

        self._fields = fields
        self._values = values

    def __getattr__(self, key):
        if key in self._fields:
            return self._values[self._fields.index(key)]
        return getattr(self._values, key)

    def __repr__(self):
        return '<%s(%s)>' % (
            self.__class__.__name__,
            ', '.join('%s=%s' % (k, v) for k, v in zip(self._fields, self._values))
        )

    def __nonzero__(self):
        return bool(self._values)

    def __len__(self):
        return len(self._values)

    def __getitem__(self, key):
        return self._values[key]

    def __iter__(self):
        return iter(self._values)

    def __contains__(self, item):
        return item in self._values

class AdaptiveIndex(object):
    def __init__(self):
        self.index = {}
        self.index_type = BITMAP_INDEX

    def add(self, value, position):
        if self.index_type == BITMAP_INDEX and len(self.index) > MAX_BIT_INDEX_SIZE:
            index = self.index
            self.index_type = BLOOMFILTER_INDEX
            self.index = Bloomfilter(BF_SIZE, BF_HASHES)
            for v in index:
                for pos in index[v].positions():
                    self.index.add([(v, pos)])

        if self.index_type == BITMAP_INDEX:
            if value not in self.index:
                self.index[value] = BitIndex()
            index = self.index[value]
            index.set(position)

        if self.index_type == BLOOMFILTER_INDEX:
            self.index.add([(value, position)])
            assert (value, position) in self.index

    def get(self, value, pos):
        if self.index_type == BITMAP_INDEX:
            return self.index[value].get(pos) if value in self.index else False
        else:
            return (value, pos) in self.index

    def filter(self, fun):
        if self.index_type == BITMAP_INDEX:
            return chain(v.positions() for k, v in self.index.items() if fun(k))

class TabularSplit(Split):
    def __init__(self, index, rdd, sp):
        self.index = index
        self.rdd = rdd
        self.split = sp

class TabularRDD(RDD):
    def __init__(self, ctx, path, fields = None, taskMemory=None):
        RDD.__init__(self, ctx)
        if taskMemory:
            self.mem = taskMemory

        if isinstance(path, basestring):
            files = self._get_files(path)
        else:
            files = chain(self._get_files(p) for p in path)

        rdds = [TabularFileRDD(ctx, f, fields) for f in files]
        self._splits = []
        i = 0
        for rdd in rdds:
            for sp in rdd.splits:
                self._splits.append(TabularSplit(i, rdd, sp))
                i += 1
        self._dependencies = [OneToOneDependency(rdd) for rdd in rdds]
        self.repr_name = '<%s %d %s...>' % (self.__class__.__name__, len(rdds),
                                  ','.join(str(rdd) for rdd in rdds[:1]))
        self._preferred_locs = {}
        for split in self._splits:
            self._preferred_locs[split] = split.rdd.preferredLocations(split.split)


    def _get_files(self, path):
        path = os.path.realpath(path)
        if os.path.isdir(path):
            for root,dirs,names in walk(path):
                for n in sorted(names):
                    if not n.startswith('.'):
                        yield os.path.join(root, n)

        else:
            yield path

    def compute(self, split):
        return split.rdd.iterator(split.split)

    def filterByIndex(self, **kw):
        return FilteredByIndexRDD(self, kw)

class FilteredByIndexRDD(RDD):
    def __init__(self, rdd, filters):
        RDD.__init__(self, rdd.ctx)
        self.rdd = rdd
        self.filters = filters
        self.mem = max(self.mem, rdd.mem)
        self._dependencies = [OneToOneDependency(rdd)]
        self._splits = self._get_splits()
        self.repr_name = '<%s %s>' % (self.__class__.__name__, rdd)
        self._preferred_locs = {}
        for split in self._splits:
            self._preferred_locs[split] = rdd.preferredLocations(split)

    def _clear_dependencies(self):
        RDD._clear_dependencies(self)
        self.rdd = None

    @cached
    def __getstate__(self):
        d = RDD.__getstate__(self)
        del d['filters']
        return d, dumps(self.filters)

    def __setstate__(self, state):
        self.__dict__, filters = state
        self.filters = loads(filters)

    def _get_splits(self):
        filters = self.filters
        def _filter(v):
            path, ids = v
            result_set = set(ids)
            with open(path, 'rb') as f:
                f.seek(-8, 2)
                footer_fields_size, footer_indices_size = struct.unpack('II', f.read(8))
                f.seek(-8 -footer_fields_size -footer_indices_size, 2)
                indices = cPickle.loads(zlib.decompress(f.read(footer_indices_size)))
                _fields = marshal.loads(decompress(f.read(footer_fields_size)))
                for k, v in filters.iteritems():
                    result = set()
                    if k not in _fields:
                        raise RuntimeError('%s is not in fields!' % k)

                    if k not in indices:
                        raise RuntimeError('%s is not indexed' % k)

                    index = indices[k]
                    if isinstance(v, types.FunctionType):
                        r = index.filter(v)
                        if r is not None:
                            result = set(r)
                        else:
                            result = result_set
                    else:
                        if not isinstance(v, list):
                            v = [v]

                        for vv in v:
                            for _id in result_set:
                                if index.get(vv, _id):
                                    result.add(_id)
            return path, result

        sp_dict = {}
        for sp in self.rdd.splits:
            path = sp.rdd.path
            _id = sp.split.begin / STRIPE_SIZE
            if path not in sp_dict:
                sp_dict[path] = [_id]
            else:
                sp_dict[path].append(_id)

        rdd = ParallelCollection(self.ctx, sp_dict.items(), len(sp_dict))
        path_ids_filter = dict(rdd.map(_filter).collect())

        splits = []
        for sp in self.rdd.splits:
            path = sp.rdd.path
            _id = sp.split.begin / STRIPE_SIZE
            if _id in path_ids_filter[path]:
                splits.append(sp)
        return splits


    def compute(self, split):
        for t in self.rdd.iterator(split):
            for k, v in self.filters.iteritems():
                value = getattr(t, k)
                if isinstance(v, types.FunctionType):
                    if not v(value):
                        break

                else:
                    if not isinstance(v, list):
                        v = [v]

                    if value not in v:
                        break
            else:
                yield t

class TabularFileRDD(TextFileRDD):
    def __init__(self, ctx, path, fields = None):
        TextFileRDD.__init__(self, ctx, path, splitSize = STRIPE_SIZE)
        if isinstance(fields, basestring):
            fields = fields.replace(',', ' ').split()

        self.fields = map(str, fields) if fields is not None else None

    def compute(self, split):
        with self.open_file() as f:
            f.seek(-8,2)
            footer_fields_size, footer_indices_size = struct.unpack('II', f.read(8))
            footer_offset = self.size - 8 - footer_fields_size - footer_indices_size
            footer_fields_offset = self.size - 8 - footer_fields_size
            if split.begin >= footer_offset:
                return

            start = split.begin
            end = min(split.end, footer_offset)
            stripe_id = start / STRIPE_SIZE
            f.seek(footer_fields_offset)
            _fields = marshal.loads(decompress(f.read(footer_fields_size)))

            if self.fields is None:
                field_ids = range(len(_fields))
                field_names = _fields
            else:
                field_names = []
                field_ids = [None] * len(_fields)
                for i, v in enumerate(self.fields):
                    if v in _fields:
                        index = _fields.index(v)
                        field_ids[index] = i
                        field_names.append(v)
                    else:
                        raise RuntimeError('Unknown field: %s' % v)

            f.seek(start)
            header_size, = struct.unpack('I', f.read(4))
            header = marshal.loads(decompress(f.read(header_size)))
            content = [None] * len(field_names)
            for id, size in enumerate(header):
                index = field_ids[id]
                if index is not None:
                    content[index] = marshal.loads(decompress(f.read(size)))
                else:
                    f.seek(size, 1)

            for r in zip(*content):
                yield NamedTuple(field_names, r)

class OutputTabularRDD(RDD):
    def __init__(self, rdd, path, field_names, indices=None, numSplits=None):
        RDD.__init__(self, rdd.ctx)
        self.prev = rdd
        self.mem = rdd.mem + 600
        self.path = path
        if os.path.exists(path):
            raise RuntimeError('path already exists: %s' % path)

        os.makedirs(path)
        if isinstance(field_names, basestring):
            field_names = field_names.replace(',', ' ').split()

        if len(set(field_names)) != len(field_names):
            raise ValueError('duplicated field names')

        self.fields = map(str, field_names)
        if isinstance(indices, types.StringTypes):
            indices = indices.replace(',', ' ').split()

        self.indices = set()
        if indices:
            for i in indices:
                i = str(i)
                if i not in self.fields:
                    raise ValueError('index field %s not in field list' % i)

                self.indices.add(i)

        prev_splits = len(rdd)
        numSplits = min(numSplits or prev_splits, prev_splits)
        self.numSplits = min(numSplits, prev_splits)
        s = [int(round(1.0*prev_splits/numSplits*i)) for i in xrange(numSplits + 1)]
        self._splits = [MultiSplit(i, rdd.splits[s[i]:s[i+1]]) for i in xrange(numSplits)]
        self._dependencies = [OneToRangeDependency(rdd, int(prev_splits/numSplits),
                                                  prev_splits)]
        self.repr_name = '<OutputTabularRDD %s %s>' % (path, rdd)

    def _clear_dependencies(self):
        RDD._clear_dependencies(self)
        self.rdd = None

    def compute(self, split):
        buffers = [list() for i in self.fields]
        remain_size = STRIPE_DATA_SIZE
        path = os.path.join(self.path, '%04d.dt' % split.index)
        indices = dict((i, AdaptiveIndex()) for i in self.indices)

        def write_stripe(f, compressed, header, padding=True):
            h = compress(marshal.dumps(header))
            assert len(h) < STRIPE_HEADER_SIZE
            f.write(struct.pack('I', len(h)))
            f.write(h)
            padding_size = STRIPE_SIZE - len(h) - 4
            for c in compressed:
                f.write(c)
                padding_size -= len(c)

            if padding:
                f.write('\0' * padding_size)

        with atomic_file(path) as f:
            stripe_id = 0
            for it in chain(self.prev.iterator(sp) for sp in split.splits):
                row = it[:len(self.fields)]
                size = len(marshal.dumps(tuple(row)))
                if size > STRIPE_DATA_SIZE:
                    raise RuntimeError('Row too big')

                if size > remain_size:
                    compressed = [compress(marshal.dumps(tuple(b))) for b in buffers]
                    _sizes = tuple(map(len, compressed))
                    _remain_size = STRIPE_DATA_SIZE - sum(_sizes)
                    if size > _remain_size:
                        write_stripe(f, compressed, _sizes)
                        buffers = [list() for i in self.fields]
                        remain_size = STRIPE_DATA_SIZE
                        stripe_id += 1
                    else:
                        remain_size = _remain_size

                remain_size -= size
                for i, value in enumerate(row):
                    buffers[i].append(value)
                    field = self.fields[i]
                    if field in self.indices:
                        indices[field].add(value, stripe_id)

            if any(buffers):
                compressed = [compress(marshal.dumps(tuple(b))) for b in buffers]
                _sizes = tuple(map(len, compressed))
                write_stripe(f, compressed, _sizes, False)

            footer_indices = zlib.compress(cPickle.dumps(indices, -1))
            footer_fields = compress(marshal.dumps(self.fields))
            f.write(footer_indices)
            f.write(footer_fields)
            f.write(struct.pack('II', len(footer_fields), len(footer_indices)))

        yield path


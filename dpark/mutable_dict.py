import uuid
import os
import cPickle
import urllib
import struct
import glob
import uuid
from dpark.env import env
from dpark.util import compress, decompress, mkdir_p, atomic_file
from dpark.tracker import GetValueMessage, AddItemMessage
from dpark.dependency import HashPartitioner
from collections import OrderedDict

class LRUDict(object):
    def __init__(self, limit=None):
        self.limit = limit
        self.value = OrderedDict()

    def get(self, key, default=None):
        result = self.value.pop(key, None)
        if result is not None:
            self.value[key] = result
            return result

        return default

    def put(self, key, value):
        self.value[key] = value
        if self.limit is not None and len(self.value) > self.limit:
            self.value.popitem(last=False)


class ConflictValues(object):
    def __init__(self, v=[]):
        self.value = list(v)

    def __repr__(self):
        return '<ConflictValues %s>' % self.value

class MutableDict(object):
    def __init__(self, partition_num , cacheLimit=None):
        self.uuid = str(uuid.uuid4())
        self.partitioner = HashPartitioner(partition_num)
        self.data = LRUDict(cacheLimit)
        self.cacheLimit = cacheLimit
        self.updated = {}
        self.generation = 1
        self.register(self)
        self.is_local = True

    def __getstate__(self):
        return (self.uuid, self.partitioner, self.generation, self.cacheLimit)

    def __setstate__(self, v):
        self.uuid, self.partitioner, self.generation, self.cacheLimit = v
        self.data = LRUDict(self.cacheLimit)
        self.updated = {}
        self.is_local = False
        self.register(self)

    def get(self, key):
        values = self.updated.get(key)
        if values is not None:
            return values[0]

        _key = self._get_key(key)
        values = self.data.get((_key, key))
        if values is None:
            for k, v in self._fetch_missing(_key).iteritems():
                self.data.put((_key, k), v)

            values = self.data.get((_key, key))

        return values[0] if values is not None else None

    def put(self, key, value):
        if isinstance(value, ConflictValues):
            raise TypeError('Cannot put ConflictValues into mutable_dict')

        self.updated[key] = (value, self.generation)

    def _flush(self):
        if not self.updated:
            return

        updated_keys = {}
        path = self._get_path()
        uri = env.get('SERVER_URI')
        server_uri = '%s/%s' % (uri, os.path.basename(path))

        for k,v in self.updated.items():
            key = self._get_key(k)
            if key in updated_keys:
                updated_keys[key][k] = v
            else:
                updated_keys[key] = {k:v}

        uid = uuid.uuid4().get_hex()
        for key, updated in updated_keys.items():
            new = self._fetch_missing(key)
            for k,v in updated.items():
                if v is None:
                    new.pop(k)
                else:
                    new[k] = v

            filename = '%s_%s_%s' % (key, self.generation, uid)
            fn = os.path.join(path, filename)
            if os.path.exists(fn):
                raise RuntimeError('conflict uuid for mutable_dict')

            url = '%s/%s' % (server_uri, filename)
            with atomic_file(fn) as f:
                data = compress(cPickle.dumps(new))
                f.write(struct.pack('<I', len(data)+4) + data)

            env.trackerClient.call(AddItemMessage('mutable_dict_new:%s' % key, url))

            files = glob.glob(os.path.join(path, '%s-*' % self.uuid ))
            for f in files:
                if int(f.split('_')[-2]) < self.generation -1:
                    try:
                        os.remove(f)
                    except OSError:
                        pass

        self.updated.clear()
        self.data = LRUDict(self.cacheLimit)

    def _merge(self):
        locs = env.trackerServer.locs
        new = []
        for k in locs:
            if k.startswith('mutable_dict_new:%s-' % self.uuid):
                new.append(k)

        if not new:
            return

        self.generation += 1
        length = len('mutable_dict_new:')
        for k in new:
            locs['mutable_dict:%s' % k[length:]] = locs.pop(k)

        self.updated.clear()
        self.data = LRUDict(self.cacheLimit)

    def _fetch_missing(self, key):
        result = {}
        urls = env.trackerClient.call(GetValueMessage('mutable_dict:%s' % key))
        for url in urls:
            f = urllib.urlopen(url)
            if f.code is not None and f.code != 200:
                raise IOError('Open %s failed:%s' % (url, f.code))

            data = f.read()
            if len(data) < 4:
                raise IOError('Transfer %s failed: %s received' % (url, len(data)))

            length, = struct.unpack('<I', data[:4])
            if length != len(data):
                raise IOError('Transfer %s failed: %s received, %s expected' % (url,
                    len(data), length))

            data = cPickle.loads(decompress(data[4:]))
            for k,v in data.items():
                if k in result:
                    r = result[k]
                    if v[1] == r[1]:
                        r0 = r[0]
                        v0 = v[0]
                        merged = r0.value if isinstance(r0, ConflictValues) else [r0]
                        merged += v0.value if isinstance(v0, ConflictValues) else [v0]
                        result[k] = (ConflictValues(merged), r[1])
                    else:
                        result[k] = v if v[1] > r[1] else r
                else:
                    result[k] = v

        return result

    def _get_key(self, key):
        return '%s-%s' % (self.uuid,
                self.partitioner.getPartition(key))

    def _get_path(self):
        dirs = env.get('WORKDIR')
        if not dirs:
            raise RuntimeError('No available workdir')

        path = os.path.join(dirs[0], 'mutable_dict')
        if os.path.exists(path):
            return path

        st = os.statvfs(dirs[0])
        ratio = st.f_bfree * 1.0 / st.f_blocks
        if ratio >= 0.66:
            mkdir_p(path)
            return path

        for d in dirs[1:]:
            p = os.path.join(d, 'mutable_dict')
            try:
                os.makedirs(p)
                os.symlink(p, path)
            except OSError, e:
                pass

            return path

        raise RuntimeError('Cannot find suitable workdir')

    _all_mutable_dicts = {}
    @classmethod
    def register(cls, md):
        uuid = md.uuid
        _md = cls._all_mutable_dicts.get(uuid)
        if not _md or _md.generation != md.generation:
            cls._all_mutable_dicts[md.uuid] = md
        else:
            md.data = _md.data
            md.updated = _md.updated

    @classmethod
    def flush(cls):
        for md in cls._all_mutable_dicts.values():
            md._flush()

    @classmethod
    def merge(cls):
        for md in cls._all_mutable_dicts.values():
            md._merge()


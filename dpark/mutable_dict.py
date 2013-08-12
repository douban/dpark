import uuid
import os
import cPickle
import urllib
import struct
import glob
from dpark.env import env
from dpark.util import compress, decompress
from dpark.tracker import GetValueMessage, AddItemMessage
from dpark.dependency import HashPartitioner
from dpark import _ctx

class ConflictValues(object):
    value = []
    def __init__(self, v):
        self.value.append(v)

    def __repr__(self):
        return '<ConflictValues %s>' % self.value

class MutableDict(object):
    
    def __init__(self, partition_num , partitioner=HashPartitioner):
        self.uuid = str(uuid.uuid4())
        self.partitioner = partitioner(partition_num)
        self.data = {}
        self.updated = {}
        _ctx.start()
        self.client = env.trackerClient
        self.generation = 1

    def __getstate__(self):
        return (self.uuid, self.partitioner, self.generation)

    def __setstate__(self, v):
        self.uuid, self.partitioner, self.generation = v
        self.client = env.trackerClient
        self.data = {}
        self.updated = {}

    def get(self, key):
        values = self.updated.get(key)
        if values is not None:
            return values

        values = self.data.get(key)
        if values is not None:
            return values

        self.data.update(self._fetch_missing(self._get_key(key)))
        return self.data.get(key)

    def put(self, key, value):
        if isinstance(value, ConflictValues):
            raise TypeError('Cannot put ConflictValues into mutable_dict')

        self.updated[key] = value

    def flush(self):
        updated_keys = {}
        path = self._get_path()
        server_uri = '%s/%s' % (env.get('SERVER_URI'), os.path.basename(path))

        st = os.statvfs(path)
        ratio = st.f_bfree * 1.0 / st.f_blocks
        if ratio < 0.66:
            raise Exception('Insufficient disk space')

        for k,v in self.updated.items():
            key = self._get_key(k)
            if key in updated_keys:
                updated_keys[key][k] = v
            else:
                updated_keys[key] = {k:v}

        for key, updated in updated_keys.items():
            new = self._fetch_missing(key)
            for k,v in updated.items():
                if v is None:
                    new.pop(key)
                else:
                    new[k] = v

            filename = '%s_%s_%s' % (key, self.generation, os.getpid())
            fn = os.path.join(path, filename)
            url = '%s/%s' % (server_uri, filename)
            with open(fn+'.tmp', 'wb+') as f:
                data = compress(cPickle.dumps(new))
                f.write(struct.pack('<I', len(data)+4) + data)

            os.rename(fn+'.tmp', fn)
            self.client.call(AddItemMessage('mutable_dict_new:%s' % key, url))

            files = glob.glob(os.path.join(path, '%s_*' % key))
            for f in files:
                if int(f.split('_')[-2]) < self.generation -1:
                    try:
                        os.remove(f)
                    except OSError, e:
                        pass

    def merge(self):
        self.generation += 1
        locs = env.trackerServer.locs
        new = []
        for k in locs:
            if k.startswith('mutable_dict_new:'):
                new.append(k)

        length = len('mutable_dict_new:')
        for k in new:
            locs['mutable_dict:%s' % k[length:]] = locs.pop(k)

    def _fetch_missing(self, key):
        result = {}
        urls = self.client.call(GetValueMessage('mutable_dict:%s' % key))
        for url in urls:
            f = urllib.urlopen(url)
            if f.code != 200:
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
                    r = r.value if isinstance(r, ConflictValues) else [r]
                    r += v.value if isinstance(v, ConflictValues) else [v]
                    result[k] = ConflictValues(r)
                else:
                    result[k] = v

        return result

    def _get_key(self, key):
        return '%s-%s' % (self.uuid,
                self.partitioner.getPartition(key))

    def _get_path(self):
        dirs = env.get('WORKDIR')
        if not dirs:
            raise Exception('No available workdir')

        path = os.path.join(dirs[0], 'mutable_dict')
        if not os.path.exists(path):
            try:
                os.mkdir(path)
            except OSError, e:
                pass

        return path


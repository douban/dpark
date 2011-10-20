import os, time
import uuid
import socket
import cPickle

import cache

REGISTER_BROADCAST_TRACKER = 0
UNREGISTER_BROADCAST_TRACKER = 1
FIND_BROADCAST_TRACKER = 2
GET_UPDATED_SHARE = 3

class Broadcast:
    initialized = False
    isMaster = False
    broadcastFactory = None

    def __init__(self):
        self.uuid = str(uuid.uuid4())

    def value(self):
        raise NotImplementedError
    
    @classmethod
    def initialize(cls, isMaster):
        if cls.initialized:
            return

        cls.broadcastFactory = FileBroadcastFactory()
        cls.isMaster = isMaster
        cls.masterHostAddress = socket.gethostname()
        cls.broadcastFactory.initialize(isMaster)
        cls.initialized = True
    
    @classmethod
    def getBroadcastFactory(cls):
        return cls.broadcastFactory

    @classmethod
    def newBroadcast(cls, value, isLocal):
        return cls.broadcastFactory.newBroadcast(value, isLocal)

class FileBroadcast(Broadcast):
    def __init__(self, value, isLocal):
        Broadcast.__init__(self)
        self.value = value
        self.cache.put(self.uuid, value)
        if not isLocal:
            print self.sendBroadcast()
        
    def sendBroadcast(self):
        path = os.path.join(self.workdir, 'broadcat-'+self.uuid)
        print path
        f = open(path, 'wb', 65536)
        cPickle.dump(self.value, f, -1)
        f.close()

    def __getstate__(self):
        return self.uuid

    def __setstate__(self, uuid):
        self.uuid = uuid
        self.value = self.cache.get(uuid)
        if self.value == 'loading':
            time.sleep(0.1)
            self.value = self.cache.get(uuid)
        if self.value is None:
            self.cache.put(uuid, 'loading')
            path = os.path.join(self.workdir, 'broadcat-'+uuid)
            self.value = cPickle.load(open(path, 'rb', 65536))
            self.cache.put(uuid, self.value)

    cache = None
    workdir = None
    initialized = False
    compress = False
    @classmethod
    def initialize(cls, isMaster):
        if cls.initialized:
            return
        if isMaster:
            cls.cache = cache.Cache()
        else:
            cls.cache = cahce.SerializeCache(cache.mmapCache)
        if os.path.exists('/mfs/tmp'):
            cls.workdir = '/mfs/tmp/dpark'
        else:
            cls.workdir = '/tmp/dpark'
        if not os.path.exists(cls.workdir):
            os.makedirs(cls.workdir)
        cls.initialized = True

class FileBroadcastFactory:
    @classmethod
    def initialize(cls, isMaster):
        return FileBroadcast.initialize(isMaster)
    @classmethod
    def newBroadcast(cls, value, isLocal):
        return FileBroadcast(value, isLocal)


if __name__ == '__main__':
    Broadcast.initialize(True)
    v = range(1000)
    v = Broadcast.newBroadcast(v, False)
    v = cPickle.loads(cPickle.dumps(v))
    print v.value

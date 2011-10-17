
from rdd import *
from schedule import *
from env import env

class SparkContext:
    nextRddId = 0
    nextShuffleId = 0

    def __init__(self, master='local', name='spark'):
        self.master = master
        self.name = name
        self.init()

    def init(self):
        #Broadcast.initialize(True)
        if self.master.startswith('local'):
            self.scheduler = LocalScheduler()
            self.isLocal = True
        elif self.master.startswith('thread'):
            self.scheduler = MultiThreadScheduler(2)
            self.isLocal = True
        elif self.master.startswith('process'):
            self.scheduler = MultiProcessScheduler(2)
            self.isLocal = False
        else:
            self.scheduler = MesosScheduler(self, self.master, "spark")
            self.isLocal = False
        
        self.defaultParallelism = self.scheduler.defaultParallelism
        self.defaultMinSplits = min(self.defaultParallelism, 2)
        
        env.start(True)
        self.env = env
        self.scheduler.start()

    def newRddId(self):
        self.nextRddId += 1
        return self.nextRddId

    def newShuffleId(self):
        self.nextShuffleId += 1
        return self.nextShuffleId

    def parallelize(self, seq, numSlices=None): 
        if numSlices is None:
            numSlices = self.defaultParallelism
        return ParallelCollection(self, seq, numSlices)

    def makeRDD(self, seq, numSlices=None):
        if numSlices is None:
            numSlices = self.defaultParallelism
        return self.parallelize(seq, numSlices)
    
    def textFile(self, path, numSplits=None, splitSize=None, ext=''):
        if not os.path.exists(path):
            raise IOError("not exists")
        if os.path.isdir(path):
            rdds = [TextFileRDD(self, os.path.join(path, n),numSplits,splitSize) 
                     for n in os.listdir(path) 
                         if not os.path.isdir(os.path.join(path, n))
                             and n.endswith(ext)]
            return self.union(rdds)
        else:
            return TextFileRDD(self, path, numSplits, splitSize)

    def sequenceFile(self, path, minSplits):
        return self.hadoopFile(path, format, minSplits).map(lambda k,v: (k,v))

    def objectFile(self, path, minSplits=None):
        if minSplits is None:
            minSplits = self.defaultMinSplits
        return self.sequenceFile(path, minSplits).flatMap(lambda x: loads(x))

    def union(self, rdds):
        return UnionRDD(self, rdds)

    def accumulator(self, init, param=None):
        return Accumulator(init, param)

    def broadcst(self, v):
        return newBroadcast(v, self.isLocal)

    def stop(self):
        self.scheduler.stop()
        self.env.stop()

    def waitForRegister(self):
        self.scheduler.waitForRegister()

    def runJob(self, rdd, func, partitions=None, allowLocal=False):
        if partitions is None:
            partitions = range(len(rdd.splits))
        return self.scheduler.runJob(rdd, lambda _,it: func(it), partitions, allowLocal)

    def __getstate__(self):
        return (self.master, self.name)

    def __setstate__(self, state):
        self.master, self.name = state
        self.env = env

import atexit
import optparse

from rdd import *
from schedule import *
from env import env
from broadcast import Broadcast

class SparkContext:
    nextRddId = 0
    nextShuffleId = 0

    def __init__(self, master=None, name=None):
        options = parse_options()
        if master is None:
            master = options.master
        if name is None:
            name = options.name

        self.master = master
        self.name = name

        shuffleDir = '/tmp/dpark'
        if self.master.startswith('local'):
            self.scheduler = LocalScheduler()
            self.isLocal = True
        elif self.master.startswith('thread'):
            self.scheduler = MultiThreadScheduler(options.parallel)
            self.isLocal = True
        elif self.master.startswith('process'):
            self.scheduler = MultiProcessScheduler(options.parallel)
            self.isLocal = True
        elif (self.master.startswith('mesos://')
              or self.master.startswith('zoo://')):
            self.scheduler = MesosScheduler(self.master, "spark")
            self.isLocal = False
            if os.path.exists('/mfs/tmp'):
                shuffleDir = os.path.join('/mfs/tmp', 'dpark') # TODO uuid
        
        self.defaultParallelism = self.scheduler.defaultParallelism()
        self.defaultMinSplits = max(self.defaultParallelism, 2)
        
        env.start(True, shuffleDir=shuffleDir)
        self.scheduler.start()
        self.started = True

        atexit.register(self.stop)

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

#    def objectFile(self, path, minSplits=None):
#        if minSplits is None:
#            minSplits = self.defaultMinSplits
#        return self.sequenceFile(path, minSplits).flatMap(lambda x: loads(x))

    def union(self, rdds):
        return UnionRDD(self, rdds)

    def accumulator(self, init, param=None):
        return Accumulator(init, param)

    def broadcast(self, v):
        return Broadcast.newBroadcast(v, self.isLocal)

    def stop(self):
        if self.started:
            self.scheduler.stop()
            env.stop()
            self.started = False

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

def parse_options():
    parser = optparse.OptionParser(usage="Usage: %prog [options] [args]")
    parser.allow_interspersed_args=False
    parser.add_option("-m", "--master", type="string", default="local")
    parser.add_option("-n", "--name", type="string", default="dpark")
    parser.add_option("-p", "--parallel", type="int", default=2)
    parser.add_option("-q", "--quiet", action="store_true")
    parser.add_option("-v", "--verbose", action="store_true")
    options, args = parser.parse_args()
    
    logging.basicConfig(format='[dpark] %(asctime)-15s %(message)s',
        level=options.quiet and logging.ERROR
              or options.verbose and logging.DEBUG or logging.WARNING)
    
    return options

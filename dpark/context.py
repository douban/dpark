import os, sys
import atexit
import optparse
import signal
import logging
import gc

from rdd import *
from accumulator import Accumulator
from schedule import LocalScheduler, MultiProcessScheduler, MesosScheduler
from env import env
from broadcast import Broadcast

logger = logging.getLogger("context")

class DparkContext(object):
    nextShuffleId = 0
    _instances = {}
    def __new__(cls, master=None):
        if master not in cls._instances:
            cls._instances[master] = super(DparkContext, cls).__new__(cls)
        return cls._instances[master]

    def __init__(self, master=None):
        
        if 'MESOS_SLAVE_PID' in os.environ and 'DRUN_SIZE' not in os.environ:
            from executor import run
            run()
            sys.exit(0)
        
        options = parse_options()
        self.options = options
        master = master or options.master

        if master == 'local':
            self.scheduler = LocalScheduler()
            self.isLocal = True
        elif master == 'process':
            self.scheduler = MultiProcessScheduler(options.parallel)
            self.isLocal = True
        else:
            if master == 'mesos':
                master = os.environ.get('MESOS_MASTER')
                if not master:
                    raise Exception("invalid uri of mesos master: %s" % master)
            if master.startswith('mesos://'):
                if '@' in master:
                    master = master[master.rfind('@')+1:]
                else:
                    master = master[master.rfind('//')+2:]
            elif master.startswith('zoo://'):
                master = 'zk' + master[3:]

            if ':' not in master:
                master += ':5050'
            self.scheduler = MesosScheduler(master, options) 
            self.isLocal = False
            
        self.master = master

        if options.parallel:
            self.defaultParallelism = options.parallel
        else:
            self.defaultParallelism = self.scheduler.defaultParallelism()
        self.defaultMinSplits = max(self.defaultParallelism, 2)
      
        try:
            from rfoo.utils import rconsole
            rconsole.spawn_server(locals(), 0)
        except ImportError:
            pass

        self.started = False

    def newShuffleId(self):
        self.nextShuffleId += 1
        return self.nextShuffleId

    def parallelize(self, seq, numSlices=None): 
        if numSlices is None:
            numSlices = self.defaultParallelism
        return ParallelCollection(self, seq, numSlices)

    def makeRDD(self, seq, numSlices=None):
        return self.parallelize(seq, numSlices)
    
    def textFile(self, path, ext='', followLink=True, maxdepth=0, cls=TextFileRDD, *ka, **kws):
        if isinstance(path, (list, tuple)):
            return self.union([self.textFile(p, ext, followLink, maxdepth, cls, *ka, **kws)
                for p in path])

        def create_rdd(cls, path, *ka, **kw):
            if cls is TextFileRDD:
                if path.endswith('.bz2'):
                    return BZip2FileRDD(self, path, *ka, **kw)
                elif path.endswith('.gz'):
                    return GZipFileRDD(self, path, *ka, **kw)
                rpath = os.path.realpath(path)
                if rpath.startswith('/mfs/'):
                    return MFSTextFileRDD(self, rpath[4:], 'mfsmaster', *ka, **kw)
                if rpath.startswith('/home2/'):
                    return MFSTextFileRDD(self, rpath[6:], 'mfsmaster2', *ka, **kw)
            return cls(self, path, *ka, **kw)

        if os.path.isdir(path):
            paths = []
            for root,dirs,names in os.walk(path, followlinks=followLink):
                if maxdepth > 0:
                    depth = len(filter(None, root[len(path):].split('/'))) + 1
                    if depth > maxdepth:
                        break
                for n in sorted(names):
                    if n.endswith(ext) and not n.startswith('.'):
                        p = os.path.join(root, n)
                        if followLink or not os.path.islink(p):
                            paths.append(p)
                dirs.sort()
                for d in dirs[:]:
                    if d.startswith('.'):
                        dirs.remove(d)

            rdds = [create_rdd(cls, p, *ka, **kws) 
                     for p in paths]
            return self.union(rdds)
        else:
            return create_rdd(cls, path, *ka, **kws)

    def bzip2File(self, *args, **kwargs):
        "deprecated"
        logger.warning("bzip2File() is deprecated, use textFile('xx.bz2') instead")
        return self.textFile(cls=BZip2FileRDD, *args, **kwargs)

    def csvFile(self, path, dialect='excel', *args, **kwargs):
        """ deprecated. """
        return self.textFile(path, cls=TextFileRDD, *args, **kwargs).fromCsv(dialect)

    def union(self, rdds):
        return UnionRDD(self, rdds)

    def zip(self, rdds):
        return ZippedRDD(self, rdds)

    def accumulator(self, init=0, param=None):
        return Accumulator(init, param)

    def broadcast(self, v):
        self.start()
        return Broadcast.newBroadcast(v, self.master=='local')

    def start(self):
        if self.started:
            return

        env.start(True, isLocal=self.isLocal)
        self.scheduler.start()
        self.started = True
        atexit.register(self.stop)

        def handler(signm, frame):
            logger.error("got signal %d, exit now", signm)
            self.scheduler.shutdown()

        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGHUP, handler)
        signal.signal(signal.SIGABRT, handler)
        signal.signal(signal.SIGQUIT, handler)

    def runJob(self, rdd, func, partitions=None, allowLocal=False):
        self.start()

        if partitions is None:
            partitions = range(len(rdd))
        try:
            gc.disable()
            for it in self.scheduler.runJob(rdd, func, partitions, allowLocal):
                yield it
        finally:
            gc.collect()
            gc.enable()

    def clear(self):
        self.scheduler.clear()
        gc.collect()

    def stop(self):
        if not self.started:
            return

        self.scheduler.stop()
        env.stop()
        self.started = False

    def __getstate__(self):
        raise ValueError("should not pickle ctx")


parser = optparse.OptionParser(usage="Usage: %prog [options] [args]")

def add_default_options():
    parser.disable_interspersed_args()

    group = optparse.OptionGroup(parser, "Dpark Options")

    group.add_option("-m", "--master", type="string", default="local",
            help="master of Mesos: local, process, or mesos://")
#    group.add_option("-n", "--name", type="string", default="dpark",
#            help="job name")
    group.add_option("-p", "--parallel", type="int", default=0, 
            help="number of processes")

    group.add_option("-c", "--cpus", type="float", default=1.0,
            help="cpus used per task")
    group.add_option("-M", "--mem", type="float", default=1000.0,
            help="memory used per task")
    group.add_option("-g", "--group", type="string", default="",
            help="which group of machines")
    group.add_option("--err", type="float", default=0.0,
            help="acceptable ignored error record ratio (0.01%)")
    group.add_option("--snapshot_dir", type="string", default="",
            help="shared dir to keep snapshot of RDDs")

    group.add_option("--self", action="store_true",
            help="user self as exectuor")
    group.add_option("--profile", action="store_true",
            help="do profiling")

    parser.add_option_group(group)

    parser.add_option("-q", "--quiet", action="store_true")
    parser.add_option("-v", "--verbose", action="store_true")

add_default_options()

def parse_options():
    options, args = parser.parse_args()
    options.logLevel = (options.quiet and logging.ERROR
                  or options.verbose and logging.DEBUG or logging.INFO)

    logging.basicConfig(format='%(asctime)-15s [%(name)-9s] %(message)s',
        level=options.logLevel)
    
    return options

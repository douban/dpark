import os, sys
import atexit
import optparse
import signal
import logging
import gc

from dpark.rdd import *
from dpark.accumulator import Accumulator
from dpark.schedule import LocalScheduler, MultiProcessScheduler, MesosScheduler
from dpark.env import env
from dpark.moosefs import walk
from dpark.tabular import TabularRDD
import dpark.conf as conf
from math import ceil

logger = logging.getLogger(__name__)

def singleton(cls):
    instances = {}
    def getinstance(*a, **kw):
        key = (cls, tuple(a), tuple(sorted(kw.items())))
        if key not in instances:
            instances[key] = cls(*a, **kw)
        return instances[key]
    return getinstance

def setup_conf(options):
    if options.conf:
        conf.load_conf(options.conf)
    elif 'DPARK_CONF' in os.environ:
        conf.load_conf(os.environ['DPARK_CONF'])
    elif os.path.exists('/etc/dpark.conf'):
        conf.load_conf('/etc/dpark.conf')

    if options.mem is None:
        options.mem = conf.MEM_PER_TASK

    conf.__dict__.update(os.environ)
    import moosefs
    moosefs.MFS_PREFIX = conf.MOOSEFS_MOUNT_POINTS
    moosefs.master.ENABLE_DCACHE = conf.MOOSEFS_DIR_CACHE

@singleton
class DparkContext(object):
    nextShuffleId = 0
    def __init__(self, master=None):
        self.master = master
        self.initialized = False
        self.started = False
        self.defaultParallelism = 2

    def init(self):
        if self.initialized:
            return

        options = parse_options()
        self.options = options

        master = self.master or options.master
        if master == 'local':
            self.scheduler = LocalScheduler()
            self.isLocal = True
        elif master == 'process':
            self.scheduler = MultiProcessScheduler(options.parallel)
            self.isLocal = False
        else:
            if master == 'mesos':
                master = conf.MESOS_MASTER

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

        self.initialized = True

    @staticmethod
    def setLogLevel(level):
        logging.getLogger('dpark').setLevel(level)

    def newShuffleId(self):
        self.nextShuffleId += 1
        return self.nextShuffleId

    def parallelize(self, seq, numSlices=None):
        self.init()
        if numSlices is None:
            numSlices = self.defaultParallelism
        return ParallelCollection(self, seq, numSlices)

    def makeRDD(self, seq, numSlices=None):
        return self.parallelize(seq, numSlices)

    def textFile(self, path, ext='', followLink=True, maxdepth=0, cls=TextFileRDD, *ka, **kws):
        self.init()
        if isinstance(path, (list, tuple)):
            return self.union([self.textFile(p, ext, followLink, maxdepth, cls, *ka, **kws)
                for p in path])

        path = os.path.realpath(path)
        def create_rdd(cls, path, *ka, **kw):
            if cls is TextFileRDD:
                if path.endswith('.bz2'):
                    return BZip2FileRDD(self, path, *ka, **kw)
                elif path.endswith('.gz'):
                    return GZipFileRDD(self, path, *ka, **kw)
            return cls(self, path, *ka, **kw)

        if os.path.isdir(path):
            paths = []
            for root,dirs,names in walk(path, followlinks=followLink):
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

    def partialTextFile(self, path, begin, end, splitSize=None, numSplits=None):
        self.init()
        return PartialTextFileRDD(self, path, begin, end, splitSize, numSplits)

    def bzip2File(self, *args, **kwargs):
        "deprecated"
        logger.warning("bzip2File() is deprecated, use textFile('xx.bz2') instead")
        return self.textFile(cls=BZip2FileRDD, *args, **kwargs)

    def csvFile(self, path, dialect='excel', *args, **kwargs):
        return self.textFile(path, cls=TextFileRDD, *args, **kwargs).fromCsv(dialect)

    def binaryFile(self, path, fmt=None, length=None, *args, **kwargs):
        return self.textFile(path, cls=BinaryFileRDD, fmt=fmt, length=length, *args, **kwargs)

    def tableFile(self, path, *args, **kwargs):
        return self.textFile(path, cls=TableFileRDD, *args, **kwargs)

    def tabular(self, path, **kw):
        self.init()
        return TabularRDD(self, path, **kw)

    def table(self, path, **kwargs):
        dpath = path[0] if isinstance(path, (list, tuple)) else path
        for root, dirs, names in walk(dpath):
            if '.field_names' in names:
                p = os.path.join(root, '.field_names')
                fields = open(p).read().split('\t')
                break
        else:
            raise Exception("no .field_names found in %s" % path)
        return self.tableFile(path, **kwargs).asTable(fields)

    def beansdb(self, path, depth=None, filter=None, fullscan=False, raw=False, only_latest=False):
        "(Key, (Value, Version, Timestamp)) data in beansdb"
        self.init()
        if isinstance(path, (tuple, list)):
            return self.union([self.beansdb(p, depth, filter, fullscan, raw, only_latest)
                    for p in path])

        path = os.path.realpath(path)
        assert os.path.exists(path), "%s no exists" % path
        if os.path.isdir(path):
            subs = []
            if not depth:
                subs = [os.path.join(path, n) for n in os.listdir(path) if n.endswith('.data')]
            if subs:
                rdd = self.union([BeansdbFileRDD(self, p, filter, fullscan, True)
                        for p in subs])
            else:
                subs = [os.path.join(path, '%x'%i) for i in range(16)]
                rdd = self.union([self.beansdb(p, depth and depth-1, filter, fullscan, True, only_latest)
                        for p in subs if os.path.exists(p)])
                only_latest = False
        else:
            rdd = BeansdbFileRDD(self, path, filter, fullscan, True)

        # choose only latest version
        if only_latest:
            rdd = rdd.reduceByKey(lambda v1,v2: v1[2] > v2[2] and v1 or v2, int(ceil(len(rdd) / 4)))
        if not raw:
            rdd = rdd.mapValue(lambda (v,ver,t): (restore_value(*v), ver, t))
        return rdd

    def union(self, rdds):
        return UnionRDD(self, rdds)

    def zip(self, rdds):
        return ZippedRDD(self, rdds)

    def accumulator(self, init=0, param=None):
        return Accumulator(init, param)

    def broadcast(self, v):
        self.start()
        from dpark.broadcast import Broadcast
        return Broadcast(v)

    def start(self):
        if self.started:
            return

        self.init()

        env.start(True)
        self.scheduler.start()
        self.started = True
        atexit.register(self.stop)

        def handler(signm, frame):
            logger.error("got signal %d, exit now", signm)
            self.scheduler.shutdown()
        try:
            signal.signal(signal.SIGTERM, handler)
            signal.signal(signal.SIGHUP, handler)
            signal.signal(signal.SIGABRT, handler)
            signal.signal(signal.SIGQUIT, handler)
        except: pass

        try:
            from rfoo.utils import rconsole
            rconsole.spawn_server(locals(), 0)
        except ImportError:
            pass

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
        if not self.started:
            return

        self.scheduler.clear()
        gc.collect()

    def stop(self):
        if not self.started:
            return

        env.stop()
        self.scheduler.stop()
        self.started = False

    def __getstate__(self):
        raise ValueError("should not pickle ctx")


parser = optparse.OptionParser(usage="Usage: %prog [options] [args]")

def add_default_options():
    parser.disable_interspersed_args()

    group = optparse.OptionGroup(parser, "Dpark Options")

    group.add_option("-m", "--master", type="string", default="local",
            help="master of Mesos: local, process, host[:port], or mesos://")
#    group.add_option("-n", "--name", type="string", default="dpark",
#            help="job name")
    group.add_option("-p", "--parallel", type="int", default=0,
            help="number of processes")

    group.add_option("-c", "--cpus", type="float", default=1.0,
            help="cpus used per task")
    group.add_option("-M", "--mem", type="float",
            help="memory used per task")
    group.add_option("-g", "--group", type="string", default="",
            help="which group of machines")
    group.add_option("--err", type="float", default=0.0,
            help="acceptable ignored error record ratio (0.01%)")
    group.add_option("--snapshot_dir", type="string", default="",
            help="shared dir to keep snapshot of RDDs")

    group.add_option("--conf", type="string",
            help="path for configuration file")
    group.add_option("--self", action="store_true",
            help="user self as exectuor")
    group.add_option("--profile", action="store_true",
            help="do profiling")
    group.add_option("--keep-order", action="store_true",
            help="deprecated, always keep order")

    parser.add_option_group(group)

    parser.add_option("-q", "--quiet", action="store_true")
    parser.add_option("-v", "--verbose", action="store_true")

add_default_options()


def parse_options():
    options, args = parser.parse_args()
    setup_conf(options)

    options.logLevel = (options.quiet and logging.ERROR
                  or options.verbose and logging.DEBUG or logging.INFO)

    log_format = '%(asctime)-15s [%(levelname)s] [%(name)-9s] %(message)s'
    logging.basicConfig(format=log_format, level=options.logLevel)

    logger = logging.getLogger('dpark')
    logger.propagate=False

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(log_format))

    logger.addHandler(handler)
    logger.setLevel(max(options.logLevel, logger.level))

    return options

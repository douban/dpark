from __future__ import absolute_import
import atexit
import optparse
import signal
import logging
import gc

from dpark.rdd import *
from dpark.utils.beansdb import restore_value
from dpark.accumulator import Accumulator
from dpark.schedule import (
    LocalScheduler, MultiProcessScheduler, MesosScheduler
)
from dpark.env import env
from dpark.file_manager import walk
from dpark.tabular import TabularRDD
from dpark.utils import memory_str_to_mb
from dpark.utils.log import init_dpark_logger, get_logger
from dpark.utils.debug import spawn_rconsole
import dpark.conf as conf
from math import ceil
import socket
from six.moves import range

logger = get_logger(__name__)


def is_gevent_monkey_patched():
    try:
        from gevent import monkey
    except ImportError:
        return False
    else:
        return bool(getattr(monkey, 'saved', False))


def singleton(cls):
    instances = {}

    def getinstance(*a, **kw):
        key = (cls, tuple(a), tuple(sorted(kw.items())))
        if key not in instances:
            if len(instances) >= 1:
                logger.error('Too many DparkContext created!')

            instances[key] = cls(*a, **kw)

        return instances[key]

    getinstance._instances = instances
    return getinstance


_shutdown_handlers = []


def _shutdown():
    started = len(_shutdown_handlers)
    for handler in _shutdown_handlers:
        try:
            handler()
        except Exception:
            logger.exception('Faield to shutdown context')

    _shutdown_handlers[:] = []
    DparkContext._instances.clear()
    if started:
        logger.info("dpark shutdown.")


atexit.register(_shutdown)
_pid = os.getpid()
_prev_handlers = {}
_signals = [
    signal.SIGINT, signal.SIGQUIT, signal.SIGTERM,
    signal.SIGABRT, signal.SIGHUP
]


def _handler(signum, frame):
    for sig, handler in _prev_handlers.items():
        signal.signal(sig, handler)

    if _pid == os.getpid():
        # called on main process, will do cleanup
        logger.error("got signal %d, exiting now...", signum)
        _shutdown()

    # resend signal to trigger previous handlers
    _prev_handlers.clear()
    os.kill(os.getpid(), signum)


def register_sighandlers():
    if not _prev_handlers:
        for sig in _signals:
            try:
                _prev_handlers[sig] = signal.signal(sig, _handler)
            except Exception:
                logger.exception('Failed to register signal handler')


@singleton
class DparkContext(object):
    nextShuffleId = 0
    options = None

    def __init__(self, master=None):
        if is_gevent_monkey_patched():
            raise RuntimeError('DPark do not support gevent.')

        self.master = master
        self.initialized = False
        self.started = False
        self.web_port = None
        self.webui_url = None
        self.data_limit = None
        self.scheduler = None
        self.is_local = True
        self.defaultParallelism = 2
        self.defaultMinSplits = 2
        self.is_dstream = False

    def init(self):
        if self.initialized:
            return

        register_sighandlers()

        cls = self.__class__
        options = cls.options
        if options is None:
            options = cls.options = parse_options()

        try:
            import dpark.web
            from dpark.web.ui import create_app
            app = create_app(self)
            self.web_port = dpark.web.start(app)
            self.webui_url = 'http://%s:%s' % (
                socket.gethostname(),
                self.web_port
            )
            logger.info('start listening on Web UI: %s', self.webui_url)
        except ImportError as e:
            self.webui_url = None
            logger.info('no web server created as %s', e)

        origin_master = master = self.master or options.master
        if master == 'local':
            logger.info("use local scheduler: %s", master)
            self.scheduler = LocalScheduler()
            self.is_local = True
        elif master == 'process':
            logger.info("use process scheduler: %s", master)
            self.scheduler = MultiProcessScheduler(options.parallel)
            self.is_local = False
        else:
            if master == 'mesos':
                master = conf.MESOS_MASTER
            else:
                master = conf.MESOS_MASTERS.get(master, master)

            if master.startswith('mesos://'):
                if '@' in master:
                    master = master[master.rfind('@') + 1:]
                else:
                    master = master[master.rfind('//') + 2:]
            elif master.startswith('zoo://'):
                master = 'zk' + master[3:]

            if ':' not in master:
                master += ':5050'
            self.scheduler = MesosScheduler(
                master, options, webui_url=self.webui_url
            )
            self.data_limit = 1024 * 1024  # 1MB
            self.is_local = False
            logger.info("use mesos scheduler: %s", master)

        self.master = master

        if options.parallel:
            self.defaultParallelism = options.parallel
        else:
            self.defaultParallelism = self.scheduler.defaultParallelism()
        self.defaultMinSplits = max(self.defaultParallelism, 2)

        self.initialized = True
        self.scheduler.is_dstream = self.is_dstream

        logger.info("DparkContext initialized, use master %s -> %s, default_rddconf = %s",
                    origin_master, master,
                    conf.default_rddconf)

    @staticmethod
    def setLogLevel(level):
        get_logger('dpark').setLevel(level)

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

        def create_rdd(_cls, _path, *_ka, **_kw):
            if _cls is TextFileRDD:
                if _path.endswith('.bz2'):
                    return BZip2FileRDD(self, _path, *_ka, **_kw)
                elif _path.endswith('.gz'):
                    return GZipFileRDD(self, _path, *_ka, **_kw)
            return _cls(self, _path, *_ka, **_kw)

        if os.path.isdir(path):
            paths = []
            for root, dirs, names in walk(path, followlinks=followLink):
                if maxdepth > 0:
                    depth = len([_f for _f in root[len(path):].split('/') if _f]) + 1
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

    def tfRecordsFile(self, path, *args, **kwargs):
        return self.textFile(path, cls=TfrecordsRDD, *args, **kwargs)

    def partialTextFile(self, path, begin, end, splitSize=None, numSplits=None):
        self.init()
        return PartialTextFileRDD(self, path, begin, end, splitSize, numSplits)

    def bzip2File(self, *args, **kwargs):
        """deprecated"""
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
                with open(p) as f:
                    fields = f.read().split('\t')

                break
        else:
            raise Exception("no .field_names found in %s" % path)
        return self.tableFile(path, **kwargs).asTable(fields)

    def beansdb(self, path, depth=None, filter=None,
                fullscan=False, raw=False, only_latest=False):
        """(Key, (VALUE, Version, Timestamp)) data in beansdb

        Data structure:
            REC = (Key, TRIPLE)
            TRIPLE = (VALUE, Version, Timestamp)
            VALUE = RAW_VALUE | REAL_VALUE
            RAW_VALUE = (flag, BYTES_VALUE)

        Args:
            path: beansdb data path
            filter: used to filter key
            depth: choice = [None, 0, 1, 2]. e.g. depth=2 assume dir tree like:
                    'path/[0-F]/[0-F]/%03d.data'
                If depth is None, dpark will guess.
            fullscan: NOT use index files, which contain (key, pos_in_datafile).
                pairs.
                Better use fullscan unless the filter selectivity is low.
                Effect of using index:
                    inefficient random access
                    one split(task) for each file instead of each moosefs chunk

                Omitted if filter is None.
            raw: VALUE = RAW_VALUE if raw else REAL_VALUE.
            only_latest: for each key, keeping the REC with the largest
                Timestamp. This will append a reduceByKey RDD.
                Need this because online beansdb data is log structured.
        """

        key_filter = filter

        self.init()
        if key_filter is None:
            fullscan = True
        if isinstance(path, (tuple, list)):
            rdd = self.union([self.beansdb(p, depth, key_filter, fullscan,
                                           raw=True, only_latest=False)
                              for p in path])
        else:
            path = os.path.realpath(path)
            assert os.path.exists(path), "%s no exists" % path
            if os.path.isdir(path):
                subs = []
                if not depth:
                    subs = [os.path.join(path, n) for n in os.listdir(path)
                            if n.endswith('.data')]
                if subs:
                    rdd = self.union([BeansdbFileRDD(self, p, key_filter,
                                                     fullscan, raw=True)
                                      for p in subs])
                else:
                    subs = [os.path.join(path, '%x' % i) for i in range(16)]
                    rdd = self.union([self.beansdb(p, depth and depth - 1, key_filter,
                                                   fullscan, raw=True, only_latest=False)
                                      for p in subs if os.path.exists(p)])
            else:
                rdd = BeansdbFileRDD(self, path, key_filter, fullscan, raw)

        # choose only latest version
        if only_latest:
            num_splits = min(int(ceil(len(rdd) / 4)), 800)
            rdd = rdd.reduceByKey(lambda v1, v2: v1[2] > v2[2] and v1 or v2,
                                  numSplits=num_splits)
        if not raw:
            rdd = rdd.mapValue(lambda v_ver_t: (restore_value(*v_ver_t[0]), v_ver_t[1], v_ver_t[2]))
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
        def shutdown():
            self.stop()
            try:
                import dpark.web
                dpark.web.stop(self.web_port)
            except ImportError:
                pass

        if self.started:
            return

        self.init()

        env.start()
        self.scheduler.start()
        self.started = True
        _shutdown_handlers.append(shutdown)

        spawn_rconsole(locals())

    def runJob(self, rdd, func, partitions=None, allowLocal=False):
        self.start()

        success = False
        if partitions is None:
            partitions = list(range(len(rdd)))
        try:
            gc.disable()
            for it in self.scheduler.runJob(rdd, func, partitions, allowLocal):
                yield it
            success = True
        finally:
            if not success:
                logger.critical('Framework failed')
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


class Parser(optparse.OptionParser):
    def _process_args(self, largs, rargs, values):
        while rargs:
            arg = rargs[0]
            if arg == "--":
                del rargs[0]
                return
            elif arg[0:2] == "--":
                try:
                    self._process_long_opt(rargs, values)
                except optparse.AmbiguousOptionError:
                    raise
                except optparse.BadOptionError:
                    largs.append(arg)
                    return
            elif arg[:1] == "-" and len(arg) > 1:
                try:
                    self._process_short_opts(rargs, values)
                except optparse.AmbiguousOptionError:
                    raise
                except optparse.BadOptionError:
                    largs.append(arg)
                    return
            else:
                return


parser = Parser(usage="Usage: %prog [options] [args]")


def add_default_options():
    group = optparse.OptionGroup(parser, "Dpark Options")

    group.add_option("-m", "--master", type="string", default="local",
                     help="master of Mesos: local, process, host[:port], or mesos://")
    group.add_option("-p", "--parallel", type="int", default=0,
                     help="number of processes")

    group.add_option("-c", "--cpus", type="float", default=1.0,
                     help="cpus used per task")
    group.add_option("-M", "--mem", type="string",
                     help="memory used per task (e.g. 300m, 1g), default unit is 'm'")
    group.add_option("-g", "--group", type="string", default="",
                     help="which group of machines")
    group.add_option("--err", type="float", default=0.0,
                     help="acceptable ignored error record ratio (0.01%)")
    group.add_option("--checkpoint_dir", type="string", default="",
                     help="shared dir to keep checkpoint of RDDs")

    group.add_option("--color", action="store_true")
    group.add_option("--no-color", action="store_false", dest='color')

    group.add_option("--profile", action="store_true",
                     help="do profiling")
    group.add_option("--role", type="string", default="")

    group.add_option("-I", "--image", type="string",
                     help="image name for Docker")
    group.add_option("-V", "--volumes", type="string",
                     help="volumes to mount into Docker")

    parser.add_option_group(group)

    parser.add_option("-q", "--quiet", action="store_true")
    parser.add_option("-v", "--verbose", action="store_true")


add_default_options()


def parse_options():
    options, args = parser.parse_args()
    if options.mem is None:
        options.mem = conf.MEM_PER_TASK
    else:
        options.mem = memory_str_to_mb(options.mem)

    options.logLevel = (options.quiet and logging.ERROR
                        or options.verbose and logging.DEBUG or logging.INFO)
    if options.color is None:
        options.color = getattr(sys.stderr, 'isatty', lambda: False)()

    init_dpark_logger(options.logLevel, use_color=options.color)

    if any(arg.startswith('-') for arg in args):
        logger.warning('unknown args found in command-line: %s', ' '.join(args))

    return options

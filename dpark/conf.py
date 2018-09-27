from __future__ import absolute_import
import os
from dpark.utils.log import get_logger

# override configs use python file at path given by env var $DPARK_CONF, see the end of this file

logger = get_logger(__name__)

# workdir used in slaves for internal files
#
DPARK_WORK_DIR = '/tmp/dpark'
if os.path.exists('/dev/shm'):
    DPARK_WORK_DIR = '/dev/shm,/tmp/dpark'

# uri of mesos master, host[:5050] or or zk://...
MESOS_MASTER = 'localhost'
MESOS_MASTERS = {}

# used for mrun, allow it to get all resources quickly, maybe more than a fair share.
MESOS_MPI_ROLE = "mpi"

# mount points of MooseFS, must be available on all slaves
# for example:  '/mfs' : 'mfsmaster',
MOOSEFS_MOUNT_POINTS = {
}

# dup log to path lick $LOGHUB/$LOGHUB_PATH_FORMAT/$FRAMEWORK_ID/log
# $LOGHUB/$LOGHUB_PATH_FORMAT should exists before run
LOGHUB = None
LOGHUB_PATH_FORMAT = "%Y/%m/%d/%H"

ENABLE_ES_LOGHUB = False
ES_HOST = None
ES_INDEX = None
ES_TYPE = None

# consistant dir cache in client, need patched mfsmaster
MOOSEFS_DIR_CACHE = False

# memory used per task, like -M (--m) option in context.
MEM_PER_TASK = 200.0

MAX_OPEN_FILE = 900
LOG_ROTATE = True
MULTI_SEGMENT_DUMP = True

TIME_TO_SUPPRESS = 60  # sec


OP_UDF = "udf"
OP_GROUPBY = "groupby"
OP_COGROUP = "cogroup"

DEFAULT_TASK_TIME = 3600  # 1 hour

_named_only_start = object()


class RDDConf(object):
    # use default_rddconf to set default values, do NOT change ATTRS
    ATTRS = {
        "disk_merge": False,
        "sort_merge": False,
        "iter_group": False,
        "ordered_group": False,
        "dump_mem_ratio": 0.9,
        "op": OP_UDF,
        "_dummy": _named_only_start,
    }

    def __init__(self, _dummy, disk_merge, sort_merge, iter_group, ordered_group, dump_mem_ratio, op):
        if _dummy != _named_only_start:
            raise TypeError("DO NOT use RDDConf directly; use dpark.conf.rddconf() instead. ")

        self.disk_merge = disk_merge
        self.sort_merge = sort_merge
        self.iter_group = iter_group
        self.ordered_group = ordered_group
        self.dump_mem_ratio = dump_mem_ratio
        self.op = op

    def __setattr__(self, name, value):
        if name not in self.ATTRS:
            msg = "'RDDConf' object has no attribute '{}'. Valid attrs: {}".format(name, self.to_dict().keys())
            raise AttributeError(msg)
        object.__setattr__(self, name, value)

    def to_dict(self):
        d = dict(self.__dict__)
        return d

    def __repr__(self):
        return "RDDConf_%r" % (self.to_dict())

    def dup(self, **kwargs):
        res = RDDConf(_named_only_start, **self.__dict__)
        for k, v in kwargs.items():
            res.__setattr__(k, v)
        return res

    @property
    def is_cogroup(self):
        return self.op == OP_COGROUP

    @property
    def is_groupby(self):
        return self.op == OP_GROUPBY


default_rddconf = RDDConf(**RDDConf.ATTRS)  # for user
_rdd = default_rddconf  # for etc


def rddconf(_dummy=_named_only_start,
            disk_merge=None, sort_merge=None,
            iter_group=False, ordered_group=None,
            dump_mem_ratio=None,
            op=OP_UDF):
    """ Return new RDDConfig object based on default values.
        Only takes named arguments.
        e.g. groupByKey(.., rddconf=dpark.conf.rddconf(...))
    """
    if _dummy != _named_only_start:
        raise TypeError("rddconf() only takes named arguments")
    kwargs = locals()
    kwargs.pop("_dummy")
    kwargs = dict([kv for kv in kwargs.items() if kv[1] is not None])
    res = default_rddconf.dup(**kwargs)
    return res


def ban(hostname):
    return False


def load_conf(path):
    if not os.path.exists(path):
        logger.warning("conf %s do not exists, use default config", path)
        return

    try:
        with open(path) as f:
            data = f.read()
            exec(data, globals(), globals())
    except Exception as e:
        logger.error("error while load conf from %s: %s", path, e)
        raise


load_conf(os.environ.get('DPARK_CONF', '/etc/dpark.conf'))

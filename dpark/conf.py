from __future__ import absolute_import
import os
from dpark.util import get_logger


logger = get_logger(__name__)

# workdir used in slaves for internal files
#
DPARK_WORK_DIR = '/tmp/dpark'
if os.path.exists('/dev/shm'):
    DPARK_WORK_DIR = '/dev/shm,/tmp/dpark'

# uri of mesos master, host[:5050] or or zk://...
MESOS_MASTER = 'localhost'

# mount points of MooseFS, must be available on all slaves
# for example:  '/mfs' : 'mfsmaster',
MOOSEFS_MOUNT_POINTS = {
}
# consistant dir cache in client, need patched mfsmaster
MOOSEFS_DIR_CACHE = False

# memory used per task, like -M (--m) option in context.
MEM_PER_TASK = 200.0


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


LOG_ROTATE = True
MULTI_SEGMENT_DUMP = True
DUMP_MEM_RATIO = 0.9

class ShuffleConfig(object):

    # configured in /etc/dpark.conf
    default = None
    MAX_OPEN_FILE = 900

    def __init__(self, sort_merge=False, use_disk=False, iter_group=False, keep_order=False):
        self.is_sort_merge = sort_merge
        self.is_disk_merge = use_disk
        self.is_iter_group = iter_group
        self.is_ordered_group = keep_order

        self.dump_mem_ratio = DUMP_MEM_RATIO

        # used internal
        self.op = "udf" # 'udf' or 'group' or 'cogroup'

    def __repr__(self):
        return "ShuffleConfig%r" % (self.__dict__)

    @classmethod
    def new(cls):
        return cls(False, False, False, False)

    def dup(self):
        sc = ShuffleConfig()
        sc.__dict__ = dict(self.__dict__)
        return sc

    def sort(self):
        self.is_sort_merge = True
        return self

    def hash(self):
        self.is_sort_merge = False
        return self

    def mem(self):
        self.is_disk_merge = False
        return self

    def disk(self, dump_mem_ratio=None):
        self.is_disk_merge = True
        if dump_mem_ratio:
            self.dump_mem_ratio = dump_mem_ratio
        return self

    def order(self):
        self.is_ordered_group = True
        return self

    def no_order(self):
        self.is_ordered_group = False
        return self

    def iter_group(self):
        self.is_iter_group = True
        return self

    def list_group(self):
        self.is_iter_group = False
        return self

    def cogroup(self):
        self.op = 'cogroup'
        return self

    def groupby(self):
        self.op = 'groupby'
        return self

    @property
    def is_cogroup(self):
        return self.op == 'cogroup'

    @property
    def is_groupby(self):
        return self.op == 'groupby'


ShuffleConfig.default =  ShuffleConfig(False, False, False, False)
load_conf(os.environ.get('DPARK_CONF', '/etc/dpark.conf'))


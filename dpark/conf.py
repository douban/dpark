import os.path
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
        logger.error("conf %s do not exists", path)
        raise Exception("conf %s do not exists" % path)

    try:
        data = open(path).read()
        exec data in globals(), globals()
    except Exception, e:
        logger.error("error while load conf from %s: %s", path, e)
        raise

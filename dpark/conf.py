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

load_conf(os.environ.get('DPARK_CONF', '/etc/dpark.conf'))

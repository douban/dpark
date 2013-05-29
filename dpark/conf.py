import os.path
import logging

logger = logging.getLogger(__name__)

# workdir used in slaves for internal files
# 
DPARK_WORK_DIR = '/tmp/dpark'
if os.path.exists('/dev/shm'):
    DPARK_WORK_DIR = '/dev/shm,/tmp/dpark'

# default port of web server in slaves
DEFAULT_SERVER_PORT = 5055

# uri of mesos master, host[:5050] or or zk://...
MESOS_MASTER = 'localhost'

# mount points of MooseFS, must be available on all slaves
# for example:  '/mfs' : 'mfsmaster',
MOOSEFS_MOUNT_POINTS = {
}

def load_conf(path):
    if not os.path.exists(path):
        logger.error("conf %s do not exists", path)
        return

    try:
        data = open(path).read()
        exec data in globals(), globals()
    except Exception, e:
        logger.error("error while load conf from %s: %s", path, e)
        raise 

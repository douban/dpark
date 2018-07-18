from __future__ import absolute_import
from __future__ import print_function
import socket
import getpass
import sys
from cProfile import Profile
from pstats import Stats
from tempfile import NamedTemporaryFile
from functools import wraps
from datetime import datetime


def profile(hostname=None, to_stdout=False):
    def print_stats(stats):
        stats.strip_dirs()
        stats.sort_stats('time', 'calls')
        stats.print_stats(20)
        stats.sort_stats('cumulative')
        stats.print_stats(20)

    def decorator(f):

        @wraps(f)
        def _(*args, **kwargs):
            prof = Profile()
            try:
                return prof.runcall(f, *args, **kwargs)
            finally:
                if to_stdout:
                    stats = Stats(prof)
                    print_stats(stats)
                else:
                    with NamedTemporaryFile(prefix='dpark_profile_', delete=False) as fd:
                        print('===\n', datetime.today(), getpass.getuser(), sys.argv[0], file=fd)
                        stats = Stats(prof, stream=fd)
                        print_stats(stats)

        return _

    if hostname is None or socket.gethostname() == hostname:
        return decorator
    else:
        return lambda f: f

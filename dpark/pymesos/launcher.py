import os, sys
import subprocess
import pwd
import logging

logger = logging.getLogger(__name__)

class Launcher(object):
    """
    This class sets up the environment for an executor and then exec()'s it.
    It can either be used after a fork() in the slave process, or run as a
    standalone program (with the main function in launcher_main.cpp).

    The environment is initialized through for steps:
        1) A work directory for the framework is created by createWorkingDirectory().
        2) The executor is fetched off HDFS if necessary by fetchExecutor().
        3) Environment variables are set by setupEnvironment().
        4) We switch to the framework's user in switchUser().

        Isolation modules that wish to override the default behaviour can subclass
        Launcher and override some of the methods to perform extra actions.
    """
    def __init__(self, framework_id, executor_id, commandInfo, user=None, workDirectory='/tmp',
            slavepid=None, redirectIO=False, switch_user=False):
        self.__dict__.update(locals())

    def run(self):
        self.initializeWorkingDirectory()
        os.chdir(self.workDirectory)
        if self.switch_user:
            self.switchUser()
        if self.redirectIO:
            sys.stdout = open('stdout', 'w')
            sys.stderr = open('stderr', 'w')
        command = self.commandInfo.value
        env = self.setupEnvironment()
        p = subprocess.Popen(['/bin/sh', '-c', command], stdout=sys.stdout,
                stderr=sys.stderr, env=env)
        p.wait()

    def setupEnvironment(self):
        env = {}
        for en in self.commandInfo.environment.variables:
            env[en.name] = en.value
        env['MESOS_DIRECTORY'] = self.workDirectory
        env['MESOS_SLAVE_PID'] = str(self.slavepid)
        env['MESOS_FRAMEWORK_ID'] = self.framework_id.value
        env['MESOS_EXECUTOR_ID'] = self.executor_id.value
        env['LIBPROCESS_PORT'] = '0'
        return env

    def initializeWorkingDirectory(self):
        if self.switch_user:
            try:
                os.chown(self.user, self.workDirectory)
            except IOError, e:
                logger.error("failed to chown: %s", e)

    def switchUser(self):
        try:
            pw = pwd.getpwnam(self.user)
            os.setuid(pw.pw_uid)
            os.setgid(pw.pw_gid)
        except OSError, e:
            logger.error("failed to swith to user %s: %s", self.user, e)

def main():
    from mesos_pb2 import FrameworkID, ExecutorID, CommandInfo
    fid = FrameworkID()
    fid.value = os.environ.get('MESOS_FRAMEWORK_ID', 'fid')
    eid = ExecutorID()
    eid.value = os.environ.get('MESOS_EXECUTOR_ID', 'eid')
    info = CommandInfo()
    info.value = os.environ.get('MESOS_COMMAND')
    return Launcher(fid, eid, info).run()

if __name__ == '__main__':
    main()

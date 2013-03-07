import os, sys
import time
import logging
import socket
import threading
import signal

from process import UPID, Process
from launcher import Launcher

from mesos_pb2 import *
from messages_pb2 import *

logger = logging.getLogger("slave")

class Resources(object):
    def __init__(self, cpus, mem):
        self.cpus = cpus
        self.mem = mem

    def __iadd__(self, other):
        if not isinstance(other, Resources):
            other = Resources.new(other)
        self.cpus += other.cpus
        self.mem += other.mem
        return self

    def __isub__(self, other):
        if not isinstance(other, Resources):
            other = Resources.new(other)
        self.cpus -= other.cpus
        self.mem -= other.mem
        return self

    @classmethod
    def new(cls, rs):
        self = cls(0, 0)
        for r in rs:
            self.__dict__[r.name] = r.scalar.value
        return self


class Executor(object):
    def __init__(self, framework_id, info, directory):
        self.framework_id = framework_id
        self.info = info
        self.directory = directory
        
        self.id = info.executor_id
        self.resources = Resources.new(info.resources)
        self.uuid = os.urandom(4)
        
        self.pid = None
        self.shutdown = False
        self.launchedTasks = {}
        self.queuedTasks = {}

    def addTask(self, task):
        assert task.task_id.value not in self.launchedTasks
        t = Task()
        t.framework_id.MergeFrom(self.framework_id)
        t.state = TASK_STAGING
        t.name = task.name
        t.task_id.MergeFrom(task.task_id)
        t.slave_id.MergeFrom(task.slave_id)
        t.resources.MergeFrom(task.resources)
        if task.command:
            t.executor_id.MergeFrom(self.id)

        self.launchedTasks[task.task_id.value] = t
        self.resources += task.resources
        return t

    def removeTask(self, task_id):
        tid = task_id.value
        self.queuedTasks.pop(tid, None)
        if tid in self.launchedTasks:
            self.resources -= self.launchedTasks[tid].resources
            del self.launchedTasks[tid]

    def updateTaskState(self, task_id, state):
        if task_id.value in self.launchedTasks:
            self.launchedTasks[task_id.value].state = state


class Framework(object):
    def __init__(self, id, info, pid, conf):
        self.id = id
        self.info = info
        self.pid = pid
        self.conf = conf

        self.executors = {}
        self.updates = {}

    def get_executor_info(self, task):
        assert task.HasField('executor') != task.HasField('command')
        if task.HasField('executor'):
            return task.executor

        info = ExecutorInfo()
        fmt = "Task %s (%s)"
        if len(task.command.value) > 15:
            id = fmt % (task.task_id.value, task.command.value[:12] + '...')
        else:
            id = fmt % (task.task_id.value, task.command.value)
        info.executor_id.value = id
        info.command.value = task.command.value
        return info

    def createExecutor(self, info, directory):
        executor = Executor(self.id, info, directory)
        self.executors[executor.id.value] = executor
        return executor

    def destroyExecutor(self, exec_id):
        self.executors.pop(exec_id.value, None)

    def get_executor(self, exec_id):
        if isinstance(exec_id, TaskID):
            tid = exec_id.value
            for executor in self.executors.values():
                if tid in executor.queuedTasks or tid in executor.launchedTasks:
                    return executor
            return 
        return self.executors.get(exec_id.value)
    getExecutor = get_executor


class ProcessInfo(object):
    def __init__(self, fid, eid, pid, directory):
        self.framework_id = fid
        self.executor_id = eid
        self.pid = pid
        self.directory = directory


class IsolationModule(object):
    def __init__(self):
        self.infos = {}
        self.pids = {}

    def initialize(self, slave):
        self.slave = slave
        t = threading.Thread(target=self.reaper)
        t.daemon = True
        t.start()

    def launchExecutor(self, framework_id, info, executor_info, directory, resources):
        fid = framework_id.value
        eid = executor_info.executor_id.value
        logger.info("Launching %s (%s) in %s with resources %s for framework %s", 
                eid, executor_info.command.value, directory, resources, fid)
        pid = os.fork()
        assert pid >= 0
        if pid:
            logger.info("Forked executor at %d", pid)
            pinfo = ProcessInfo(framework_id, executor_info.executor_id, pid, directory)
            self.infos.setdefault(fid, {})[eid] = pinfo
            self.pids[pid] = pinfo
            self.slave.executorStarted(framework_id, executor_info.executor_id, pid)
        else:
            #pid = os.setsid()
            # copy UPID of slave
            launcher = Launcher(framework_id, executor_info.executor_id, executor_info.command, 
                    info.user, directory, self.slave)
            launcher.run()

    def killExecutor(self, framework_id, executor_id):
        pinfo = self.infos.get(framework_id.value, {}).get(executor_id.value)
        if pinfo is None:
            logger.error("ERROR! Asked to kill an unknown executor! %s", executor_id.value)
            return
        pid = pinfo.pid
        os.kill(pid, signal.SIGKILL)
        # remove when waitpid()
        #self._removeExecutor(pinfo) 

    def removeExecutor(self, e):
        logger.info("remove executor: %s", e)
        if e.framework_id.value not in self.infos:
            return
        self.infos[e.framework_id.value].pop(e.executor_id.value, None)
        if not self.infos[e.framework_id.value]:
            del self.infos[e.framework_id.value]
        self.pids.pop(e.pid)

    def resourcesChanged(self, framework_id, executor_id, resources):
        pass

    def reaper(self):
        while True:
            if not self.pids:
                time.sleep(1)
                continue

            try:
                pid, status = os.waitpid(-1, 0)
            except OSError, e:
                time.sleep(1)
                logger.error("waitpid: %s", e)
                continue

            if pid > 0 and pid in self.pids and not os.WIFSTOPPED(status):
                e = self.pids[pid]
                logger.info("Telling slave of lost executor %s of framework %s",
                        e.executor_id, e.framework_id)
                self.slave.executorExited(e.framework_id, e.executor_id, status)
                self.removeExecutor(e)


def isTerminalTaskState(state):
    return state in (TASK_FINISHED, TASK_FAILED, TASK_KILLED, TASK_LOST)


class Slave(Process):
    def __init__(self, options):
        Process.__init__(self, "slave")
        self.options = options
        self.resources = Resources(options.cpus, options.mem)
        self.attributes = options.attributes

        self.id = None
        self.isolation = IsolationModule()
        self.info = self.getSlaveInfo() 
        self.master = UPID('master', options.master)
        self.frameworks = {}
        self.startTime = time.time()
        self.connected = False
   
    def getSlaveInfo(self):
        info = SlaveInfo()
        info.hostname = socket.gethostname()
        info.webui_hostname = ''
        info.webui_port = 8081
        cpus = info.resources.add()
        cpus.name = 'cpus'
        cpus.type = 0
        cpus.scalar.value = self.resources.cpus
        mem = info.resources.add()
        mem.name = 'mem'
        mem.type = 0
        mem.scalar.value = self.resources.mem
        if self.attributes:
            for attrs in self.attributes.split(','):
                name,value = attrs.split(':')
                a = self.info.attributes.add()
                a.name = name
                a.type = Value.TEXT # 3 # TEXT
                a.text.value = value
        return info

    def getFramework(self, framework_id):
        return self.frameworks.get(framework_id.value)
    
    def onNewMasterDetectedMessage(self, pid):
        self.master = UPID(pid)
        self.register()

    def onNoMasterDetectedMessage(self):
        self.master = None
        self.connected = False

    def register(self):
        if self.id is None:
            msg = RegisterSlaveMessage()
            msg.slave.MergeFrom(self.info)
        else:
            msg = ReregisterSlaveMessage()
            msg.slave_id.MergeFrom(self.id)
            msg.slave.MergeFrom(self.info)
            for framework in self.frameworks.itervalues():
                for executor in framework.executors.values():
                    msg.executor_infos.add().MergeFrom(executor.info)
                    for task in executor.launchedTasks.itervalues():
                        msg.tasks.add().MergeFrom(task)
        return self.send(self.master, msg)

    def onSlaveRegisteredMessage(self, slave_id):
        logger.info("slave registed %s", slave_id.value)
        self.id = slave_id
        self.connected = True

    def onSlaveReregisteredMessage(self, slave_id):
        assert self.id == slave_id
        self.connected = True

    def onRunTaskMessage(self, framework_id, framework_info, pid, task):
        logger.info("Got assigned task %s for framework %s", 
                task.task_id.value, framework_id.value)
        fid = framework_id.value
        if fid not in self.frameworks:
            framework = Framework(framework_id, framework_info, UPID(pid), self.options)
            self.frameworks[fid] = framework
        else:
            framework = self.frameworks[fid]

        executorInfo = framework.get_executor_info(task)
        eid = executorInfo.executor_id
        executor = framework.getExecutor(eid)
        if executor:
            if executor.shutdown:
                logger.warning("WARNING! executor is shuting down")
            elif not executor.pid:
                executor.queuedTasks[task.task_id.value] = task
            else:
                executor.addTask(task)
                #self.isolation.resourcesChanged(framework_id, executor.id, executor.resources)

                msg = RunTaskMessage()
                msg.framework.MergeFrom(framework.info)
                msg.framework_id.MergeFrom(framework_id)
                msg.pid = str(framework.pid)
                msg.task.MergeFrom(task)
                self.send(executor.pid, msg)
        else:
            directory = self.createUniqueWorkDirectory(framework.id, eid)
            executor = framework.createExecutor(executorInfo, directory)
            executor.queuedTasks[task.task_id.value] = task
            self.isolation.launchExecutor(framework.id, framework.info, executor.info,
                    directory, executor.resources)

    def onKillTaskMessage(self, framework_id, task_id):
        framework = self.getFramework(framework_id)
        if not framework or not framework.getExecutor(task_id):
            msg = StatusUpdateMessage()
            update = msg.update
            update.framework_id.MergeFrom(framework_id)
            update.slave_id.MergeFrom(self.id)
            update.status.task_id.MergeFrom(task_id)
            update.status.state = TASK_LOST
            update.timestamp = time.time()
            update.uuid = os.urandom(4)
            return self.send(self.master, msg)
        
        executor = framework.getExecutor(task_id)
        if not executor.pid:
            executor.removeTask(task_id)
            msg = StatusUpdateMessage()
            update = msg.update
            update.framework_id.MergeFrom(framework_id)
            update.slave_id.MergeFrom(self.id)
            update.status.task_id.MergeFrom(task_id)
            update.status.state = TASK_KILLED
            update.timestamp = time.time()
            update.uuid = os.urandom(4)
            return self.send(self.master, msg)

        msg = KillTaskMessage()
        msg.framework_id.MergeFrom(framework_id)
        msg.task_id.MergeFrom(task_id)
        return self.send(executor.pid, msg)

    def onShutdownFrameworkMessage(self, framework_id):
        framework = self.getFramework(framework_id)
        if framework:
            for executor in framework.executors.values():
                self.shutdownExecutor(framework, executor)

    def shutdownExecutor(self, framework, executor):
        logger.info("shutdown %s %s", framework.id.value, executor.id.value)
        self.send(executor.pid, ShutdownExecutorMessage())
        executor.shutdown = True

        # delay check TODO
        time.sleep(3)
        self.isolation.killExecutor(framework.id, executor.id)
        framework.destroyExecutor(executor.id)
        if not framework.executors and not framework.updates:
            self.frameworks.pop(framework.id.value)

    def onUpdateFrameworkMessage(self, framework_id, pid):
        framework = self.getFramework(framework_id)
        if framework:
            framework.pid = pid

    def executorStarted(self, framework_id, executor_id, pid):
        pass

    def executorExited(self, framework_id, executor_id, status):
        logger.info("Executor %s of framework %s exited with %d",
                executor_id.value, framework_id.value, status)
        framework = self.getFramework(framework_id)
        if not framework:
            return
        executor = framework.getExecutor(executor_id)
        if not executor:
            return

        isCommandExecutor = False
        for task in executor.launchedTasks.values():
            if not isTerminalTaskState(task.state):
                isCommandExecutor = not task.HasField('executor_id')
                self.transitionLiveTask(task.task_id, executor_id, 
                    framework_id, isCommandExecutor, status)

        for task in executor.queuedTasks.values():
            isCommandExecutor = task.HasField('command')
            self.transitionLiveTask(task.task_id, executor_id, 
                    framework_id, isCommandExecutor, status)

        if not isCommandExecutor:
            msg = ExitedExecutorMessage()
            msg.slave_id.MergeFrom(self.id)
            msg.framework_id.MergeFrom(framework_id)
            msg.executor_id.MergeFrom(executor_id)
            msg.status = status
            self.send(self.master, msg)
        framework.destroyExecutor(executor_id)

    def onRegisterExecutorMessage(self, framework_id, executor_id):
        framework = self.getFramework(framework_id)
        if not framework:
            # TODO shutdown executor
            return 
        executor = framework.getExecutor(executor_id)
        if not executor or executor.pid or executor.shutdown:
            # TODO shutdown executor
            return
        executor.pid = self.sender

        msg = ExecutorRegisteredMessage()
        msg.executor_info.MergeFrom(executor.info)
        msg.framework_id.MergeFrom(framework_id)
        msg.framework_info.MergeFrom(framework.info)
        msg.slave_id.MergeFrom(self.id)
        msg.slave_info.MergeFrom(self.info)
        self.send(executor.pid, msg)

        for task in executor.queuedTasks.values():
            msg = RunTaskMessage()
            msg.framework_id.MergeFrom(framework.id)
            msg.framework.MergeFrom(framework.info)
            msg.pid = str(framework.pid)
            msg.task.MergeFrom(task)
            self.send(executor.pid, msg)

        for task in executor.queuedTasks.values():
            executor.addTask(task)
        executor.queuedTasks.clear()

    def onStatusUpdateMessage(self, update):
        status = update.status
        framework = self.getFramework(update.framework_id)
        if not framework:
            return
        executor = framework.getExecutor(status.task_id)
        if not executor:
            return
        executor.updateTaskState(status.task_id, status.state)
        if isTerminalTaskState(status.state):
            executor.removeTask(status.task_id)

        msg = StatusUpdateMessage()
        msg.update.MergeFrom(update)
        msg.pid = str(self) # pid
        self.send(self.master, msg)

        framework.updates[update.uuid] = update
    
    def onStatusUpdateAcknowledgementMessage(self, slave_id, framework_id, task_id, uuid):
        framework = self.getFramework(framework_id)
        if framework and uuid in framework.updates:
            framework.updates.pop(uuid)
            if not framework.executors and not framework.updates:
                self.frameworks.pop(framework_id.value)

    def onFrameworkToExecutorMessage(self, slave_id, framework_id, executor_id, data):
        framework = self.getFramework(framework_id)
        if not framework:
            return
        executor = framework.getExecutor(executor_id)
        if not executor:
            return
        if not executor.pid:
            return
        msg = FrameworkToExecutorMessage()
        msg.slave_id.MergeFrom(slave_id)
        msg.framework_id.MergeFrom(framework_id)
        msg.executor_id.MergeFrom(executor_id)
        msg.data = data
        self.send(executor.pid, msg)
    
    def onExecutorToFrameworkMessage(self, slave_id, framework_id, executor_id, data):
        framework = self.getFramework(framework_id)
        if not framework:
            return
        msg = ExecutorToFrameworkMessage()
        msg.slave_id.MergeFrom(slave_id)
        msg.framework_id.MergeFrom(framework_id)
        msg.executor_id.MergeFrom(executor_id)
        msg.data = data
        self.send(framework_id.pid, msg)

    def onShutdownMessage(self):
        self.stop()

    def start(self):
        Process.start(self)
        self.isolation.initialize(self)
        self.register() # master detector TODO

    def onPing(self):
        self.reply("PONG")

    def createStatusUpdate(self, task_id, executor_id, framework_id,
            taskState, reason):
        status = TaskStatus()
        status.task_id.MergeFrom(task_id)
        status.state = taskState
        status.message = reason

        update = StatusUpdate()
        update.framework_id.MergeFrom(framework_id)
        update.slave_id.MergeFrom(self.id)
        update.executor_id.MergeFrom(executor_id)
        update.status.MergeFrom(status)
        update.timestamp = time.time()
        update.uuid = os.urandom(4)

        return update

    def statusUpdate(self, update):
        logger.info("status update")
        status = update.status
        framework = self.getFramework(update.framework_id)
        if not framework:
            return
        executor = framework.getExecutor(status.task_id)
        if not executor:
            return
        if isTerminalTaskState(status.state):
            executor.removeTask(status.task_id)
        msg = StatusUpdateMessage()
        msg.update.MergeFrom(update)
        msg.pid = str(self)
        self.send(self.master, msg)
        # check
        framework.updates[update.uuid] = update

    def transitionLiveTask(self, task_id, executor_id, framework_id, 
            isCommandExecutor, status):
        if isCommandExecutor:
            update = self.createStatusUpdate(task_id, executor_id, 
                    framework_id, TASK_FAILED, "Executor running the task's command failed")
        else:
            update = self.createStatusUpdate(task_id, executor_id, 
                    framework_id, TASK_LOST, "Executor exited")
        self.statusUpdate(update)

    def createUniqueWorkDirectory(self, framework_id, executor_id):
        root = self.options.work_dir
        path = os.path.join(root, 'slaves', self.id.value, 
                'frameworks', framework_id.value, 
                'executors', executor_id.value, 'runs')
        for i in range(10000):
            p = os.path.join(path, str(i))
            if not os.path.exists(p):
                os.makedirs(p)
                return p

def main():
    import optparse
    parser = optparse.OptionParser(usage="Usage: %prog [options]")
    parser.add_option("-s", "--master", type="string", default="localhost:5050",
            help="May be one of:\n"
              " host[:5050]"
              "  host:port\n"
              "  zk://host1:port1,host2:port2,.../path\n"
              "  zk://username:password@host1:port1,host2:port2,.../path\n"
              "  file://path/to/file (where file contains one of the above)")
    parser.add_option("-c", "--cpus", type="int", default=2)
    parser.add_option("-m", "--mem", type="int", default=1024)
    parser.add_option("-a", "--attributes", type="string")
    parser.add_option("-w", "--work_dir", type="string", default="/tmp/mesos")
    parser.add_option("-q", "--quiet", action="store_true")
    parser.add_option("-v", "--verbose", action="store_true")

    options, args = parser.parse_args()
    logging.basicConfig(format='[slave] %(asctime)-15s %(message)s',
                    level=options.quiet and logging.ERROR
                        or options.verbose and logging.DEBUG
                        or logging.WARNING)
    
    slave = Slave(options)
    slave.run()

if __name__ == '__main__':
    main()

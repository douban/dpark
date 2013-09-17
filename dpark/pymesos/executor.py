import os, sys
import time

from process import UPID, Process, async

from mesos_pb2 import FrameworkID, ExecutorID
from messages_pb2 import RegisterExecutorMessage, ExecutorToFrameworkMessage, StatusUpdateMessage

class Executor(object):
    #def disconnected(self, driver): pass
    #def error(self, driver, message): pass
    def registered(self, driver, executrInfo, frameworkInfo, slaveInfo): pass
    def launchTask(self, driver, task): pass
    def killTask(self, driver, taskId): pass
    def frameworkMessage(self, driver, message): pass
    def shutdown(self, driver): pass


class ExecutorDriver(object):
    def start(self): pass
    def join(self): pass
    def run(self): pass
    def abort(self): pass
    def stop(self): pass
    def sendStatusUpdate(self, update): pass
    def sendFrameworkMessage(self, data): pass

class MesosExecutorDriver(Process, ExecutorDriver):
    def __init__(self, executor):
        Process.__init__(self, 'executor')
        self.executor = executor

        env = os.environ
        self.local = bool(env.get('MESOS_LOCAL'))
        slave_pid = env.get('MESOS_SLAVE_PID')
        assert slave_pid, 'expecting MESOS_SLAVE_PID in environment'
        self.slave = UPID(slave_pid)
        self.framework_id = FrameworkID()
        self.framework_id.value = env.get('MESOS_FRAMEWORK_ID')
        self.executor_id = ExecutorID()
        self.executor_id.value = env.get('MESOS_EXECUTOR_ID')
        self.workDirectory = env.get('MESOS_DIRECTORY')

    def onExecutorRegisteredMessage(self, executor_info, framework_id,
            framework_info, slave_id, slave_info):
        assert framework_id == self.framework_id
        self.slave_id = slave_id
        return self.executor.registered(self, executor_info, framework_info,
            slave_info)

    def onRunTaskMessage(self, framework_id, framework, pid, task):
        return self.executor.launchTask(self, task)

    def onKillTaskMessage(self, framework_id, task_id):
        return self.executor.killTask(self, task_id)

    def onFrameworkToExecutorMessage(self, slave_id, framework_id,
            executor_id, data):
        return self.executor.frameworkMessage(self, data)

    def onShutdownExecutorMessage(self):
        self.executor.shutdown(self)
        if not self.local:
            sys.exit(0)
        else:
            self.stop()

    def onStatusUpdateAcknowledgementMessage(self, slave_id, framework_id, task_id, uuid):
        pass

    def start(self):
        Process.start(self)
        msg = RegisterExecutorMessage()
        msg.framework_id.MergeFrom(self.framework_id)
        msg.executor_id.MergeFrom(self.executor_id)
        return self.send(self.slave, msg)

    @async
    def sendFrameworkMessage(self, data):
        msg = ExecutorToFrameworkMessage()
        msg.slave_id.MergeFrom(self.slave_id)
        msg.framework_id.MergeFrom(self.framework_id)
        msg.executor_id.MergeFrom(self.executor_id)
        msg.data = data
        return self.send(self.slave, msg)

    @async
    def sendStatusUpdate(self, status):
        msg = StatusUpdateMessage()
        msg.update.framework_id.MergeFrom(self.framework_id)
        msg.update.executor_id.MergeFrom(self.executor_id)
        msg.update.slave_id.MergeFrom(self.slave_id)
        msg.update.status.MergeFrom(status)
        msg.update.timestamp = time.time()
        msg.update.uuid = os.urandom(16)
        return self.send(self.slave, msg)

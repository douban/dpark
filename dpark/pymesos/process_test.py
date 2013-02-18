import sys, os
import time

from process import *

class Master(Process):
    def __init__(self):
        Process.__init__(self, 'master')

    def onRegisterFrameworkMessage(self, framework):
        #print 'got framework', framework
        reply = FrameworkRegisteredMessage()
        reply.framework_id.value = 'framework_id'
        reply.master_info.id = 'master_id'
        reply.master_info.ip = 123
        reply.master_info.port = bind_port
        self.send(sender, reply)

class TestSchedulerProcess(Process):
    def __init__(self, master_addr):
        self.master = UPID('master', master_addr)
        self.framework_id = None
        Process.__init__(self, 'scheduler')

    def register(self):
        if self.framework_id is None:
            msg = RegisterFrameworkMessage()
            msg.framework.user = 'davies'
            msg.framework.name = 'test framework'
            msg.framework.executor.executor_id.value = 'default'
            msg.framework.executor.uri = ''
        else:
            msg = ReregisterFrameworkMessage()
            msg.framework.MergeFrom(self.framework)
            msg.framework_id = self.framework_id
            msg.failover = self.failover
        self.send(self.master, msg)
        # redo after 1 second

    def unregister(self):
        msg = UnregisterFrameworkMessage()
        msg.framework_id.value = self.framework_id
        self.send(self.master, msg)

    def onFrameworkRegisteredMessage(self, framework_id):
        self.framework_id = framework_id.value
        self.connected = True
        self.failover = False


def test():
    initialize()

    #master = Master()
    #sched = SchedulerProcess(master.addr)
    #sched.register()
    sched = TestSchedulerProcess('theoden:5050')
    sched.register()
    time.sleep(1)
    sched.unregister()
    time.sleep(10)

if __name__ == '__main__':
    test()

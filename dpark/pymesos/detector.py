
import time

from kazoo.client import KazooClient, KazooState
from kazoo.recipe.watchers import ChildrenWatch, DataWatch
from kazoo.exceptions import ZookeeperError 
from process import UPID, Process
from messages_pb2 import *


class MasterDetector(Process):
    def __init__(self, uri, sched):
        Process.__init__(self, 'detector')
        self.uri = uri
        self.sched = sched
        self.zk = KazooClient(uri, 10)
        self.masterSeq = None

    def choose(self, children):
        if not children:
            self.send(self.sched, NoMasterDetectedMessage())
            return True
        masterSeq = max(children)
        if masterSeq == self.masterSeq:
            return True
        self.masterSeq = masterSeq
        DataWatch(self.zk, '/' + masterSeq, self.notify)
        return True

    def notify(self, master_addr, _):
        msg = NewMasterDetectedMessage()
        msg.pid = master_addr
        self.send(self.sched, msg)
        return False

    def start(self):
        Process.start(self)
        self.zk.start()
        try:
            ChildrenWatch(self.zk, '', self.choose)
        except ZookeeperError:
            self.send(self.sched, NoMasterDetectedMessage())
            self.stop()

    def stop(self):
        self.zk.stop()
        Process.stop(self)


def test():
    import logging
    logging.basicConfig()
    d = MasterDetector('zk://zk1:2181/mesos_master3', UPID('scheduler', 'a:1'))
    d.start()
    time.sleep(2)

if __name__ == '__main__':
    test()

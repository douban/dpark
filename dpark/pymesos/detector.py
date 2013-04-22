from kazoo.client import KazooClient, KazooState
from kazoo.recipe.watchers import ChildrenWatch, DataWatch
from kazoo.exceptions import ZookeeperError 

def adjust_kazoo_logging_level():
    import logging
    import kazoo
    kazoo.client.log.setLevel(logging.WARNING)
    kazoo.protocol.connection.log.setLevel(logging.WARNING)

class MasterDetector(object):
    def __init__(self, uri, agent):
        self.uri = uri
        self.agent = agent
        self.zk = KazooClient(uri, 10)
        self.masterSeq = None

    def choose(self, children):
        if not children:
            self.agent.onNoMasterDetectedMessage()
            return True
        masterSeq = max(children)
        if masterSeq == self.masterSeq:
            return True
        self.masterSeq = masterSeq
        DataWatch(self.zk, '/' + masterSeq, self.notify)
        return True

    def notify(self, master_addr, _):
        self.agent.onNewMasterDetectedMessage(master_addr)
        return False

    def start(self):
        adjust_kazoo_logging_level()
        self.zk.start()
        try:
            ChildrenWatch(self.zk, '', self.choose)
        except ZookeeperError:
            self.agent.onNoMasterDetectedMessage()
            self.stop()

    def stop(self):
        try: self.zk.stop()
        except: pass


def test():
    import logging
    import time
    logging.basicConfig()
    class Agent:
        def onNewMasterDetectedMessage(self, addr):
            print 'got', addr
        def onNoMasterDetectedMessage(self):
            print 'no master'
    d = MasterDetector('zk1:2181/mesos_master2', Agent())
    d.start()
    time.sleep(2)

if __name__ == '__main__':
    test()

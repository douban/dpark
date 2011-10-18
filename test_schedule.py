
from task import *
from schedule import *
from env import env

class MockDriver():
    def reviveOffers(self):
        print 'revive offers'

    def replyToOffer(self, oid, tasks, param):
        print 'got reply for', oid, len(tasks), param
#        for t in tasks:
#            print t

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    env.start(True)
    sched = MesosScheduler('mesos://localhost:5050')
    driver = MockDriver()
    #sched.start()
    sched.driver = driver
    sched.getExecutorInfo(driver)
    
    tasks = [Task() for i in range(10)]
    sched.registered(driver, "test")
    sched.submitTasks(tasks)

    offer = mesos_pb2.SlaveOffer()
    offer.hostname = 'localhost'
    offer.slave_id.value = '1'
    cpus = offer.resources.add()
    cpus.name = 'cpus'
    cpus.type = mesos_pb2.Resource.SCALAR
    cpus.scalar.value = 15
    mem = offer.resources.add()
    mem.name = 'mem'
    mem.type = mesos_pb2.Resource.SCALAR
    mem.scalar.value = 2000
    sched.resourceOffer(driver, "1", [offer])
    
    status = mesos_pb2.TaskStatus()
    status.state = 3
    status.task_id.value = "1"
    status.data = pickle.dumps((1, '', [], {}))
    sched.statusUpdate(driver, status)
    
    sched.resourceOffer(driver, "2", [offer])

    status.state = 2
    for i in range(10):
        status.task_id.value = str(tasks[i].id)
        sched.statusUpdate(driver, status)

    sched.error(driver, 1, 'error')

#!/usr/bin/env python
import logging
import os, sys, time
import os.path
import marshal
import cPickle
import multiprocessing
import threading
import SocketServer
import SimpleHTTPServer
import shutil
import socket
import urllib2
import platform
import gc
gc.disable()

import zmq
import mesos
import mesos_pb2

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(s):
        pass

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dpark.serialize import marshalable
from dpark.accumulator import Accumulator
from dpark.schedule import Success, OtherFailure
from dpark.env import env
from dpark.shuffle import LocalFileShuffle
from dpark.broadcast import Broadcast

logger = logging.getLogger("executor")

TASK_RESULT_LIMIT = 1024 * 256
DEFAULT_WEB_PORT = 5055
MAX_TASKS_PER_WORKER = 50
MAX_IDLE_TIME = 30
MAX_IDLE_WORKERS = 2

Script = ''

def reply_status(driver, task, status, data=None):
    update = mesos_pb2.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = status
    if data is not None:
        update.data = data
    driver.sendStatusUpdate(update)

def run_task(task, ntry):
    try:
        setproctitle('dpark worker %s: run task %s' % (Script, task))
        Accumulator.clear()
        result = task.run(ntry)
        accUpdate = Accumulator.values()

        if marshalable(result):
            flag, data = 0, marshal.dumps(result)
        else:
            flag, data = 1, cPickle.dumps(result, -1)
        if len(data) > TASK_RESULT_LIMIT:
            workdir = env.get('WORKDIR')
            name = 'task_%s_%s.result' % (task.id, ntry)
            path = os.path.join(workdir, name) 
            f = open(path, 'w')
            f.write(data)
            if env.dfs:
                f.flush()
                os.fsync(f.fileno())
            f.close()
            data = LocalFileShuffle.getServerUri() + '/' + name
            flag += 2

        setproctitle('dpark worker: idle')
        return mesos_pb2.TASK_FINISHED, cPickle.dumps((task.id, Success(), (flag, data), accUpdate), -1)
    except Exception, e:
        import traceback
        msg = traceback.format_exc()
        setproctitle('dpark worker: idle')
        return mesos_pb2.TASK_FAILED, cPickle.dumps((task.id, OtherFailure(msg), None, None), -1)

def init_env(args):
    setproctitle('dpark worker: idle')
    env.start(False, args)

class LocalizedHTTP(SimpleHTTPServer.SimpleHTTPRequestHandler):
    basedir = None
    def translate_path(self, path):
        out = SimpleHTTPServer.SimpleHTTPRequestHandler.translate_path(self, path)
        return self.basedir + '/' + os.path.relpath(out)
    
    def log_message(self, format, *args):
        pass

def startWebServer(path):
    # check the default web server
    with open(os.path.join(path, 'test'), 'w') as f:
        f.write('testdata')
    default_uri = 'http://%s:%d/%s' % (socket.gethostname(), DEFAULT_WEB_PORT,
            os.path.basename(path))
    try:
        data = urllib2.urlopen(default_uri + '/' + 'test').read()
        if data == 'testdata':
            return default_uri
    except IOError, e:
        pass
    
    logger.warning("default webserver at %s not available", DEFAULT_WEB_PORT)
    LocalizedHTTP.basedir = os.path.dirname(path)
    ss = SocketServer.TCPServer(('0.0.0.0', 0), LocalizedHTTP)
    threading.Thread(target=ss.serve_forever).start()
    uri = "http://%s:%d/%s" % (socket.gethostname(), ss.server_address[1], 
            os.path.basename(path))
    return uri 

def forword(fd, addr, prefix=''):
    f = os.fdopen(fd, 'r')
    ctx = zmq.Context()
    out = ctx.socket(zmq.PUSH)
    out.connect(addr)
    buf = []
    while True:
        try:
            line = f.readline()
            if not line: break
            buf.append(line)
            if line.endswith('\n'):
                out.send(prefix+''.join(buf))
                buf = []
        except IOError:
            break
    if buf:
        out.send(''.join(buf))
    out.close()
    f.close()
    ctx.shutdown()

def start_forword(addr, prefix=''):
    rfd, wfd = os.pipe()
    t = threading.Thread(target=forword, args=[rfd, addr, prefix])
    t.daemon = True
    t.start()    
    return t, os.fdopen(wfd, 'w', 0) 

def get_pool_memory(pool):
    if platform.system() == 'Linux':
        for line in open('/proc/%d/status' % pool._pool[0].pid):
            if line.startswith('VmRSS:'):
                return int(line.split()[1]) >> 10
    return 0

def get_task_memory(task):
    for r in task.resources:
        if r.name == 'mem':
            return r.scalar.value
    return 0

class MyExecutor(mesos.Executor):
    def __init__(self):
        self.workdir = None
        self.idle_workers = []
        self.busy_workers = {}

    def check_memory(self, driver):
        mem_limit = {}
        while True:
            for tid, (task, pool) in self.busy_workers.items():
                rss = get_pool_memory(pool)
                offered = get_task_memory(task)
                if rss > offered * 2:
                    logger.error("task %s used too much memory: %dMB > %dMB * 2, kill it. "
                            + "use -M argument to request more memory.", tid, rss, offered)
                    reply_status(driver, task, mesos_pb2.TASK_KILLED)
                    self.busy_workers.pop(tid)
                    pool.terminate()
                elif rss > offered * mem_limit.get(tid, 1.0):
                    logger.warning("task %s used too much memory: %dMB > %dMB, "
                            + "use -M to request more memory", tid, rss, offered)
                    mem_limit[tid] = rss / offered + 0.2

            now = time.time() 
            n = len([1 for t, p in self.idle_workers if t + MAX_IDLE_TIME < now])
            if n:
                for _, p in self.idle_workers[:n]:
                    p.terminate()
                self.idle_workers = self.idle_workers[n:]

            time.sleep(5) 

    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        try:
            global Script
            Script, cwd, python_path, self.parallel, out_logger, err_logger, logLevel, args = marshal.loads(executorInfo.data)
            self.init_args = [args]
            try:
                os.chdir(cwd)
            except OSError:
                driver.sendFrameworkMessage("switch cwd failed: %s not exists!" % cwd)
            sys.path = python_path
            
            prefix = '[%s] ' % socket.gethostname()
            if out_logger:
                self.outt, sys.stdout = start_forword(out_logger, prefix)
            if err_logger:
                self.errt, sys.stderr = start_forword(err_logger, prefix)
            logging.basicConfig(format='%(asctime)-15s [%(name)-9s] %(message)s', level=logLevel)

            if args['DPARK_HAS_DFS'] != 'True':
                self.workdir = args['WORKDIR']
                if not os.path.exists(self.workdir):
                    os.mkdir(self.workdir)
                args['SERVER_URI'] = startWebServer(args['WORKDIR'])

            t = threading.Thread(target=self.check_memory, args=[driver])
            t.daemon = True
            t.start()
 
            logger.debug("executor started at %s", slaveInfo.hostname)

        except Exception, e:
            import traceback
            msg = traceback.format_exc()
            driver.sendFrameworkMessage("init executor failed:\n " +  msg)

    def reregitered(self, driver, slaveInfo):
        logger.info("executor is reregistered at %s", slaveInfo.hostname)

    def disconnected(self, driver, slaveInfo):
        logger.info("executor is disconnected at %s", slaveInfo.hostname)

    def get_idle_worker(self):
        try:
            return self.idle_workers.pop()[1]
        except IndexError:
            p = multiprocessing.Pool(1, init_env, self.init_args)
            p.done = 0
            return p 

    def launchTask(self, driver, task):
        try:
            t, ntry = cPickle.loads(task.data)
            
            reply_status(driver, task, mesos_pb2.TASK_RUNNING)
            
            logging.debug("launch task %s", t.id)
            
            pool = self.get_idle_worker()
            self.busy_workers[task.task_id.value] = (task, pool)

            def callback((state, data)):
                if task.task_id.value not in self.busy_workers:
                    return
                _, pool = self.busy_workers.pop(task.task_id.value)
                pool.done += 1
                reply_status(driver, task, state, data)
                if (len(self.idle_workers) + len(self.busy_workers) < self.parallel 
                        and len(self.idle_workers) < MAX_IDLE_WORKERS
                        and pool.done < MAX_TASKS_PER_WORKER 
                        and get_pool_memory(pool) < get_task_memory(task)): # maybe memory leak in executor
                    self.idle_workers.append((time.time(), pool))
                else:
                    try: pool.terminate() 
                    except: pass
        
            pool.apply_async(run_task, [t, ntry], callback=callback)
    
        except Exception, e:
            import traceback
            msg = traceback.format_exc()
            reply_status(driver, task, mesos_pb2.TASK_LOST, msg)
            return

    def killTask(self, driver, taskId):
        if taskId.value in self.busy_workers:
            task, pool = self.busy_workers.pop(taskId.value)
            pool.terminate()
            reply_status(driver, task, mesos_pb2.TASK_KILLED)

    def shutdown(self, driver):
        for _, p in self.idle_workers:
            try: p.terminate()
            except: pass
        for p in self.busy_workers.values():
            try: p.terminate()
            except: pass
        # flush
        try:
            sys.stdout.close()
            sys.stderr.close()
            self.outt.join()
            self.errt.join()
        except: pass
        
        # clean work files
        if self.workdir:
            try: shutil.rmtree(self.workdir, True)
            except: pass

    def error(self, driver, code, message):
        logger.error("error: %s, %s", code, message)

    def frameworkMessage(self, driver, data):
        pass

def run():
    executor = MyExecutor()
    driver = mesos.MesosExecutorDriver(executor)
    driver.run()

if __name__ == '__main__':
    run()

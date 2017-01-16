import gc
import os
import sys
import time
import errno
import fcntl
import shutil
import signal
import socket
import cPickle
import logging
import marshal
import urllib2
import threading
import subprocess
import SocketServer
import multiprocessing
import SimpleHTTPServer


import zmq
from addict import Dict
from pymesos import Executor, MesosExecutorDriver, encode_data, decode_data

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dpark.util import compress, decompress, spawn, mkdir_p, get_logger
from dpark.serialize import marshalable
from dpark.accumulator import Accumulator
from dpark.schedule import Success, FetchFailed, OtherFailure
from dpark.env import env
from dpark.shuffle import LocalFileShuffle
from dpark.mutable_dict import MutableDict
from dpark.serialize import loads
from dpark.moosefs import close_mfs

logger = get_logger('dpark.executor@%s' % socket.gethostname())

TASK_RESULT_LIMIT = 1024 * 256
DEFAULT_WEB_PORT = 5055
MAX_EXECUTOR_IDLE_TIME = 60 * 60 * 24
KILL_TIME_OUT = 0.1  # 0.1 sec
CLEAN_ZOMBIE_TIME_OUT = 10
Script = ''


def setproctitle(x):
    try:
        from setproctitle import setproctitle as _setproctitle
        _setproctitle(x)
    except ImportError:
        pass


def reply_status(driver, task_id, state, data=None):
    status = Dict()
    status.task_id = task_id
    status.state = state
    status.timestamp = time.time()
    if data is not None:
        status.data = encode_data(data)
    driver.sendStatusUpdate(status)


def run_task(task_data):
    try:
        gc.disable()
        task, ntry = loads(decompress(task_data))
        Accumulator.clear()
        result = task.run(ntry)
        accUpdate = Accumulator.values()
        MutableDict.flush()

        if marshalable(result):
            try:
                flag, data = 0, marshal.dumps(result)
            except Exception as e:
                flag, data = 1, cPickle.dumps(result, -1)

        else:
            flag, data = 1, cPickle.dumps(result, -1)
        data = compress(data)

        if len(data) > TASK_RESULT_LIMIT:
            path = LocalFileShuffle.getOutputFile(0, ntry, task.id, len(data))
            f = open(path, 'w')
            f.write(data)
            f.close()
            data = '/'.join(
                [LocalFileShuffle.getServerUri()] + path.split('/')[-3:]
            )
            flag += 2

        return 'TASK_FINISHED', cPickle.dumps(
            (Success(), (flag, data), accUpdate), -1)
    except FetchFailed as e:
        return 'TASK_FAILED', cPickle.dumps((e, None, None), -1)
    except:
        import traceback
        msg = traceback.format_exc()
        return 'TASK_FAILED', cPickle.dumps(
            (OtherFailure(msg), None, None), -1)
    finally:
        close_mfs()
        gc.collect()
        gc.enable()


def init_env(args):
    env.start(False, args)


class LocalizedHTTP(SimpleHTTPServer.SimpleHTTPRequestHandler):
    basedir = None

    def translate_path(self, path):
        out = SimpleHTTPServer.SimpleHTTPRequestHandler.translate_path(
            self, path)
        return self.basedir + '/' + os.path.relpath(out)

    def log_message(self, format, *args):
        pass


def startWebServer(path):
    # check the default web server
    if not os.path.exists(path):
        os.makedirs(path)
    testpath = os.path.join(path, 'test')
    with open(testpath, 'w') as f:
        f.write(path)
    default_uri = 'http://%s:%d/%s' % (socket.gethostname(), DEFAULT_WEB_PORT,
                                       os.path.basename(path))
    try:
        data = urllib2.urlopen(default_uri + '/' + 'test').read()
        if data == path:
            return default_uri
    except IOError:
        pass

    logger.warning('default webserver at %s not available', DEFAULT_WEB_PORT)
    LocalizedHTTP.basedir = os.path.dirname(path)
    ss = SocketServer.TCPServer(('0.0.0.0', 0), LocalizedHTTP)
    spawn(ss.serve_forever)
    uri = 'http://%s:%d/%s' % (socket.gethostname(), ss.server_address[1],
                               os.path.basename(path))
    return uri


def terminate(tid, proc):
    name = 'worker(tid: %s, pid: %s)' % (tid, proc.pid)
    try:
        os.kill(proc.pid, signal.SIGKILL)
        proc.join(KILL_TIME_OUT)
        existcode = proc.exitcode
        if proc.exitcode != - signal.SIGKILL:
            logger.debug('%s terminate fail: %s', name, existcode)
        else:
            logger.debug('%s terminate ok', name)
    except Exception as e:
        logger.warn('%s terminate exception: %s', name, e)


def get_task_memory(task):
    for r in task.resources:
        if r.name == 'mem':
            return r.scalar.value
    logger.error('no memory in resource: %s', task.resources)
    return 100  # 100M


def safe(f):
    def _(self, *a, **kw):
        with self.lock:
            r = f(self, *a, **kw)
        return r
    return _


def setup_cleaner_process(workdir):
    ppid = os.getpid()
    pid = os.fork()
    if pid == 0:
        os.setsid()
        pid = os.fork()
        if pid == 0:
            try:
                import psutil
            except ImportError:
                os._exit(1)
            try:
                psutil.Process(ppid).wait()
                os.killpg(ppid, signal.SIGKILL)  # kill workers
            except Exception:
                pass  # make sure to exit
            finally:
                for d in workdir:
                    while os.path.exists(d):
                        try:
                            shutil.rmtree(d, True)
                        except:
                            pass
        os._exit(0)
    os.wait()


class Redirect(object):

    def __init__(self, fd, addr, prefix):
        self.fd = fd
        self.addr = addr
        self.prefix = prefix

        self.fd_dup = os.dup(self.fd)
        self.origin_wfile = None

        self.pipe_rfd, self.pipe_wfd = os.pipe()
        self.pipe_rfile = os.fdopen(self.pipe_rfd, 'r')
        self.pipe_wfile = os.fdopen(self.pipe_wfd, 'w', 0)

        os.close(self.fd)
        os.dup2(self.pipe_wfd, self.fd)
        # assert os.dup(self.pipe_wfd) == self.fd, 'redirect io failed'

        self.ctx = zmq.Context()
        self._shutdown = False
        self.thread = None
        self.sock = None

        self.thread = spawn(self._forward)

    def reset(self):
        err = None
        try:
            self._shutdown = True
            self.pipe_wfile.close()
            os.close(self.fd)

            self.thread.join(1)
            if self.sock:
                self.sock.close()
            self.ctx.destroy()
        except Exception as e:
            err = e

        os.dup2(self.fd_dup, self.fd)  # will close fd first
        self.origin_wfile = os.fdopen(self.fd, 'w', 0)

        logger.debug('should see me in sandbox')
        if err:
            logger.error('redirect reset err:', err)

        if self.thread.isAlive():
            logger.error('redirect thread not exit')

        return self.origin_wfile

    def _send(self, buf):
        if not self.sock:
            self.sock = self.ctx.socket(zmq.PUSH)
            self.sock.setsockopt(zmq.LINGER, 0)
            self.sock.connect(self.addr)

        data = self.prefix + ''.join(buf)

        while not self._shutdown:
            try:
                self.sock.send(data, zmq.NOBLOCK)
                return
            except zmq.Again:
                time.sleep(0.1)
                continue

    def _forward(self):
        buf = []
        try:
            while not self._shutdown:
                try:
                    line = self.pipe_rfile.readline()
                    if not line:
                        break
                    buf.append(line)
                    if line.endswith('\n'):
                        self._send(buf)
                        buf = []
                except IOError:
                    break
            if buf:
                self._send(buf)
        except Exception as e:
            logger.error('_forward err: %s', e)


class MyExecutor(Executor):

    def __init__(self):
        self.workdir = []

        # task_id.value -> (task, process)
        self.tasks = {}
        # (task_id.value, (status, data))
        self.result_queue = multiprocessing.Queue()

        self.lock = threading.RLock()

        # Keep the file descriptor of current workdir,
        # so we can check whether a workdir is in use externally.
        self._fd_for_locks = []

    def check_memory(self, driver):
        try:
            import psutil
        except ImportError:
            logger.error('no psutil module')
            return

        mem_limit = {}
        idle_since = time.time()

        while True:
            with self.lock:
                tids_to_pop = []
                for tid, (task, proc) in self.tasks.iteritems():
                    task_id = task.task_id
                    try:
                        pid = proc.pid
                        p = psutil.Process(pid)
                        rss = p.memory_info().rss >> 20
                    except Exception as e:
                        logger.error(
                            'worker process %d of task %s is dead: %s',
                            pid, tid, e
                        )
                        reply_status(driver, task_id, 'TASK_LOST')
                        tids_to_pop.append(tid)
                        continue

                    if p.status() == psutil.STATUS_ZOMBIE or not p.is_running():
                        reply_status(driver, task_id, 'TASK_LOST')
                        proc.join(CLEAN_ZOMBIE_TIME_OUT)
                        try:
                            os.waitpid(proc.pid, os.WNOHANG)
                        except OSError as e:
                            if e.errno == errno.ECHILD:
                                tids_to_pop.append(tid)
                            else:
                                logger.exception('process termination fail: ', e.message)
                        else:
                            logger.error('worker process %d of task %s is zombie', pid, tid)
                        continue

                    offered = get_task_memory(task)
                    if not offered:
                        continue
                    if rss > offered * 1.5:
                        logger.warning(
                            'task %s used too much memory: %dMB > %dMB * 1.5, '
                            'kill it. ' 'use -M argument or taskMemory '
                            'to request more memory.',
                            tid, rss, offered
                        )

                        reply_status(driver, task_id, 'TASK_KILLED')
                        tids_to_pop.append(tid)
                        terminate(tid, proc)
                    elif rss > offered * mem_limit.get(tid, 1.0):
                        logger.debug(
                            'task %s used too much memory: %dMB > %dMB, '
                            'use -M to request or taskMemory for more memory',
                            tid, rss, offered)
                        mem_limit[tid] = rss / offered + 0.1
                for tid in tids_to_pop:
                    self.tasks.pop(tid)
                now = time.time()
                if self.tasks:
                    idle_since = now
                elif idle_since + MAX_EXECUTOR_IDLE_TIME < now:
                    os._exit(0)

            time.sleep(1)

    def _try_flock(self, path):
        fd = os.open(path, os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as e:
            try:
                pids = subprocess.check_output(['fuser', path]).split()
                curr_pid = os.getpid()
                logger.warning(
                    'current process: %s, processes that are using %s: %s',
                    curr_pid, path, pids)
            except Exception:
                pass
            raise e
        self._fd_for_locks.append(fd)

    @safe
    def registered(self, driver, executorInfo, frameworkInfo, agent_info):
        try:
            global Script
            (
                Script, cwd, python_path, osenv, self.parallel,
                out_logger, err_logger, logLevel, args
            ) = marshal.loads(decode_data(executorInfo.data))

            self.init_args = args
            sys.path = python_path
            os.environ.update(osenv)
            setproctitle('[Executor]' + Script)

            prefix = '[%s] ' % socket.gethostname()

            fmt = '%(asctime)-15s [%(levelname)s] [%(name)-9s] %(message)s'
            logging.basicConfig(format=fmt, level=logLevel)

            r1 = self.stdout_redirect = Redirect(1, out_logger, prefix)
            sys.stdout = r1.pipe_wfile

            r2 = self.stderr_redirect = Redirect(2, err_logger, prefix)
            sys.stderr = r2.pipe_wfile

            if os.path.exists(cwd):
                try:
                    os.chdir(cwd)
                except Exception as e:
                    logger.warning('change cwd to %s failed: %s', cwd, e)
            else:
                logger.warning('cwd (%s) not exists', cwd)

            self.workdir = args['WORKDIR']
            main_workdir = self.workdir[0]

            root = os.path.dirname(main_workdir)
            if not os.path.exists(root):
                os.mkdir(root)
                os.chmod(root, 0o777)  # because umask

            mkdir_p(main_workdir)
            self._try_flock(main_workdir)

            args['SERVER_URI'] = startWebServer(main_workdir)
            if 'MESOS_SLAVE_PID' in os.environ:  # make unit test happy
                setup_cleaner_process(self.workdir)

            spawn(self.check_memory, driver)
            spawn(self.replier, driver)

            logger.debug('executor started at %s', agent_info.hostname)

        except Exception as e:
            import traceback
            msg = traceback.format_exc()
            logger.error('init executor failed: %s', msg)
            raise

    def replier(self, driver):
        while True:
            try:
                result = self.result_queue.get()
                if result is None:
                    return
                (task_id_value, result) = result
                state, data = result

                with self.lock:
                    task, _ = self.tasks.pop(task_id_value)

                reply_status(driver, task.task_id, state, data)

            except Exception as e:
                logger.warning('reply fail %s', e)

    @safe
    def launchTask(self, driver, task):
        task_id = task.task_id
        reply_status(driver, task_id, 'TASK_RUNNING')
        logger.debug('launch task %s', task.task_id.value)

        def worker(name, q, task_id_value, task_data, init_args):
            setproctitle(name)
            init_env(init_args)
            q.put((task_id_value, run_task(task_data)))

        try:
            name = '[Task-%s]%s' % (task.task_id.value, Script)
            proc = multiprocessing.Process(target=worker,
                                           args=(name,
                                                 self.result_queue,
                                                 task.task_id.value,
                                                 decode_data(task.data),
                                                 self.init_args))
            proc.name = name
            proc.daemon = True
            proc.start()
            self.tasks[task.task_id.value] = (task, proc)

        except Exception:
            import traceback
            msg = traceback.format_exc()
            reply_status(driver, task_id, 'TASK_LOST', msg)

    @safe
    def killTask(self, driver, taskId):
        reply_status(driver, taskId, 'TASK_KILLED')
        if taskId.value in self.tasks:
            _, proc = self.tasks.pop(taskId.value)
            terminate(taskId.value, proc)

    @safe
    def shutdown(self, driver=None):
        for tid, (_, proc) in self.tasks.iteritems():
            terminate(tid, proc)
        self.tasks = {}
        self.result_queue.put(None)

        # clean work files
        for fd in self._fd_for_locks:
            os.close(fd)
        for d in self.workdir:
            try:
                shutil.rmtree(d, True)
            except:
                pass

        sys.stdout = self.stdout_redirect.reset()
        sys.stderr = self.stderr_redirect.reset()


def run():
    setproctitle('Executor')
    if os.getuid() == 0:
        gid = os.environ['GID']
        uid = os.environ['UID']
        os.setgid(int(gid))
        os.setuid(int(uid))

    executor = MyExecutor()
    driver = MesosExecutorDriver(executor, use_addict=True)
    driver.run()

if __name__ == '__main__':
    run()

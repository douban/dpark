from __future__ import absolute_import
import gc
import os
import sys
import time
import errno
import fcntl
import shutil
import signal
import socket
import logging
import marshal
import threading
import subprocess
import multiprocessing
import six
from six.moves import socketserver, cPickle, SimpleHTTPServer, urllib
import resource

import zmq
from addict import Dict
from pymesos import Executor, MesosExecutorDriver, encode_data, decode_data

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dpark.utils import (
    compress, decompress, spawn, mkdir_p, DparkUserFatalError
)
from dpark.utils.log import get_logger, init_dpark_logger, formatter_message
from dpark.utils.memory import ERROR_TASK_OOM, set_oom_score
from dpark.serialize import marshalable
from dpark.accumulator import Accumulator
from dpark.env import env
from dpark.shuffle import LocalFileShuffle
from dpark.mutable_dict import MutableDict
from dpark.serialize import loads
from dpark.task import TTID, TaskState, TaskEndReason, FetchFailed
from dpark.utils.debug import spawn_rconsole

logger = get_logger('dpark.executor')

TASK_RESULT_LIMIT = 1024 * 256
DEFAULT_WEB_PORT = 5055
MAX_EXECUTOR_IDLE_TIME = 60 * 60 * 24  # 1 day
KILL_TIMEOUT = 0.1  # 0.1 sec, to reply to mesos fast
TASK_LOST_JOIN_TIMEOUT = 3
TASK_LOST_DISCARD_TIMEOUT = 60
Script = ''


def setproctitle(x):
    try:
        from setproctitle import setproctitle as _setproctitle
        _setproctitle(x)
    except ImportError:
        pass


def reply_status(driver, task_id, state, reason=None, msg=None, data=None):
    status = Dict()
    status.task_id = task_id
    status.state = state
    if reason is not None:
        status.message = '{}:{}'.format(reason, msg)
    status.timestamp = time.time()
    if data is not None:
        status.data = encode_data(data)
    driver.sendStatusUpdate(status)


def run_task(task_data):
    try:
        gc.disable()
        task, task_try_id = loads(decompress(task_data))
        ttid = TTID(task_try_id)
        Accumulator.clear()
        result = task.run(ttid.ttid)
        env.task_stats.bytes_max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        accUpdate = Accumulator.values()
        MutableDict.flush()

        if marshalable(result):
            try:
                flag, data = 0, marshal.dumps(result)
            except Exception:
                flag, data = 1, cPickle.dumps(result, -1)

        else:
            flag, data = 1, cPickle.dumps(result, -1)
        data = compress(data)

        if len(data) > TASK_RESULT_LIMIT:
            path = LocalFileShuffle.getOutputFile(0, task.id, ttid.task_try, len(data))
            f = open(path, 'wb')
            f.write(data)
            f.close()
            data = '/'.join(
                [LocalFileShuffle.getServerUri()] + path.split('/')[-3:]
            )
            flag += 2

        return TaskState.finished, cPickle.dumps(((flag, data), accUpdate, env.task_stats), -1)
    except FetchFailed as e:
        return TaskState.failed, TaskEndReason.fetch_failed, str(e), cPickle.dumps(e)
    except Exception as e:
        import traceback
        msg = traceback.format_exc()
        ename = e.__class__.__name__
        fatal_exceptions = (DparkUserFatalError, ArithmeticError,
                            ValueError, LookupError, SyntaxError,
                            TypeError, AssertionError)
        prefix = "FATAL" if isinstance(e, fatal_exceptions) else "FAILED"
        return TaskState.failed, '{}_EXCEPTION_{}'.format(prefix, ename), msg, cPickle.dumps(e)
    finally:
        gc.collect()
        gc.enable()


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
        data = urllib.request.urlopen(default_uri + '/' + 'test').read()
        if data == path.encode('utf-8'):
            return default_uri
    except IOError:
        pass

    logger.warning('default webserver at %s not available', DEFAULT_WEB_PORT)
    LocalizedHTTP.basedir = os.path.dirname(path)
    ss = socketserver.TCPServer(('0.0.0.0', 0), LocalizedHTTP)
    spawn(ss.serve_forever)
    uri = 'http://%s:%d/%s' % (socket.gethostname(), ss.server_address[1],
                               os.path.basename(path))
    return uri


def terminate(tid, proc):
    name = 'worker(tid: %s, pid: %s)' % (tid, proc.pid)
    try:
        os.kill(proc.pid, signal.SIGTERM)
    except Exception as e:
        if proc.join(timeout=KILL_TIMEOUT / 2) is None:
            try:
                os.kill(proc.pid, signal.SIGKILL)
            except Exception as e:
                if proc.join(KILL_TIMEOUT / 2) is not None:
                    logger.exception('%s terminate fail', name)


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
        self.pipe_rfile = os.fdopen(self.pipe_rfd, 'rb')
        self.pipe_wfile = os.fdopen(self.pipe_wfd, 'wb', 0)

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
        self.origin_wfile = os.fdopen(self.fd, 'wb', 0)

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

        # task_id.value -> (task, timestamp)
        self.finished_tasks = {}

        # (task_id.value, (status, data))
        self.result_queue = multiprocessing.Queue()

        self.lock = threading.RLock()

        # Keep the file descriptor of current workdir,
        # so we can check whether a workdir is in use externally.
        self._fd_for_locks = []
        self.stdout_redirect = None
        self.stderr_redirect = None

    def check_alive(self, driver):
        try:
            import psutil
        except ImportError:
            logger.error('no psutil module')
            return

        idle_since = time.time()

        kill_ecs = [-signal.SIGTERM]

        while True:
            with self.lock:
                tasks = self.tasks.items()

            tids_to_pop = []
            for tid, (task, proc) in tasks:
                task_id = task.task_id
                name = "task %s (pid = %d)" % (tid, proc.pid)
                proc_end = False
                reason = None
                msg = None

                try:
                    p = psutil.Process(proc.pid)
                except Exception:
                    proc_end = True

                if proc_end or p.status() == psutil.STATUS_ZOMBIE or (not p.is_running()):
                    proc.join(TASK_LOST_JOIN_TIMEOUT)  # join in py2 not return exitcode
                    ec = proc.exitcode
                    if ec == 0:  # p.status() == psutil.STATUS_ZOMBIE
                        continue  # handled in replier

                    if ec is not None:
                        proc_end = True
                        msg = 'exitcode: {}'.format(ec)
                        if ec in kill_ecs:
                            reason = TaskEndReason.recv_sig
                        elif ec == -signal.SIGKILL:
                            reason = TaskEndReason.recv_sig_kill
                        elif ec == ERROR_TASK_OOM:
                            reason = TaskEndReason.task_oom
                        else:
                            reason = TaskEndReason.other_ecs
                            logger.warning('%s lost with exit code: %s', tid, ec)
                    else:
                        try:
                            os.waitpid(proc.pid, os.WNOHANG)
                        except OSError as e:
                            proc_end = True
                            if e.errno != errno.ECHILD:
                                logger.exception('%s lost, raise exception when waitpid', tid)
                        else:
                            t = self.finished_tasks.get(tid)
                            if t is not None and time.time() - t > TASK_LOST_DISCARD_TIMEOUT:
                                logger.warning('%s is zombie for %d secs, discard it!', name, TASK_LOST_DISCARD_TIMEOUT)

                if proc_end:
                    tids_to_pop.append(tid)
                    reply_status(driver, task_id, TaskState.failed, reason, msg)

            with self.lock:
                for tid_ in tids_to_pop:
                    try:
                        self.tasks.pop(tid_)
                    except:
                        pass
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
                out_logger, err_logger, logLevel, use_color, args
            ) = marshal.loads(decode_data(executorInfo.data))

            sys.path = python_path
            os.environ.update(osenv)
            setproctitle('[Executor]' + Script)

            prefix = formatter_message(
                '{MAGENTA}[%s]{RESET} ' % socket.gethostname().ljust(10),
                use_color
            )

            init_dpark_logger(logLevel, use_color=use_color)
            logging.root.setLevel(logLevel)

            r1 = self.stdout_redirect = Redirect(1, out_logger, prefix)
            sys.stdout = r1.pipe_wfile

            r2 = self.stderr_redirect = Redirect(2, err_logger, prefix)
            sys.stderr = r2.pipe_wfile

            spawn_rconsole(locals())

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

            spawn(self.check_alive, driver)
            spawn(self.replier, driver)

            env.environ.update(args)
            from dpark.broadcast import start_download_manager
            start_download_manager()

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

                reason = None
                message = None

                task_id_value, result = result
                if result[0] == TaskState.failed:
                    state, reason, message, data = result
                else:
                    state, data = result

                with self.lock:
                    task, _ = self.tasks.pop(task_id_value)
                    self.finished_tasks[task_id_value] = time.time()

                reply_status(driver, task.task_id, state, reason, message, data)

            except Exception as e:
                logger.warning('reply fail %s', e)

    @safe
    def launchTask(self, driver, task):
        task_id = task.task_id
        reply_status(driver, task_id, TaskState.running)
        logger.debug('launch task %s', task.task_id.value)

        def worker(procname, q, task_id_value, task_data):
            task_id_str = "task %s" % (task_id_value,)
            threading.current_thread().name = task_id_str
            setproctitle(procname)
            set_oom_score(100)
            env.start()
            q.put((task_id_value, run_task(task_data)))

        try:
            name = '[Task-%s]%s' % (task.task_id.value, Script)
            proc = multiprocessing.Process(target=worker,
                                           args=(name,
                                                 self.result_queue,
                                                 task.task_id.value,
                                                 decode_data(task.data),))
            proc.name = name
            proc.daemon = True
            proc.start()
            self.tasks[task.task_id.value] = (task, proc)

        except Exception as e:
            import traceback
            msg = traceback.format_exc()
            reply_status(driver, task_id, TaskState.failed, TaskEndReason.launch_failed, msg, cPickle.dumps(e))

    @safe
    def killTask(self, driver, taskId):
        reply_status(driver, taskId, TaskState.killed)
        if taskId.value in self.tasks:
            _, proc = self.tasks.pop(taskId.value)
            terminate(taskId.value, proc)

    @safe
    def shutdown(self, driver=None):
        for tid, (_, proc) in six.iteritems(self.tasks):
            terminate(tid, proc)
        self.tasks = {}
        self.result_queue.put(None)
        from dpark.broadcast import stop_manager
        stop_manager()

        # clean work files
        for fd in self._fd_for_locks:
            os.close(fd)
        for d in self.workdir:
            try:
                shutil.rmtree(d, True)
            except:
                pass

        if self.stdout_redirect:
            sys.stdout = self.stdout_redirect.reset()
        if self.stderr_redirect:
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
    fmt = '%(asctime)-15s [%(levelname)s] [%(threadName)s] [%(name)-9s] %(message)s'
    logging.basicConfig(format=fmt, level=logging.INFO)
    run()

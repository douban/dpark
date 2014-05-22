import zookeeper
import threading
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)
zookeeper.set_debug_level(zookeeper.LOG_LEVEL_WARN)



# Mapping of connection state values to human strings.
STATE_NAME_MAPPING = {
    zookeeper.ASSOCIATING_STATE: "associating",
    zookeeper.AUTH_FAILED_STATE: "auth-failed",
    zookeeper.CONNECTED_STATE: "connected",
    zookeeper.CONNECTING_STATE: "connecting",
    zookeeper.EXPIRED_SESSION_STATE: "expired",
}

# Mapping of event type to human string.
TYPE_NAME_MAPPING = {
    zookeeper.NOTWATCHING_EVENT: "not-watching",
    zookeeper.SESSION_EVENT: "session",
    zookeeper.CREATED_EVENT: "created",
    zookeeper.DELETED_EVENT: "deleted",
    zookeeper.CHANGED_EVENT: "changed",
    zookeeper.CHILD_EVENT: "child",
}
class TimeoutException( zookeeper.ZooKeeperException):
    pass

def logevent(h,typ, state, path):
    logger.debug("event,handle:%d, type:%s, state:%s, path:%s", h, TYPE_NAME_MAPPING.get(typ, "unknown"), STATE_NAME_MAPPING.get(state, "unknown"), path)

class ZKClient:
    def __init__(self, servers, timeout= 10):
        self.timeout = timeout
        self.connected = False
        self.handle = -1
        self.servers = servers
        self.watchers  = set()
        self._lock = threading.Lock()
        self.conn_cv = threading.Condition()

    def start(self):
        self.handle = zookeeper.init(self.servers, self.connection_watcher, self.timeout * 1000)
        self.conn_cv.acquire()
        self.conn_cv.wait(self.timeout)
        self.conn_cv.release()
        if not self.connected:
            raise TimeoutException

    def stop(self):
        return zookeeper.close(self.handle)

    def connection_watcher(self, h, typ, state, path):
        logevent(h, typ, state, path)
        if  typ == zookeeper.SESSION_EVENT:
            if state == zookeeper.CONNECTED_STATE:
                self.handle = h
                with self._lock:
                    self.connected = True
                    watchers = list(self.watchers)
                for watcher in watchers:
                    watcher.watch()

        self.conn_cv.acquire()
        self.conn_cv.notifyAll()
        self.conn_cv.release()

    def del_watcher(self, watcher):
        with self._lock:
            self.watchers.discard(watcher)

    def add_watcher(self, watcher):
        with self._lock:
            self.watchers.add(watcher)
        if self.connected:
            watcher.watch()

class DataWatch:
    def __init__(self, client, path, func):
        self._client = client
        self._path = path
        self._func = func
        self._stopped = False
        client.add_watcher(self)

    def watcher(self, h, typ, state, path):
        logevent(h, typ, state, path)
        self.watch()

    def _do(self):
        data, stat = zookeeper.get(self._client.handle, self._path, self.watcher)
        return self._func(data, stat)

    def watch(self):
        if self._stopped:
            return
        try:
            result = self._do()
            if result is False:
                self._stopped = True
        except zookeeper.NoNodeException:
            raise
        except zookeeper.ZooKeeperException as e:
            logger.error("ZooKeeperException, type:%s, msg: %s", type(e), e)


class ChildrenWatch(DataWatch):
    def _do(self):
        children = zookeeper.get_children(self._client.handle, self._path, self.watcher)
        return self._func(children)



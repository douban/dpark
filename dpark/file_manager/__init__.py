from __future__ import absolute_import
from .utils import FileInfo, unpack, read_chunk
from .consts import *
from .mfs_proxy import ProxyConn
from dpark.util import get_logger
from .fs import MooseFS, PosixFS
import os

logger = get_logger(__name__)


class FileManager(object):
    def __init__(self):
        self.fs_list = [MooseFS(), PosixFS()]

    def register_fs(self, fs):
        self.fs_list = [fs] + self.fs_list

    def _get_fs(self, path):
        for fs in self.fs_list:
            if fs.check_ok(path):
                return fs
        return None

    def open_file(self, path):
        path = os.path.realpath(path)
        fs = self._get_fs(path)
        return fs.open_file(path)

    def walk(self, path, followlinks=True):
        fs = self._get_fs(path)
        return fs.walk(path, followlinks=followlinks)

file_manager = FileManager()


def open_file(path):
    return file_manager.open_file(path)


def walk(path, followlinks=True):
    return file_manager.walk(path, followlinks=followlinks)

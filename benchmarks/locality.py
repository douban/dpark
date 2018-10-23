import time
from dpark import DparkContext, optParser
from dpark.file_manager import file_manager
dc = DparkContext()

optParser.set_usage("%prog [options] path")
options, args = optParser.parse_args()

path = args[0]


def run(split_size=1):
    t = time.time()
    dc.textFile(path).mergeSplit(splitSize=split_size).filter(lambda x: "yangxiufeng" in x).count()
    return time.time() - t


run()  # file cache
print("{}s with locality".format(run()))
file_manager.fs_list = file_manager.fs_list[1:]
print("{}s merge & without locality".format(run(10)))
print("{}s without locality, ".format(run()))


#!/usr/bin/env python
import sys, os, os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from dpark import DparkContext
dpark = DparkContext()

def radio_stat():
    servers = ['theoden','balin']
    log_path = "/log/access-log/2011/radiolog/%s"
    radiolog = dpark.union(
            [dpark.mfsTextFile(log_path % h, splitSize=8<<20)
                for h in servers]).mergeSplit(16)
    #radiolog = dpark.csvFile(log_path%'balin'+'/radiolog_current')[:2]
    print len(radiolog)

    cnt = radiolog.map(
            lambda line:line.split(',')
        ).filter(
            lambda line:len(line) > 6 and line[1]!= '0' and line[5] in 'sp'
        ).map(
            lambda line:("%s:%s:%s"%(line[1],line[3],line[5]), 1)
        ).reduceByKey(lambda x,y:(x+y), 128).filter(lambda (k,v):v > 1)
#    print cnt.count()
    cnt.map(lambda (k,v): "%s,%s" % (k,v)).saveAsTextFile('/mfs/tmp/radio_stats_0628', overwrite=True)

def build_counter(path, name, flag):
    from douban.counter import counter_store_from_config
    store = counter_store_from_config('douban-online')
    bc = store.get_counter(name)
    m = bc.m
    print bc.m

    cnt = dpark.mfsTextFile(path).mergeSplit(8).map(lambda line:line.split(','))
    #print cnt.count()
    def hashit(it):
        import pyhash
        murmur = pyhash.murmur3_32()
        superfast = pyhash.super_fast_hash()
        for k,v in it:
            yield murmur(k[:-2]) & 0xffffffff, 1

    old = 2263027 * 256 #cnt.count()

    n = cnt.mapPartitions(hashit).reduceByKey(
            lambda x,y:x+y, 64
            ).count()
    print 100.0 * n / old

#radio_stat()
build_counter('/tmp/radio_stats_0628', 'RADIO_USER_SONG_PLAY_COUNT', 'p') 
build_counter('/tmp/radio_stats_0628', 'RADIO_USER_SONG_SKIP_COUNT', 's') 

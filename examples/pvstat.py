#!/usr/bin/env python
import sys, os, os.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date,timedelta
from dpark import DparkContext

dpark = DparkContext()

today = date.today()
day = date.today().strftime("%Y%m%d")
theday = today - timedelta(days=1)
path = '/mfs/log/weblog/%s/' % theday.strftime("%Y/%m/%d")

weblog = dpark.textFile(path)[:1]

bad_ip = weblog.map(lambda line:line.split(',')[3]


from context import SparkContext

dpark = SparkContext()

name = '/mfs/tmp/weblog-pre-20111019.csv'
name = '/mfs/tmp/weblog-20111019.csv'
#name = '/mfs/tmp/weblog-20111019.csv.small'
#name = '/mfs/tmp/weblog-20111019.csv.medium'
pv = dpark.textFile(name)
upv = pv.map(lambda x:x.split(',')[2]).filter(lambda uid:uid)
print upv.count() 
#print upv.reduceByKey(lambda x,y:x+y).count()

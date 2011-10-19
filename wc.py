
from context import SparkContext

dpark = SparkContext()

#name = '/mfs/tmp/weblog-pre-20111019.csv'
#name = '/mfs/tmp/weblog-20111019.csv'
name = '/tmp/weblog-20111019.csv.small'
name = '/tmp/weblog-20111019.csv.medium'
pv = dpark.textFile(name)
#print pv.count()
pv = pv.map(lambda x:x.split(',')).map(lambda l:(l[3],l[7])).cache()
#print pv.count() #, pv.first()
movie = pv.filter(lambda (bid,url): url.startswith('/movie'))
#print movie.count()
movie = movie.reduceByKey(lambda x,y:None) #.cache()
print movie.count()
group = pv.filter(lambda (bid,url): url.startswith('/group')).reduceByKey(lambda x,y:None)#.cache()
print group.count()
print movie.join(group).count()

#print pv.map(lambda x:x.split(',')[2]).uniq().count()
#print pv.map(lambda x:(x.split(',')[2],None)).reduceByKey(lambda x,y:None).count()
#.filter(lambda uid:uid)
#print upv.count() 
#print upv.reduceByKey(lambda x,y:x+y).count()

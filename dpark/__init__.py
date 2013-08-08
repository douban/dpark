from context import DparkContext, parser as optParser
from bagel import Bagel

_ctx = DparkContext()

parallelize = _ctx.parallelize

makeRDD = _ctx.makeRDD

textFile = _ctx.textFile

partialTextFile = _ctx.partialTextFile

csvFile = _ctx.csvFile

binaryFile = _ctx.binaryFile

tableFile = _ctx.tableFile

#table = _ctx.table

beansdb = _ctx.beansdb

union = _ctx.union

zip = _ctx.zip

#accumulator = _ctx.accumulator

#broadcast = _ctx.broadcast

start = _ctx.start

stop = _ctx.stop

clear = _ctx.clear

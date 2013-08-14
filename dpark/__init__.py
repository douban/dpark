from context import DparkContext, parser as optParser
from bagel import Bagel
from .decorator import jit

_ctx = DparkContext()

parallelize = _ctx.parallelize

makeRDD = _ctx.makeRDD

textFile = _ctx.textFile

partialTextFile = _ctx.partialTextFile

csvFile = _ctx.csvFile

binaryFile = _ctx.binaryFile

tableFile = _ctx.tableFile

beansdb = _ctx.beansdb

union = _ctx.union

zip = _ctx.zip

start = _ctx.start

stop = _ctx.stop

clear = _ctx.clear

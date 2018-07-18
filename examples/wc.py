from dpark import DparkContext, optParser

dc = DparkContext()
options, args = optParser.parse_args()
infile = args[0]
outfile = args[1]
print("from {} to {}".format(infile, outfile))


def fm(x):
    for w in x.strip().split():
        yield (w, 1)


(dc.textFile(infile)
    .flatMap(fm)
    .reduceByKey(lambda x, y: x + y, numSplits=6)
    .map(lambda x: " ".join(list(map(str, x))))
    .saveAsTextFile(outfile, overwrite=False))

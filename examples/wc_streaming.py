import time
import logging
from dpark.dstream import StreamingContext
from dpark import optParser

def load_context(checkpointDir=None):
    if checkpointDir:
        try:
            ssc, start_time = StreamingContext.load(checkpointDir)
            return ssc, start_time
        except:
            logging.warning('Failed to load checkpoint from %s, start fresh', checkpointDir)
            
    ssc = StreamingContext(1)
    return ssc, None

if __name__ == '__main__':
    options, args = optParser.parse_args()
    host, port = args
    port = int(port)

    ssc, start_time = load_context(options.checkpoint_dir)
    if start_time is None:
        ssc.checkpoint(options.checkpoint_dir, 20)
        st = ssc.networkTextStream(host, port) \
                .flatMap(lambda x: x.split()) \
                .map(lambda x: (x,1)) \
                .updateStateByKey(lambda vs, state: sum(vs) + (state or 0)) \
                .show()

    ssc.start(start_time)
    ssc.awaitTermination()

import urllib
import logging
import pickle
from env import env

class ShuffleFetcher:
    def fetch(self, shuffleId, reduceId, func):
        raise NotImplementedError
    def stop(self):
        pass

class SimpleShuffleFetcher(ShuffleFetcher):
    def fetch(self, shuffleId, reduceId, func):
        logging.info("Fetching outputs for shuffle %d, reduce %d", shuffleId, reduceId)
        splitsByUri = {}
        serverUris = env.mapOutputTracker.getServerUris(shuffleId)
        for i, uri in enumerate(serverUris):
            splitsByUri.setdefault(uri, []).append(i)
        for uri, ids in splitsByUri.items():
            for id in ids:
                try:
                    url = "%s/%d/%d/%d" % (uri, shuffleId, id, reduceId)
                    logging.info("fetch %s", url)
                    for k,v in pickle.loads(urllib.urlopen(url).read()):
                        logging.info("read %s : %s", k, v)
                        func(k,v)
                except IOError, e:
                    logging.error("Fetch failed for shuffle %d, reduce %d, %d, %s", shuffleId, reduceId, i, url)
                    raise 


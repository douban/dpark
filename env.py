
class SparkEnv:
    def create(self, isMaster):
        from tracker import MapOutputTracker, CacheTracker
        from fetch import SimpleShuffleFetcher
        #self.cache = Cache()
        #self.cacheTracker = CacheTracker(isMaster, None)
        self.mapOutputTracker = MapOutputTracker(isMaster)
        self.shuffleFetcher = SimpleShuffleFetcher()

env = SparkEnv()


class SparkEnv:
    def create(self, isMaster):
        from cache import BoundedMemoryCache, CacheTracker
        from tracker import MapOutputTracker
        from fetch import SimpleShuffleFetcher
        self.cache = BoundedMemoryCache()
        self.cacheTracker = CacheTracker(isMaster, self.cache)
        self.mapOutputTracker = MapOutputTracker(isMaster)
        self.shuffleFetcher = SimpleShuffleFetcher()

env = SparkEnv()

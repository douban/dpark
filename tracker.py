

class MapOutputTracker:
    def __init__(self, isMaster):
        self.serverUris = {}
        if isMaster:
            pass # TODO
        self.generation = 0

    def registerMapOutput(self, shuffleId, numMaps, mapId, serverUri):
        self.serverUris.setdefault(shuffleId, [None] * numMaps)[mapId] = serverUri

    def registerMapOutputs(self, shuffleId, locs):
        self.serverUris[shuffleId] = locs

    def unregisterMapOutput(self, shuffleId, mapId, serverUri):
        locs = self.serverUris.get(shuffleId, None)
        if locs is not None:
            if locs[mapId] == serverUri:
                locs[mapId] = None
            self.incrementGeneration()

    def getServerUris(self, shuffleId):
        return self.serverUris.get(shuffleId)

    def getMapOutputUri(self, serverUri, shuffleId, mapId, reduceId):
        return "%s/shuffle/%s/%s/%s" % (serverUri, shuffleId, mapId, reduceId)

    def stop(self):
        self.serverUris.clear()

    def incrementGeneration(self):
        self.generation += 1

    def getGeneration(self):
        return self.generation

    def updateGeneration(self, newGen):
        if newGen > self.generation:
            logging.info("Updating generation to %d and clearing cache", newGen)
            self.generation = newGen
            self.serverUris.clear()


class CacheTracker:
    def __init__(self, isMaster, theCache):
        self.cache = theCache.newKeySpace()
        self.registeredRddIds = {}
        self.loading = {}

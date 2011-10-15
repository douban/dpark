import os, os.path
import random
import logging

class LocalFileShuffle:
    initialized = False

    @classmethod
    def initializeIfNeeded(cls):
        if cls.initialized:
            return
        localDirRoot = "/tmp/spark/"
        tries = 0
        foundLocalDir = False
         
        while tries < 10:
            path = os.path.join(localDirRoot,str(random.randint(1, 100)))
            if not os.path.exists(path):
                foundLocalDir = True
                os.makedirs(path)
                break
        if not foundLocalDir:
            raise Exception("no dir")
        localDir = path
        shuffleDir = os.path.join(localDir, "shuffle")
        os.makedirs(shuffleDir)
        logging.info("shuffle dir: %s", shuffleDir)

        cls.shuffleDir = shuffleDir
        cls.serverUri = "file:///" + shuffleDir
        cls.initialized = True

    @classmethod
    def getOutputFile(cls, shuffleId, inputId, outputId):
        cls.initializeIfNeeded()
        path = os.path.join(cls.shuffleDir, str(shuffleId), str(inputId))
        if not os.path.exists(path):
            os.makedirs(path)
        return os.path.join(path, str(outputId))

    @classmethod
    def getServerUri(cls):
        cls.initializeIfNeeded()
        return cls.serverUri

    nextShuffleId = 0
    @classmethod
    def newShuffleId(cls):
        cls.nextShuffleId += 1
        return cls.nextShuffleId

#LocalFileShuffle.initializeIfNeeded()

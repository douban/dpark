import os
import uuid

REGISTER_BROADCAST_TRACKER = 0
UNREGISTER_BROADCAST_TRACKER = 1
FIND_BROADCAST_TRACKER = 2
GET_UPDATED_SHARE = 3


class Broadcast:

    initialized = False
    isMaster = False
    broadcastFactory = None

    def __init__(self):
        self.uuid = uuid.uuid4()

    def value(self):
        raise NotImplementedError
    
    @classmethod
    def initialize(cls, isMaster):
        if cls.initialized:
            return

        cls.broadcastFactory = DfsBroadcastFactory()
        cls.isMaster = isMaster
        cls.masterHostAddress = '' #TODO
        cls.broadcastFactory.initialize(isMaster)
        cls.initialized = True
    
    @classmethod
    def getBroadcastFactory(cls):
        return cls.broadcastFactory



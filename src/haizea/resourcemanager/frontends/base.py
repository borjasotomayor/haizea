from haizea.common.utils import abstract

class RequestFrontend(object):
    def __init__(self, rm):
        self.rm = rm
    
    def getAccumulatedRequests(self): abstract()
    
    def existsMoreRequests(self): abstract()
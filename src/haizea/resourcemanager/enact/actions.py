class EnactmentAction(object):
    def __init__(self):
        self.leaseHaizeaID = None
        self.leaseEnactmentInfo = None
            
    def fromRR(self, rr):
        self.leaseHaizeaID = rr.lease.leaseID
        self.leaseEnactmentInfo = rr.lease.enactmentInfo
        
class VNode(object):
    def __init__(self, enactmentInfo):
        self.enactmentInfo = enactmentInfo
        self.pnode = None
        self.res = None
        self.diskimage = None
        
class VMEnactmentAction(EnactmentAction):
    def __init__(self):
        EnactmentAction.__init__(self)
        self.vnodes = {}
    
    def fromRR(self, rr):
        EnactmentAction.fromRR(self, rr)
        self.vnodes = dict([(vnode, VNode(info)) for (vnode,info) in rr.lease.vnodeEnactmentInfo.items()])

class VMEnactmentStartAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentStopAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentSuspendAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentResumeAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentConfirmSuspendAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentConfirmResumeAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

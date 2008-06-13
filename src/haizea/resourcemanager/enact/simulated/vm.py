from haizea.resourcemanager.enact.base import VMEnactmentBase

class VMEnactment(VMEnactmentBase):
    def __init__(self, resourcepool):
        VMEnactmentBase.__init__(self, resourcepool)
        
    def start(self, vms):
        for vm in vms:
            pass
    
    def stop(self, vms):
        for vm in vms:
            pass

    def suspend(self, vms):
        for vm in vms:
            pass

    def resume(self, vms):
        for vm in vms:
            pass

import workspace.haizea.datastruct as ds

class Scheduler(object):
    def __init__(self, rm):
        self.rm = rm
        self.slottable = ds.SlotTable(self)
        self.queue = ds.Queue(self)
        self.scheduledleases = ds.LeaseTable(self)
        self.completedleases = ds.LeaseTable(self)
        
    def schedule(self, requests):
        # Process starting/ending reservations
        
        # Process lease requests
        
        # Process queue
        
        # return (accepted, rejected) leases
        pass
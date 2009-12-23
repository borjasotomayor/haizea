# -------------------------------------------------------------------------- #
# Copyright 2006-2009, University of Chicago                                 #
# Copyright 2008-2009, Distributed Systems Architecture Group, Universidad   #
# Complutense de Madrid (dsa-research.org)                                   #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License. You may obtain    #
# a copy of the License at                                                   #
#                                                                            #
# http://www.apache.org/licenses/LICENSE-2.0                                 #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
# -------------------------------------------------------------------------- #

from haizea.core.scheduler.slottable import ResourceReservation
import haizea.common.constants as constants
import sys

class SchedException(Exception):
    """The base class for scheduling exceptions"""
    pass

class NotSchedulableException(SchedException):
    """A simple exception class used when a lease cannot be scheduled
    
    This exception must be raised when a lease cannot be scheduled
    """
    
    def __init__(self, reason):
        self.reason = reason

class CancelLeaseException(SchedException):
    pass

class NormalEndLeaseException(SchedException):
    pass

class RescheduleLeaseException(SchedException):
    pass


class SchedulingError(Exception):
    """The base class for scheduling errors"""
    pass

class InconsistentScheduleError(SchedulingError):
    pass

class InconsistentLeaseStateError(SchedulingError):
    def __init__(self, lease, doing):
        self.lease = lease
        self.doing = doing
        
        self.message = "Lease %i is in an inconsistent state (%i) when %s" % (lease.id, lease.get_state(), doing)

class EnactmentError(SchedulingError):
    pass

class UnrecoverableError(SchedulingError):
    def __init__(self, exc):
        self.exc = exc
        self.exc_info = sys.exc_info()
        
    def get_traceback(self):
        return self.exc_info[2]


class ReservationEventHandler(object):
    """A wrapper for reservation event handlers.
    
    Reservations (in the slot table) can start and they can end. This class
    provides a convenient wrapper around the event handlers for these two
    events (see Scheduler.__register_handler for details on event handlers)
    """
    def __init__(self, sched, on_start, on_end):
        self.sched = sched
        self.on_start_method = on_start
        self.on_end_method = on_end
        
    def on_start(self, lease, rr):
        self.on_start_method(self.sched, lease, rr)
        
    def on_end(self, lease, rr):
        self.on_end_method(self.sched, lease, rr)        
        
class EarliestStartingTime(object):
    EARLIEST_NOPREPARATION = 0
    EARLIEST_MIGRATION = 1
    
    def __init__(self, time, type):
        self.time = time
        self.type = type  
        
class MigrationResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vmrr, transfers):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.transfers = transfers
        
    def clear_rrs(self):
        self.vmrr = None
        
        
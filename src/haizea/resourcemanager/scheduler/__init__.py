# -------------------------------------------------------------------------- #
# Copyright 2006-2008, University of Chicago                                 #
# Copyright 2008, Distributed Systems Architecture Group, Universidad        #
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


class SchedException(Exception):
    """A simple exception class used for scheduling exceptions"""
    pass

class NotSchedulableException(SchedException):
    """A simple exception class used when a lease cannot be scheduled
    
    This exception must be raised when a lease cannot be scheduled
    (this is not necessarily an error condition, but the scheduler will
    have to react to it)
    """
    pass

class CriticalSchedException(SchedException):
    """A simple exception class used for critical scheduling exceptions
    
    This exception must be raised when a non-recoverable error happens
    (e.g., when there are unexplained inconsistencies in the schedule,
    typically resulting from a code error)
    """
    pass

class PreparationSchedException(SchedException):
    pass

class CancelLeaseException(Exception):
    pass

class NormalEndLeaseException(Exception):
    pass

class RescheduleLeaseException(SchedException):
    pass


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
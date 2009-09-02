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

"""Accounting probes that collect data on resource utilization"""

from haizea.core.accounting import AccountingProbe, AccountingDataCollection

class CPUUtilizationProbe(AccountingProbe):
    """
    Collects information on CPU utilization
    
    * Counters
    
      - "CPU utilization": Amount of CPU resources used in the entire site
        at a given time. The value ranges between 0 and 1.

    """    
    COUNTER_UTILIZATION="CPU utilization"        
    
    def __init__(self, accounting):
        """See AccountingProbe.__init__"""        
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_counter(UtilizationProbe.COUNTER_UTILIZATION, AccountingDataCollection.AVERAGE_NONE)
        
    def at_timestep(self, lease_scheduler):
        """See AccountingProbe.at_timestep"""
        self.accounting.append_to_counter(UtilizationProbe.COUNTER_UTILIZATION, 0)


class DiskUsageProbe(AccountingProbe):
    """
    Collects information on disk usage
    
    * Counters
    
      - "Disk usage": Maximum disk space used across nodes.

    """    
    COUNTER_DISKUSAGE="Disk usage"
    
    def __init__(self, accounting):
        """See AccountingProbe.__init__"""        
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_counter(UtilizationProbe.COUNTER_DISKUSAGE, AccountingDataCollection.AVERAGE_NONE)
        
    def at_timestep(self, lease_scheduler):
        """See AccountingProbe.at_timestep"""
        pass

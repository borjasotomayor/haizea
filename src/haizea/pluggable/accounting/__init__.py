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

"""This package contains modules with pluggable accounting probes for Haizea.
"""

# The following dictionaries provide a shorthand notation to refer to
# the accounting probes (this shorthand is used in the configuration file,
# so the fully-qualified class name doesn't have to be written)
probe_class_mappings = {             "ar": "haizea.pluggable.accounting.leases.ARProbe",
                            "best-effort": "haizea.pluggable.accounting.leases.BEProbe",
                              "immediate": "haizea.pluggable.accounting.leases.IMProbe",
                        "cpu-utilization": "haizea.pluggable.accounting.utilization.CPUUtilizationProbe",
                             "disk-usage": "haizea.pluggable.accounting.utilization.DiskUsageProbe",
                        }

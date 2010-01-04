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

"""This package contains modules with pluggable policies for Haizea.
"""

# The following dictionaries provide a shorthand notation to refer to
# the policy classes (this shorthand is used in the configuration file,
# so the fully-qualified class name doesn't have to be written)
admission_class_mappings = {"accept-all": "haizea.pluggable.policies.admission.AcceptAllPolicy",
                            "no-ARs": "haizea.pluggable.policies.admission.NoARsPolicy"}

preemption_class_mappings = {"no-preemption": "haizea.pluggable.policies.preemption.NoPreemptionPolicy",
                             "ar-preempts-everything": "haizea.pluggable.policies.preemption.ARPreemptsEverythingPolicy",
                             "deadline":"haizea.pluggable.policies.preemption.DeadlinePolicy"}

host_class_mappings = {"no-policy": "haizea.pluggable.policies.host_selection.NoPolicy",
                       "greedy": "haizea.pluggable.policies.host_selection.GreedyPolicy"}

pricing_mappings = {"free": "haizea.pluggable.policies.pricing.FreePolicy",
                    "always-fair": "haizea.pluggable.policies.pricing.AlwaysFairPricePolicy",
                    "random": "haizea.pluggable.policies.pricing.RandomMultipleOfFairPricePolicy",
                    "maximum": "haizea.pluggable.policies.pricing.MaxMultipleOfFairPricePolicy",
                    "adaptive": "haizea.pluggable.policies.pricing.AdaptiveFairPricePolicy"}
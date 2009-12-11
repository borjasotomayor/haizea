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

import random
import operator 


class Distribution(object):
    def __init__(self):
        pass
    
    def seed(self, x):
        random.seed(x)

class ContinuousDistribution(Distribution):
    def __init__(self):
        pass
        
    def get(self): 
        abstract()
            
    def get_list(self, n):
        l = []
        for i in xrange(1, n):
            l.append(self.get())
        return l

        
class BoundedContinuousDistribution(ContinuousDistribution):
    def __init__(self, min, max):
        ContinuousDistribution.__init__(self)
        self.min = float(min)
        self.max = float(max)
        

class UniformDistribution(BoundedContinuousDistribution):
    def __init__(self, min, max):
        BoundedContinuousDistribution.__init__(self, min, max)
        
    def get(self):
        return random.uniform(self.min, self.max)
                    
class NormalDistribution(ContinuousDistribution):
    def __init__(self, mu, sigma):
        ContinuousDistribution.__init__(self)
        self.mu = mu
        self.sigma = sigma
        
    def get(self):
        return random.normalvariate(self.mu, self.sigma)
    
class BoundedNormalDistribution(BoundedContinuousDistribution):
    def __init__(self, min, max, mu, sigma):
        BoundedContinuousDistribution.__init__(self, min, max)
        self.mu = float(mu)
        self.sigma = float(sigma)
        
    def get(self):
        n = random.normalvariate(self.mu, self.sigma) 
        if n < self.min:
            n = self.min
        elif n > self.max:
            n = self.max
        return n
        
    
class BoundedParetoDistribution(BoundedContinuousDistribution):
    def __init__(self, min, max, alpha, invert = False):
        BoundedContinuousDistribution.__init__(self, min, max)
        self.alpha = float(alpha)
        self.invert = invert
        
    def get(self):
        u = random.random()
        l = self.min
        h = self.max
        a = self.alpha
        p = (-((u*h**a - u*l**a - h**a)/((h**a)*(l**a))))**(-1/a)
        if self.invert:
            p = h - p
        return p
                  
            
class DiscreteDistribution(Distribution):
    def __init__(self, values, probabilities):
        self.values = values
        self.probabilities = probabilities[:]
        self.accumprobabilities = probabilities[:]
        accum = 0.0
        for i, prob in enumerate(self.probabilities):
            accum += prob
            self.accumprobabilities[i] = accum
        self.num_values = len(self.values)

    # Expects value in [0,1)
    def _get_from_prob(self, prob):
        pos = None
        for i, p in enumerate(self.accumprobabilities):
            if prob < p:
                pos = i
                break
        return self.values[pos]
    
class DiscreteUniformDistribution(DiscreteDistribution):
    def __init__(self, values):
        probabilities= [1.0/len(values)] * len(values)
        DiscreteDistributionBase.__init__(self, values, probabilities)
        
    def get(self):
        return self._get_from_prob(random.random())            
    
    

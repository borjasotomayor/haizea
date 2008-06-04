import random
import operator 

TESTDIST_NUM_ITERS=10000

class DiscreteDistributionBase(object):
    def __init__(self, values, probabilities):
        self.values = values
        self.probabilities = probabilities[:]
        self.accumprobabilities = probabilities[:]
        accum = 0.0
        for i,prob in enumerate(self.probabilities):
            accum += prob
            self.accumprobabilities[i] = accum
        self.numValues = len(self.values)
        
    def getAvg(self):
        return reduce(operator.add, [x[0]*x[1] for x in zip(self.values,self.probabilities)])

    def getValueFromProb(self, prob):
        pos = None
        for i,p in enumerate(self.accumprobabilities):
            if prob < p:
                pos = i
                break #Ugh
        return self.values[pos]
    
    def testDistribution(self):
        vals = []
        histogram = {}
        for v in self.values:
            histogram[v]=0
        for i in xrange(1,TESTDIST_NUM_ITERS):
            v = self.get()
            vals.append(v)
            histogram[v] += 1

        for k in histogram:
            histogram[k] = float(histogram[k]) / float(TESTDIST_NUM_ITERS)
        
        print histogram

    
    
class DiscreteDistribution(DiscreteDistributionBase):
    def __init__(self,values,probabilities):
        DiscreteDistributionBase.__init__(self,values,probabilities)
        
    def get(self):
        return self.getValueFromProb(random.random())

        
class DiscreteUniformDistribution(DiscreteDistributionBase):
    def __init__(self,values):
        probabilities= [1.0/len(values)] * len(values)
        DiscreteDistributionBase.__init__(self,values,probabilities)
        
    def get(self):
        return self.getValueFromProb(random.random())

        
class ContinuousDistributionBase(object):
    def __init__(self,min,max):
        self.min = float(min)
        self.max = float(max)
        
    def getList(self, n):
        l = []
        for i in xrange(1,n):
            l.append(self.get())
        return l
    
class ContinuousUniformDistribution(ContinuousDistributionBase):
    def __init__(self,min,max):
        ContinuousDistributionBase.__init__(self,min,max)
        
    def get(self):
        return random.uniform(self.min, self.max)
                    
class ContinuousNormalDistribution(ContinuousDistributionBase):
    def __init__(self,min,max,mu,sigma):
        ContinuousDistributionBase.__init__(self,min,max)
        self.mu = mu
        self.sigma = sigma
        
    def get(self):
        valid = False
        while not valid:
            number = random.normalvariate(self.mu, self.sigma)
            if number >= self.min and number <= self.max:
                valid = True
        return number
    
class ContinuousParetoDistribution(ContinuousDistributionBase):
    def __init__(self,min,max,alpha):
        ContinuousDistributionBase.__init__(self,min,max)
        self.alpha = alpha
        
    def get(self):
        return random.paretovariate(self.alpha)
                  
            
            
    
    

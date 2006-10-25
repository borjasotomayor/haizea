import random
from workspace.graphing import graph

TESTDIST_NUM_ITERS=10000

class DiscreteDistributionBase(object):
    def __init__(self, values, probabilities):
        self.values = values
        self.probabilities = probabilities
        accum = 0.0
        for i,prob in enumerate(self.probabilities):
            accum += prob
            self.probabilities[i] = accum
        self.numValues = len(self.values)

    def getValueFromProb(self, prob):
        pos = None
        for i,p in enumerate(self.probabilities):
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
    
    def testDistribution(self):
        g = graph.ContinuousHistogram(self.getList(1000))
        g.plot()
        g.show()
    
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
#        valid = False
#        while not valid:
#            number = random.normalvariate(self.mu, self.sigma)
#            if number >= self.min and number <= self.max:
#                valid = True
#        return number   
        return random.paretovariate(self.alpha)
                        
if __name__ == "__main__":    
#    values=["A","B","C","D"]
#    probs=[0.2,0.35,0.15,0.3]
#    uniform = DiscreteUniformDistribution(values,probs)
#    
#    vals = []
#    histogram = {}
#    for v in values:
#        histogram[v]=0
#    for i in xrange(1,10000):
#        v = uniform.get()
#        vals.append(v)
#        histogram[v] += 1
#    
#    for k in histogram:
#        histogram[k] = float(histogram[k]) / 10000.0
#    
#    print histogram

#    dist = ContinuousNormalDistribution(10,90, 50,20)
    dist = ContinuousParetoDistribution(10,90, 5)
    l = dist.getList(10000)

    g = graph.ContinuousHistogram(l)
    g.plot()
    g.show()
    
    

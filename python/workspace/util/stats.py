import random
from workspace.graphing import graph

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
    
    
class DiscreteUniformDistribution(DiscreteDistributionBase):
    def __init__(self,values,probabilities):
        DiscreteDistributionBase.__init__(self,values,probabilities)
        
    def get(self):
        return self.getValueFromProb(random.random())
        
        
class ContinuousDistributionBase(object):
    def __init__(self,min,max):
        self.min = min
        self.max = max
        
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

    dist = ContinuousUniformDistribution(1,100)
    l = dist.getList(1000)
    
    g = graph.ContinuousHistogram(l)
    g.plot()
    g.show()
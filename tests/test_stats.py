from sample_slottables import *
import haizea.common.stats as stats
import math

# 64 seed values, generated using int(random.uniform(1,2**32-1))
SEEDS = [660756695, 1080106124, 441535308, 1531785557, 3449773776, 2239192905, 1944782933, 377958281,
        1698866825, 4281919021, 2985069635, 1929791444, 2054454583, 3428593444, 3259033264, 643731936,
        2921350595, 1575932719, 2236362645, 1020609972, 1592461297, 1460695161, 1636954632, 76307538,
        862656448, 2450493480, 247968499, 766348682, 3084561872, 1179378301, 1391629128, 1038658793,
        3582609773, 392253809, 2732213167, 3688908610, 866221636, 1817396766, 3402959080, 2653694808,
        1596091165, 188549655, 1900651916, 1577002145, 3060320535, 1268074655, 1752021485, 2783937267,
        3482472935, 1513342535, 1655096731, 2485501475, 3972059090, 822958367, 4172029370, 3057570066,
        1599256642, 2858736230, 1414979451, 303997155, 3247160141, 1629523852, 2258358509, 2132879613]

PAIRS = [(93.0, 93.0), (245.0, 178.0), (32.0, 234.0), (9.0, 151.0)]

NUM_VALUES = 10000

def mean(l):
    return sum(l, 0.0) / len(l)

def stdev(l, m):
    return math.sqrt(sum([(x - m)**2 for x in l], 0.0) / len(l))

class TestStats(object):
    def __init__(self):
        pass
    
    def do_test(self, distribution, expected_mean, expected_stdev, error):
        for seed in SEEDS:
            distribution.seed(seed)
            l = distribution.get_list(NUM_VALUES)
            m = mean(l)
            diff = abs(expected_mean - mean(l))
            assert(diff < error)
            s = stdev(l, m)
            if expected_stdev != None:
                diff = abs(expected_stdev - stdev(l, m))
                assert(diff < error)
            
    def test_uniform(self):
        for a, length in PAIRS:
            b = a + length
            dist = stats.UniformDistribution(a, b)
            expected_mean = (a+b)/2
            expected_stdev = math.sqrt((b-a)**2 / 12)
            error = length * 0.01
            self.do_test(dist, expected_mean, expected_stdev, error)
            
    def test_normal(self):
        for mu, sigma in PAIRS:
            print mu,sigma
            dist = stats.NormalDistribution(mu, sigma)
            error = (sigma / math.sqrt(NUM_VALUES)) * 3
            self.do_test(dist, mu, sigma, error)
            
    def test_pareto(self):
        for l, length in PAIRS:
            h = l + length
            for a in [2.01, 2.5, 3.0, 3.5]:
                dist = stats.BoundedParetoDistribution(l, h, a)
                expected_mean = (l**a/(1-(l/h)**a)) * (a/(a-1)) * ((1/l**(a-1)) - (1/h**(a-1)))
                expected_stdev = None # No formula to test this
                error = length * 0.01
                self.do_test(dist, expected_mean, expected_stdev, error)         
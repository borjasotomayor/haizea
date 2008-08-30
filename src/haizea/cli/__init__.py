from haizea.cli.optionparser import OptionParser

class Command(object):
    
    def __init__(self, argv):
        self.argv = argv
        self.optparser = OptionParser()
        
    def parse_options(self):
        opt, args = self.optparser.parse_args(self.argv)
        self.opt = opt
        
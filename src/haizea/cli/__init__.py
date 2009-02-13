from haizea.cli.optionparser import OptionParser, Option

class Command(object):
    
    def __init__(self, argv):
        self.argv = argv
        self.optparser = OptionParser()
        self.optparser.add_option(Option("-D", "--debug", action="store_true", dest="debug",
                                         help = """
                                         Run command in debug mode.
                                         """))
        
    def parse_options(self):
        opt, args = self.optparser.parse_args(self.argv)
        self.opt = opt
        self.args = args
        
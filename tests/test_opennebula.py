from common import BaseOpenNebulaTest

class TestOpenNebula(BaseOpenNebulaTest):
    def __init__(self):
        self.config = self.load_configfile("base_config_opennebula.conf")
   
    def test_twoleases(self):
        self.do_test("one-twoleases.db")          
        
    def test_threeleases(self):
        self.do_test("one-threeleases.db")
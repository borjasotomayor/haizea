import xmlrpclib
import os
import hashlib
import xml.etree.ElementTree as ET

class OpenNebulaXMLRPCClient(object):
    def __init__(self, host, port, user, password):
        uri = "http://%s:%i" % (host, port)
        self.rpc = xmlrpclib.ServerProxy(uri)
        try:
            methods = self.rpc.system.listMethods()
        except xmlrpclib.Fault, err:
            raise Exception("Cannot connect to ONE XML RPC server at %s" % uri)        
        
        if not set(["one.hostpool.info", 
                    "one.host.info",
                    "one.vmpool.info", 
                    "one.vm.info"]).issubset(set(methods)):
            raise Exception("XML RPC server does not support required methods. OpenNebula 1.4 or higher is required.")
    
        passhash = hashlib.sha1(password).hexdigest()
        
        self.auth = "%s:%s" % (user, passhash)
        
    @staticmethod
    def get_userpass_from_env():
        if not os.environ.has_key("ONE_AUTH"):
            return None
        else:
            auth = os.environ["ONE_AUTH"]
            user, passw = auth.split(":")
            return user, passw
        
    def hostpool_info(self):
        try:
            (rc, value) = self.rpc.one.hostpool.info(self.auth)
            if rc == False:
                raise Exception("ONE reported an error: %s" % value)
            else:
                hosts = OpenNebulaHost.from_hostpool_xml(value)
                return hosts
        except xmlrpclib.Fault, err:
            raise Exception("XMLRPC fault: %s" % err.faultString)
        
    def host_info(self, hid):
        try:
            (rc, value) = self.rpc.one.host.info(self.auth, hid)
            if rc == False:
                raise Exception("ONE reported an error: %s" % value)
            else:
                host = OpenNebulaHost.from_host_xml(value)
                return host
        except xmlrpclib.Fault, err:
            raise Exception("XMLRPC fault: %s" % err.faultString)     
        
    def vmpool_info(self):
        try:
            (rc, value) = self.rpc.one.vmpool.info(self.auth, -2) # -2: Get all VMs
            if rc == False:
                raise Exception("ONE reported an error: %s" % value)
            else:
                hosts = OpenNebulaVM.from_vmpool_xml(value)
                return hosts
        except xmlrpclib.Fault, err:
            raise Exception("XMLRPC fault: %s" % err.faultString)
        
    def vm_info(self, id):
        try:
            (rc, value) = self.rpc.one.vm.info(self.auth, id)
            if rc == False:
                raise Exception("ONE reported an error: %s" % value)
            else:
                host = OpenNebulaVM.from_vm_xml(value)
                return host
        except xmlrpclib.Fault, err:
            raise Exception("XMLRPC fault: %s" % err.faultString)     
        
    def vm_deploy(self, vid, hid):
        try:
            rv = self.rpc.one.vm.deploy(self.auth, vid, hid)
            if rv[0] == False:
                raise Exception("ONE reported an error: %s" % rv[1])
            else:
                return
        except xmlrpclib.Fault, err:
            raise Exception("XMLRPC fault: %s" % err.faultString)                    

    def vm_action(self, action, vid):
        if not action in ["shutdown", "hold", "release", "stop", 
                          "cancel", "suspend", "resume", "restart", 
                          "finalize" ]:
            raise Exception("%s is not a valid action" % action)
        try:
            rv = self.rpc.one.vm.action(self.auth, action, vid)
            if rv[0] == False:
                raise Exception("ONE reported an error: %s" % rv[1])
            else:
                return
        except xmlrpclib.Fault, err:
            raise Exception("XMLRPC fault: %s" % err.faultString)  
        
    def vm_shutdown(self, vid):
        return self.vm_action("shutdown", vid)                  

    def vm_hold(self, vid):
        return self.vm_action("hold", vid)                  

    def vm_release(self, vid):
        return self.vm_action("release", vid)                  

    def vm_stop(self, vid):
        return self.vm_action("stop", vid)                  

    def vm_cancel(self, vid):
        return self.vm_action("cancel", vid)                  

    def vm_suspend(self, vid):
        return self.vm_action("suspend", vid)                  

    def vm_resume(self, vid):
        return self.vm_action("resume", vid)                  

    def vm_restart(self, vid):
        return self.vm_action("restart", vid)                  

    def vm_finalize(self, vid):
        return self.vm_action("finalize", vid)                  

    
class OpenNebulaHost(object):

    STATE_INIT       = 0
    STATE_MONITORING = 1
    STATE_MONITORED  = 2
    STATE_ERROR      = 3
    STATE_DISABLED   = 4

    
    def __init__(self, host_element):
        self.id = int(host_element.find("ID").text)
        self.name = host_element.find("NAME").text
        self.state = int(host_element.find("STATE").text)
        self.im_mad = host_element.find("IM_MAD").text
        self.vm_mad = host_element.find("VM_MAD").text
        self.tm_mad = host_element.find("TM_MAD").text
        self.last_mon_time = int(host_element.find("LAST_MON_TIME").text)
        
        host_share_element = host_element.find("HOST_SHARE")

        self.disk_usage = int(host_share_element.find("DISK_USAGE").text)
        self.mem_usage = int(host_share_element.find("MEM_USAGE").text)
        self.cpu_usage = int(host_share_element.find("CPU_USAGE").text)
        self.max_disk = int(host_share_element.find("MAX_DISK").text)
        self.max_mem = int(host_share_element.find("MAX_MEM").text)
        self.max_cpu = int(host_share_element.find("MAX_CPU").text)
        self.free_disk = int(host_share_element.find("FREE_DISK").text)
        self.free_mem = int(host_share_element.find("FREE_MEM").text)
        self.free_cpu = int(host_share_element.find("FREE_CPU").text)
        self.used_disk = int(host_share_element.find("USED_DISK").text)
        self.used_mem = int(host_share_element.find("USED_MEM").text)
        self.used_cpu = int(host_share_element.find("USED_CPU").text)
        self.running_vms = int(host_share_element.find("RUNNING_VMS").text)
        
        self.template = parse_template(host_element.find("TEMPLATE"))
           

    @classmethod
    def from_host_xml(cls, xmlstr):
        host_element = ET.fromstring(xmlstr)
        return cls(host_element)
    
    @classmethod
    def from_hostpool_xml(cls, xmlstr):
        hostpool_element = ET.fromstring(xmlstr)
        host_elements = hostpool_element.findall("HOST")
        return [cls(host_element) for host_element in host_elements]
    
class OpenNebulaVM(object):

    STATE_INIT      = 0
    STATE_PENDING   = 1
    STATE_HOLD      = 2
    STATE_ACTIVE    = 3
    STATE_STOPPED   = 4
    STATE_SUSPENDED = 5
    STATE_DONE      = 6
    STATE_FAILED    = 7
    
    LCMSTATE_LCM_INIT       = 0
    LCMSTATE_PROLOG         = 1
    LCMSTATE_BOOT           = 2
    LCMSTATE_RUNNING        = 3
    LCMSTATE_MIGRATE        = 4
    LCMSTATE_SAVE_STOP      = 5
    LCMSTATE_SAVE_SUSPEND   = 6
    LCMSTATE_SAVE_MIGRATE   = 7
    LCMSTATE_PROLOG_MIGRATE = 8
    LCMSTATE_PROLOG_RESUME  = 9
    LCMSTATE_EPILOG_STOP    = 10
    LCMSTATE_EPILOG         = 11
    LCMSTATE_SHUTDOWN       = 12
    LCMSTATE_CANCEL         = 13
    LCMSTATE_FAILURE        = 14
    LCMSTATE_DELETE         = 15
    LCMSTATE_UNKNOWN        = 16
    

    def __init__(self, vm_element):
        self.id = int(vm_element.find("ID").text)
        self.uid = int(vm_element.find("UID").text)
        username_element = vm_element.find("USERNAME")
        if username_element == None:
            self.username = None
        else:
            self.username = username_element.text   
        self.name = vm_element.find("NAME").text
        self.last_poll = int(vm_element.find("LAST_POLL").text)
        self.state = int(vm_element.find("STATE").text)
        self.lcm_state = int(vm_element.find("LCM_STATE").text)
        self.stime = int(vm_element.find("STIME").text)
        self.etime = int(vm_element.find("ETIME").text)
        deploy_id = vm_element.find("DEPLOY_ID").text
        if deploy_id == None:
            self.deploy_id = None
        else:
            self.deploy_id = deploy_id
        self.memory = int(vm_element.find("MEMORY").text)
        self.cpu = int(vm_element.find("CPU").text)
        self.net_tx = int(vm_element.find("NET_TX").text)
        self.net_rx = int(vm_element.find("NET_RX").text)

        self.template = parse_template(vm_element.find("TEMPLATE"))

    
    @classmethod
    def from_vm_xml(cls, xmlstr):
        vm_element = ET.fromstring(xmlstr)
        return cls(vm_element)
    
    @classmethod
    def from_vmpool_xml(cls, xmlstr):
        vmpool_element = ET.fromstring(xmlstr)
        vm_elements = vmpool_element.findall("VM")
        return [cls(vm_element) for vm_element in vm_elements]
    
def parse_template(template_element):
    template = {}
    if template_element != None:
        for subelement in template_element:
            name = subelement.tag
            if len(subelement) == 0:
                template[name] = subelement.text
            else:
                template[name] = {}
                for subsubelement in subelement:
                    template[name][subsubelement.tag] = subsubelement.text
                    
    return template
    
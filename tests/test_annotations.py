from haizea.core.leases import LeaseWorkload, LeaseAnnotation, LeaseAnnotations, UnmanagedSoftwareEnvironment, DiskImageSoftwareEnvironment, Timestamp
from mx.DateTime import Time, DateTime
from common import *
from haizea.common.utils import reset_lease_id_counter

def get_config():
    return load_configfile("base_config_simulator.conf")
            
def test_annotation1():
    reset_lease_id_counter()
    lease_workload = LeaseWorkload.from_xml_file("preemption.lwf")
    annotations = LeaseAnnotations.from_xml_file("annotations1.lwfa")
    leases = lease_workload.get_leases()

    annotations.apply_to_leases(leases)

    lease1 = leases[0]
    lease2 = leases[1]

    assert lease1.start.requested == DateTime(0, 1, 1, 10)
    assert lease1.deadline == DateTime(0, 1, 1, 20)
    assert isinstance(lease1.software, UnmanagedSoftwareEnvironment)
    assert lease1.extras["one"] == "1"
    assert lease1.extras["two"] == "2"
        
    assert lease2.start.requested == Timestamp.UNSPECIFIED
    assert lease2.deadline == None
    assert isinstance(lease2.software, DiskImageSoftwareEnvironment)
    assert lease2.software.image_id == "annotation.img"
    assert lease2.software.image_size == 4096
    assert lease2.extras["three"] == "3"
    assert lease2.extras["four"] == "4"

def test_annotation_simul():
    c= get_config()
    c.set("tracefile", "annotationfile", "annotations2.lwfa")
    h = load_tracefile(c, PREEMPTION_TRACE)
    h.start()    
   
    lease1 = h.scheduler.completed_leases.get_lease(1)
    lease2 = h.scheduler.completed_leases.get_lease(2)
    
    assert lease1.start.requested == DateTime(2006, 11, 25, 14)
    assert lease1.deadline == None
    assert isinstance(lease1.software, DiskImageSoftwareEnvironment)
    assert lease1.software.image_id == "annotation1.img"
    assert lease1.software.image_size == 4096
    assert lease1.extras["five"] == "5"
    assert lease1.extras["six"] == "6"
        
    assert lease2.start.requested == DateTime(2006, 11, 25, 15, 15)
    assert lease2.deadline == None
    assert isinstance(lease2.software, DiskImageSoftwareEnvironment)
    assert lease2.software.image_id == "annotation2.img"
    assert lease2.software.image_size == 4096
    assert lease2.extras["seven"] == "7"
    assert lease2.extras["eight"] == "8"   
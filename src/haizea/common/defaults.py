import os.path


CONFIG_LOCATIONS = ["/etc/haizea.conf",
                            os.path.expanduser("~/.haizea/haizea.conf")]

PERSISTENCE_LOCATION = "~/.haizea/leases.dat"

DAEMON_PIDFILE = "/var/tmp/haizea.pid"

RPC_SERVER = "localhost"
RPC_PORT = 42493
RPC_URI = "http://%s:%i" % (RPC_SERVER, RPC_PORT)

OPENNEBULA_RPC_PORT = 2633
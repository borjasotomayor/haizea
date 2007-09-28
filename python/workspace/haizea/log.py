import logging

log = logging.getLogger("haizea")

handler = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

loglevel = {"CRITICAL": 50,
            "ERROR": 40,
            "WARNING": 30,
            "INFO": 20,
            "SCHED": 15,
            "DEBUG": 10,
            "DEBUGSQL": 9,
            "TRACE": 5,
            "NOTSET": 0}

def info(msg, comp, time):
    if time == None:
        time = "                      "
    log.info("[%s] %s %s" % (time,comp.ljust(7),msg))
    
def debug(msg, comp, time):
    log.debug("[%s] %s %s" % (time,comp.ljust(7),msg))
    
def warning(msg, comp, time):
    log.warning("[%s] %s %s" % (time,comp.ljust(7),msg))    
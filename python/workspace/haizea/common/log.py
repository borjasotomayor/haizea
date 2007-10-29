import logging

log = logging.getLogger("haizea")

handler = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

loglevel = {"CRITICAL": 50,
            "ERROR": 40,
            "WARNING": 30,
            "STATUS": 25,
            "INFO": 20,
            "DEBUG": 10,
            "SQL": 9,
            "EXTREMEDEBUG": 5,
            "NOTSET": 0}

def info(msg, comp, time):
    if time == None:
        time = "                      "
    log.info("[%s] %s %s" % (time,comp.ljust(7),msg))
    
def debug(msg, comp, time):
    if time == None:
        time = "                      "
    log.debug("[%s] %s %s" % (time,comp.ljust(7),msg))

def edebug(msg, comp, time):
    if time == None:
        time = "                      "
    log.log(loglevel["EXTREMEDEBUG"],"[%s] %s %s" % (time,comp.ljust(7),msg))

def status(msg, comp, time):
    if time == None:
        time = "                      "
    log.log(loglevel["STATUS"],"[%s] %s %s" % (time,comp.ljust(7),msg))
    
def warning(msg, comp, time):
    if time == None:
        time = "                      "
    log.warning("[%s] %s %s" % (time,comp.ljust(7),msg))    
    
def error(msg, comp, time):
    if time == None:
        time = "                      "
    log.error("[%s] %s %s" % (time,comp.ljust(7),msg))        
import logging

class Logger(object):
    def __init__(self, rm):
        self.rm = rm

        self.logger = logging.getLogger("haizea")
        self.extremedebug = False
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.loglevel = {"CRITICAL": 50,
                    "ERROR": 40,
                    "WARNING": 30,
                    "STATUS": 25,
                    "INFO": 20,
                    "DEBUG": 10,
                    "EXTREMEDEBUG": 5,
                    "NOTSET": 0}
        
        level = self.rm.config.getLogLevel()
        self.logger.setLevel(self.loglevel[level])
        self.extremedebug = (level == "EXTREMEDEBUG")

    def info(self, msg, comp):
        self.logger.info("[%s] %s %s" % (self.rm.clock.getTime(),comp.ljust(7),msg))
        
    def debug(self, msg, comp):
        self.logger.debug("[%s] %s %s" % (self.rm.clock.getTime(),comp.ljust(7),msg))
    
    def edebug(self, msg, comp):
        # Since there is such a huge amount of edebug messages, we use the
        # extremedebug variable to decide if we call the log function
        # (this actually saves quite a bit of cycles spent in logging functions
        # that ultimately determine that the message doesn't have to printed)
        if self.extremedebug:
            self.logger.log(self.loglevel["EXTREMEDEBUG"],"[%s] %s %s" % (self.rm.clock.getTime(),comp.ljust(7),msg))
    
    def status(self, msg, comp):
        self.logger.log(self.loglevel["STATUS"],"[%s] %s %s" % (self.rm.clock.getTime(),comp.ljust(7),msg))
        
    def warning(self, msg, comp):
        self.logger.warning("[%s] %s %s" % (self.rm.clock.getTime(),comp.ljust(7),msg))    
        
    def error(self, msg, comp):
        self.logger.error("[%s] %s %s" % (self.rm.clock.getTime(),comp.ljust(7),msg))        
        
    def log(self, level, msg, comp):
        if level != "EXTREMEDEBUG" or self.extremedebug:
            self.logger.log(self.loglevel[level], "[%s] %s %s" % (self.rm.clock.getTime(),comp.ljust(7),msg))        
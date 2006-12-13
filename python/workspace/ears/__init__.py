import logging

srvlog = logging.getLogger("server")
reslog = logging.getLogger("reservations")

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
srvlog.addHandler(handler)

loglevel = {"CRITICAL": 50,
            "ERROR": 40,
            "WARNING": 30,
            "INFO": 20,
            "SCHED": 15,
            "DEBUG": 10,
            "DEBUGSQL": 9,
            "TRACE": 5,
            "NOTSET": 0}

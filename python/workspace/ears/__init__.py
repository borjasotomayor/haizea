import logging

srvlog = logging.getLogger("server")
reslog = logging.getLogger("reservations")

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
srvlog.addHandler(handler)
srvlog.setLevel(logging.WARN)

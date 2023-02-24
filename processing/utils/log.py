import logging
from configs import LOG_DIR

def logfile(logname):
    logging.basicConfig(filename="%s/%s" % (LOG_DIR, logname),
                        format='%(levelname)s %(asctime)s :: %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    return log

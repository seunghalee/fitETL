import os
import logging

PROJ_DIR = "/home/ubuntu/fitETL/"


def get_logfile_name(file_name, logdir=PROJ_DIR+"logs/"):
    base_name = os.path.basename(file_name)
    log_fname = os.path.splitext(base_name)[0]+".log"
    return logdir+log_fname


def setup_logger(file_name, name):
    LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s \n"
    logging.basicConfig(filename=get_logfile_name(file_name),
                        level=logging.DEBUG,
                        filemode='w',
                        format=LOG_FORMAT)
    logger = logging.getLogger(name)
    return logger

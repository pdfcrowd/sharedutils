import logging

logger = logging.getLogger()

def setup(logger_name):
    global logger
    logger = logging.getLogger(logger_name)

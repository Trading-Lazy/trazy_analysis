""" wrapper around logging module """
import logging
import os


def get_root_logger(logger_name, filename=None, stdout=True):
    """ get the logger object """
    logger = logging.getLogger(logger_name)
    debug = os.environ.get("ENV", "development") == "development"
    logger.setLevel(logging.CRITICAL if debug else logging.CRITICAL)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    if stdout:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if filename:
        fh = logging.FileHandler(filename)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def get_child_logger(root_logger, name):
    return logging.getLogger(".".join([root_logger, name]))

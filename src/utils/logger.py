import logging
from logging.config import dictConfig

DEBUG = logging.DEBUG
INFO = logging.INFO
WARN = logging.WARN
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL


def configure():
    """Configure Logger

    :param level: Unused, defaults to INFO
    :type level: int, optional
    """
    dictConfig(
        {
            "version": 1,
            "formatters": {"f": {"format": "%(asctime)s %(levelname)-8s %(message)s"}},
            "handlers": {
                "h": {
                    "class": "logging.StreamHandler",
                    "formatter": "f",
                    "level": INFO,
                },
                "fH": {
                    "class": "logging.FileHandler",
                    "filename": "/tmp/dictionary.log",
                    "formatter": "f",
                    "level": DEBUG,
                },
            },
            "root": {
                "handlers": ["fH", "h"],
                "level": DEBUG,
            },
        }
    )


def debug(*args, **kwargs):
    """Logs debug entry"""
    logging.debug(*args, **kwargs)


def info(*args, **kwargs):
    """Info debug entry"""
    logging.info(*args, **kwargs)


def warning(*args, **kwargs):
    """Warning debug entry"""
    logging.warning(*args, **kwargs)


def error(*args, **kwargs):
    """Error debug entry"""
    logging.error(*args, **kwargs)


configure()

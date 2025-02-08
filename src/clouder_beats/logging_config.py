import logging
import logging.config
import logging.handlers
import os
import sys
from datetime import datetime

from src.clouder_beats.config import settings


def setup_logging():
    """Sets up logging configuration."""

    os.makedirs("logs", exist_ok=True)

    log_filename = os.path.join("logs", f"{datetime.now().strftime('%Y-%m-%d')}.log")
    log_level = settings.log_level

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "handlers": {
                "default": {
                    "level": log_level,
                    "formatter": "standard",
                    "class": "logging.StreamHandler",
                    "stream": sys.stdout,
                },
                "logger_file": {
                    "level": log_level,
                    "formatter": "standard",
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": log_filename,
                    "maxBytes": 10_000_000,
                    "backupCount": 5,
                },
            },
            "loggers": {
                "main": {
                    "handlers": ["default", "logger_file"],
                    "level": log_level,
                    "propagate": False,
                },
                "bp": {
                    "handlers": ["default", "logger_file"],
                    "level": log_level,
                    "propagate": False,
                },
                "sp": {
                    "handlers": ["default", "logger_file"],
                    "level": log_level,
                    "propagate": False,
                },
                "mongo": {
                    "handlers": ["default", "logger_file"],
                    "level": log_level,
                    "propagate": False,
                },
                "collectors": {
                    "handlers": ["default", "logger_file"],
                    "level": log_level,
                    "propagate": False,
                },
            },
        }
    )

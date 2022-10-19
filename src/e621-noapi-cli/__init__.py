import logging
import logging.config
import logging.handlers

logging.config.dictConfig(
    config={
        "version": 1,
        "formatters": {
            "stamped": {"format": "[%(asctime)s] [%(levelname)s] %(message)s"},
            "plain": {"format": "%(message)s"},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "plain",
                "stream": "ext://sys.stdout",
                "level": "INFO",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "stamped",
                "filename": "./e621-noapi-cli.log",
                "maxBytes": 2000000,
                "level": "NOTSET",
            },
        },
        "root": {"level": "NOTSET", "handlers": ["file", "console"]},
        "e621-noapi-cli": {
            "handlers": ["file", "console"],
            "propagate": "no",
        },
    }
)
# See: https://github.com/camptocamp/pytest-odoo/issues/15
logging.getLogger("PIL").setLevel(logging.WARNING)

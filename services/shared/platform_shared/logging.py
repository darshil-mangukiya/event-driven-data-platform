from __future__ import annotations

import logging
import sys

from pythonjsonlogger import jsonlogger


def configure_logging(service_name: str, level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s %(service)s %(trace_id)s %(tenant_id)s"
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())

    logging.LoggerAdapter(logging.getLogger(service_name), {"service": service_name})


def get_logger(name: str, service_name: str) -> logging.LoggerAdapter:
    logger = logging.getLogger(name)
    return logging.LoggerAdapter(logger, {"service": service_name, "trace_id": "-", "tenant_id": "-"})


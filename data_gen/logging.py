"""Structured logging configuration for data-gen."""

import logging
import sys
from typing import Any


def setup_logging(
    level: str = "INFO",
    format_type: str = "standard",
) -> None:
    """Configure logging for data-gen.

    Parameters
    ----------
    level : str
        Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    format_type : str
        Format type: "standard" or "json".
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    if format_type == "json":
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Set specific loggers
    logging.getLogger("data_gen").setLevel(log_level)

    # Reduce noise from external libraries
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
    logging.getLogger("psycopg").setLevel(logging.WARNING)


class JsonFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        import json
        from datetime import datetime, timezone

        log_data: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields if present
        if hasattr(record, "extra"):
            log_data.update(record.extra)

        return json.dumps(log_data)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name.

    Parameters
    ----------
    name : str
        Logger name (usually __name__).

    Returns
    -------
    logging.Logger
        Configured logger.
    """
    return logging.getLogger(name)

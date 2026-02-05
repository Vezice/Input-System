"""
iBot v2 Logging Utility

Provides structured logging for Cloud Functions environment.
Compatible with Google Cloud Logging for production.
"""

import logging
import os
import sys
from typing import Any, Dict, Optional

# Check if running in Cloud Functions
IS_CLOUD_FUNCTION = os.getenv("K_SERVICE") is not None or os.getenv("FUNCTION_NAME") is not None

# Log level from environment
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


class StructuredFormatter(logging.Formatter):
    """
    Formatter that outputs JSON for Cloud Logging.
    Falls back to standard format for local development.
    """

    def __init__(self, use_json: bool = True):
        super().__init__()
        self.use_json = use_json

    def format(self, record: logging.LogRecord) -> str:
        if self.use_json:
            return self._format_json(record)
        return self._format_standard(record)

    def _format_json(self, record: logging.LogRecord) -> str:
        """Format as JSON for Cloud Logging."""
        import json

        log_entry = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)

    def _format_standard(self, record: logging.LogRecord) -> str:
        """Format for local development."""
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")

        # Build extra fields string
        extra_str = ""
        if hasattr(record, "extra_fields") and record.extra_fields:
            extra_parts = [f"{k}={v}" for k, v in record.extra_fields.items()]
            extra_str = " | " + ", ".join(extra_parts)

        base_msg = f"{timestamp} [{record.levelname}] {record.module}.{record.funcName}:{record.lineno} - {record.getMessage()}{extra_str}"

        # Add exception if present
        if record.exc_info:
            base_msg += "\n" + self.formatException(record.exc_info)

        return base_msg


class StructuredLogger(logging.Logger):
    """Logger with support for structured fields."""

    def _log_with_extra(
        self,
        level: int,
        msg: str,
        args: tuple,
        exc_info: Any = None,
        extra: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Log with extra structured fields."""
        if extra is None:
            extra = {}

        # Combine extra dict with kwargs
        extra_fields = {**extra, **kwargs}

        # Create a new extra dict with our fields
        full_extra = {"extra_fields": extra_fields}

        super()._log(level, msg, args, exc_info=exc_info, extra=full_extra)

    def info(self, msg: str, *args, **kwargs) -> None:
        """Log info with optional extra fields."""
        extra = kwargs.pop("extra", None)
        self._log_with_extra(logging.INFO, msg, args, extra=extra, **kwargs)

    def debug(self, msg: str, *args, **kwargs) -> None:
        """Log debug with optional extra fields."""
        extra = kwargs.pop("extra", None)
        self._log_with_extra(logging.DEBUG, msg, args, extra=extra, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        """Log warning with optional extra fields."""
        extra = kwargs.pop("extra", None)
        self._log_with_extra(logging.WARNING, msg, args, extra=extra, **kwargs)

    def error(self, msg: str, *args, exc_info: bool = False, **kwargs) -> None:
        """Log error with optional extra fields."""
        extra = kwargs.pop("extra", None)
        self._log_with_extra(logging.ERROR, msg, args, exc_info=exc_info, extra=extra, **kwargs)

    def critical(self, msg: str, *args, exc_info: bool = True, **kwargs) -> None:
        """Log critical with optional extra fields."""
        extra = kwargs.pop("extra", None)
        self._log_with_extra(logging.CRITICAL, msg, args, exc_info=exc_info, extra=extra, **kwargs)


# Set custom logger class
logging.setLoggerClass(StructuredLogger)

# Cache for loggers
_loggers: Dict[str, StructuredLogger] = {}


def get_logger(name: str) -> StructuredLogger:
    """
    Get or create a structured logger.

    Usage:
        logger = get_logger(__name__)
        logger.info("Processing file", filename="test.xlsx", rows=100)

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured StructuredLogger instance
    """
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Don't add handlers if they already exist (prevents duplicate logs)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

        # Use JSON format in Cloud Functions, standard format locally
        formatter = StructuredFormatter(use_json=IS_CLOUD_FUNCTION)
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    # Prevent propagation to root logger
    logger.propagate = False

    _loggers[name] = logger
    return logger

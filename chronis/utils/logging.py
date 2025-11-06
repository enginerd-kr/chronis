"""Structured logging utilities for Chronis."""

import logging
from typing import Any


def setup_logger(name: str = "scheduler", level: int = logging.INFO) -> logging.Logger:
    """
    Configure structured logger.

    Args:
        name: Logger name
        level: Logging level

    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # Console handler
    handler = logging.StreamHandler()
    handler.setLevel(level)

    # Structured format (time, level, context, message)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(context)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


class ContextLogger:
    """Structured logger wrapper with context information."""

    def __init__(self, logger: logging.Logger, context: dict[str, Any] | None = None) -> None:
        self.logger = logger
        self.context = context or {}

    def _format_context(self, extra_context: dict[str, Any] | None = None) -> str:
        """Format context as string."""
        ctx = {**self.context, **(extra_context or {})}
        return ", ".join(f"{k}={v}" for k, v in ctx.items())

    def info(self, message: str, **extra_context: Any) -> None:
        """Info log."""
        self.logger.info(message, extra={"context": self._format_context(extra_context)})

    def warning(self, message: str, **extra_context: Any) -> None:
        """Warning log."""
        self.logger.warning(message, extra={"context": self._format_context(extra_context)})

    def error(self, message: str, exc_info: bool = False, **extra_context: Any) -> None:
        """Error log."""
        self.logger.error(
            message,
            extra={"context": self._format_context(extra_context)},
            exc_info=exc_info,
        )

    def debug(self, message: str, **extra_context: Any) -> None:
        """Debug log."""
        self.logger.debug(message, extra={"context": self._format_context(extra_context)})

    def with_context(self, **context: Any) -> "ContextLogger":
        """Create logger with additional context."""
        return ContextLogger(self.logger, {**self.context, **context})


# Default logger
_default_logger = setup_logger()

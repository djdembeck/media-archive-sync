"""Logging utilities for media archive synchronization.

This module provides a simple logging setup for the media archive sync library.
"""

import logging


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name.

    Attaches a NullHandler if no handlers are present to prevent
    "No handler found" warnings. The application/CLI should configure
    the actual logging handlers and levels.

    Args:
        name: The name for the logger, typically __name__.

    Returns:
        A Logger instance with a NullHandler if needed.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger

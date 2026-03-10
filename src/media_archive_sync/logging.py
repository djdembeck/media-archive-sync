"""Logging utilities for media archive synchronization.

This module provides a simple logging setup for the media archive sync library.
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name.

    Configures a basic StreamHandler with a standard formatter if no
    handlers are already attached to the logger.

    Args:
        name: The name for the logger, typically __name__.

    Returns:
        A Logger instance configured with a StreamHandler.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

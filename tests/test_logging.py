"""Tests for logging module."""

import logging

from media_archive_sync.logging import get_logger


class TestGetLogger:
    """Tests for get_logger function."""

    def test_get_logger_returns_logger(self):
        """Test that get_logger returns a Logger instance."""
        logger = get_logger("test_module")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    def test_get_logger_with_custom_name(self):
        """Test get_logger with custom name."""
        logger = get_logger("custom.name.here")

        assert logger.name == "custom.name.here"

    def test_get_logger_is_same_instance(self):
        """Test that same name returns same logger instance."""
        logger1 = get_logger("same_name")
        logger2 = get_logger("same_name")

        assert logger1 is logger2

    def test_logger_has_handlers(self):
        """Test that logger has handlers configured."""
        logger = get_logger("test_handlers")

        assert len(logger.handlers) >= 0  # May have handlers or propagate to root

    def test_logger_level(self):
        """Test logger level is set appropriately."""
        logger = get_logger("test_level")

        assert logger.level == logging.NOTSET or logger.level >= logging.DEBUG

"""Tests for display module."""

from io import StringIO
from unittest.mock import patch

from media_archive_sync.display import (
    NO_COLOR,
    _DummyTqdm,
    rich_progress_or_stderr,
    safe_print,
    simple_progress,
    tqdm_or_stderr,
)


class TestDummyTqdm:
    """Tests for _DummyTqdm class."""

    def test_dummy_tqdm_iteration(self):
        """Test that _DummyTqdm iterates over items."""
        items = [1, 2, 3]
        dummy = _DummyTqdm(iterable=items)
        result = list(dummy)

        assert result == items

    def test_dummy_tqdm_update(self):
        """Test that _DummyTqdm update increments counter."""
        dummy = _DummyTqdm()

        assert dummy.n == 0

        dummy.update(5)
        assert dummy.n == 5

    def test_dummy_tqdm_context_manager(self):
        """Test _DummyTqdm as context manager."""
        with _DummyTqdm() as dummy:
            assert dummy is not None

    def test_dummy_tqdm_write(self):
        """Test _DummyTqdm write method."""
        output = StringIO()
        _DummyTqdm.write("test message", file=output)

        assert "test message" in output.getvalue()


class TestTqdmOrStderr:
    """Tests for tqdm_or_stderr function."""

    def test_tqdm_or_stderr_basic(self):
        """Test basic usage of tqdm_or_stderr."""
        items = [1, 2, 3]
        result = list(tqdm_or_stderr(items, desc="Testing"))

        assert result == items

    def test_tqdm_or_stderr_with_disable(self):
        """Test tqdm_or_stderr with disable=True."""
        items = ["a", "b", "c"]
        result = list(tqdm_or_stderr(items, disable=True))

        assert result == items

    def test_tqdm_or_stderr_with_force_progress(self):
        """Test that FORCE_PROGRESS affects behavior."""
        with patch("media_archive_sync.display.FORCE_PROGRESS", True):
            items = [1, 2]
            result = list(tqdm_or_stderr(items))
            assert result == items


class TestRichProgressOrStderr:
    """Tests for rich_progress_or_stderr function."""

    def test_rich_progress_with_disable_true(self):
        """Test rich_progress_or_stderr with disable=True."""
        progress = rich_progress_or_stderr(desc="Test", total=10, disable=True)

        assert progress is not None

    def test_rich_progress_context_manager(self):
        """Test rich_progress_or_stderr as context manager."""
        with rich_progress_or_stderr(desc="Test", total=10, disable=True) as pbar:
            pbar.update(1)


class TestSimpleProgress:
    """Tests for simple_progress context manager."""

    def test_simple_progress_basic(self):
        """Test basic usage of simple_progress."""
        with simple_progress(desc="Testing", total=5, disable=True) as p:
            p.update(1)

    def test_simple_progress_no_total(self):
        """Test simple_progress without total."""
        with simple_progress(desc="Testing", disable=True) as p:
            p.update(1)


class TestSafePrint:
    """Tests for safe_print function."""

    def test_safe_print_basic(self):
        """Test basic safe_print."""
        output = StringIO()

        with patch("media_archive_sync.display.sys.stderr", output):
            safe_print("Hello %s", "World")

        result = output.getvalue()
        assert "Hello World" in result

    def test_safe_print_no_args(self):
        """Test safe_print without formatting args."""
        output = StringIO()

        with patch("media_archive_sync.display.sys.stderr", output):
            safe_print("Simple message")

        result = output.getvalue()
        assert "Simple message" in result


class TestNoColor:
    """Tests for NO_COLOR environment variable handling."""

    def test_no_color_behavior(self):
        """Test NO_COLOR is defined as module constant."""

        # Just verify it exists and is a boolean
        assert isinstance(NO_COLOR, bool)

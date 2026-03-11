"""Extended tests for display module."""

from io import StringIO
from unittest.mock import MagicMock, patch

from media_archive_sync.display import (
    _DummyTqdm,
    _stderr_is_tty,
    _TqdmProgressWrapper,
    rich_progress_or_stderr,
    safe_print,
    simple_progress,
    tqdm_or_stderr,
)


class TestStderrIsTty:
    """Tests for _stderr_is_tty function."""

    def test_stderr_is_tty_true(self):
        """Test when stderr is a TTY."""
        mock_stderr = MagicMock()
        mock_stderr.isatty.return_value = True

        with patch("media_archive_sync.display.sys.stderr", mock_stderr):
            result = _stderr_is_tty()

        assert result is True

    def test_stderr_is_tty_false(self):
        """Test when stderr is not a TTY."""
        mock_stderr = MagicMock()
        mock_stderr.isatty.return_value = False

        with patch("media_archive_sync.display.sys.stderr", mock_stderr):
            result = _stderr_is_tty()

        assert result is False

    def test_stderr_no_isatty(self):
        """Test when stderr doesn't have isatty method."""
        mock_stderr = MagicMock()
        del mock_stderr.isatty

        with patch("media_archive_sync.display.sys.stderr", mock_stderr):
            result = _stderr_is_tty()

        assert result is False


class TestTqdmOrStderrExtended:
    """Extended tests for tqdm_or_stderr function."""

    def test_tqdm_or_stderr_force_progress(self):
        """Test FORCE_PROGRESS flag."""
        with patch("media_archive_sync.display.FORCE_PROGRESS", True):
            with patch("media_archive_sync.display._stderr_is_tty", return_value=False):
                items = [1, 2, 3]
                result = list(tqdm_or_stderr(items, total=3))
                assert result == items

    def test_tqdm_or_stderr_with_custom_file(self):
        """Test with custom file parameter."""
        custom_file = StringIO()
        items = [1, 2]
        result = list(tqdm_or_stderr(items, file=custom_file))
        assert result == items


class TestRichProgressExtended:
    """Extended tests for Rich progress."""

    def test_rich_progress_wrapper_methods(self):
        """Test Rich progress wrapper methods."""
        with patch("media_archive_sync.display.RICH_AVAILABLE", True):
            progress = rich_progress_or_stderr(desc="Test", total=10, disable=True)
            assert progress is not None

    def test_rich_progress_with_rich_available(self):
        """Test when Rich is available."""
        with patch("media_archive_sync.display.RICH_AVAILABLE", True):
            with patch("media_archive_sync.display._stderr_is_tty", return_value=True):
                progress = rich_progress_or_stderr(desc="Test", total=10)
                assert progress is not None


class TestDummyTqdmExtended:
    """Extended tests for _DummyTqdm class."""

    def test_dummy_tqdm_close(self):
        """Test close method."""
        dummy = _DummyTqdm()
        dummy.close()  # Should not raise
        assert True

    def test_dummy_tqdm_refresh(self):
        """Test refresh method."""
        dummy = _DummyTqdm()
        dummy.refresh()  # Should not raise
        assert True

    def test_dummy_tqdm_set_description_str(self):
        """Test set_description_str method."""
        dummy = _DummyTqdm()
        dummy.set_description_str("new description")
        assert dummy.desc == "new description"

    def test_dummy_tqdm_enter_exit(self):
        """Test context manager enter and exit."""
        dummy = _DummyTqdm()

        with dummy as d:
            assert d is dummy

    def test_dummy_tqdm_exit_returns_false(self):
        """Test __exit__ returns False."""
        dummy = _DummyTqdm()
        result = dummy.__exit__(None, None, None)
        assert result is False


class TestTqdmProgressWrapper:
    """Tests for _TqdmProgressWrapper class."""

    def test_wrapper_init(self):
        """Test wrapper initialization."""
        wrapper = _TqdmProgressWrapper(desc="Test", total=10)
        assert wrapper.desc == "Test"
        assert wrapper.total == 10
        assert wrapper.n == 0

    def test_wrapper_context_manager(self):
        """Test wrapper as context manager."""
        wrapper = _TqdmProgressWrapper(desc="Test", total=10, disable=True)

        with wrapper as w:
            assert w is wrapper
            w.update(5)
            assert w.n == 5


class TestSafePrintExtended:
    """Extended tests for safe_print function."""

    def test_safe_print_with_args(self):
        """Test safe_print with format arguments."""
        output = StringIO()

        with patch("media_archive_sync.display.sys.stderr", output):
            safe_print("Value: %d, String: %s", 42, "test")

        result = output.getvalue()
        assert "42" in result
        assert "test" in result

    def test_safe_print_no_args(self):
        """Test safe_print with no format arguments."""
        output = StringIO()

        with patch("media_archive_sync.display.sys.stderr", output):
            safe_print("Test message")

        assert "Test message" in output.getvalue()


class TestSimpleProgressExtended:
    """Extended tests for simple_progress function."""

    def test_simple_progress_with_total(self):
        """Test simple_progress with total."""
        items = [1, 2, 3]
        result = []

        with simple_progress(desc="Test", total=3, disable=True) as pbar:
            for item in items:
                result.append(item)
                pbar.update(1)

        assert result == items

    def test_simple_progress_without_total(self):
        """Test simple_progress without total."""
        items = [1, 2]
        result = []

        with simple_progress(desc="Test", disable=True) as pbar:
            for item in items:
                result.append(item)
                pbar.update(1)

        assert result == items

"""Tests for downloader module."""

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

from media_archive_sync.downloader import (
    DownloadCancelledError,
    DownloadManager,
    download_file,
)


class TestDownloadCancelledError:
    """Tests for DownloadCancelledError handling."""

    def test_exception_is_raised(self):
        """Test that DownloadCancelledError can be raised and caught."""
        stop_event = threading.Event()
        stop_event.set()

        try:
            if stop_event.is_set():
                raise DownloadCancelledError()
        except DownloadCancelledError:
            assert True
            return
        assert False, "DownloadCancelledError should have been caught"


class TestDownloadFileCancellation:
    """Tests for download_file cancellation behavior."""

    @patch("media_archive_sync.downloader.requests.Session")
    def test_download_catches_cancelled_error(self, mock_session_class):
        """Test that download_file catches DownloadCancelledError."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        chunks = [b"chunk1", b"chunk2"]
        mock_response.iter_content.return_value = chunks
        mock_session.get.return_value = mock_response

        stop_event = threading.Event()
        stop_event.set()  # Set stop event to trigger cancellation

        with patch.object(
            mock_response, "iter_content", side_effect=DownloadCancelledError()
        ):
            result = download_file(
                "http://example.com/video.mp4",
                Path("/tmp/video.mp4"),
                stop_event=stop_event,
                session=mock_session,
            )

        assert result == (False, 0)


class TestDownloadManagerStop:
    """Tests for DownloadManager stop functionality."""

    def test_stop_sets_event(self):
        """Test that stop() sets the stop event."""
        manager = DownloadManager()
        assert not manager._stop_event.is_set()

        manager.stop()

        assert manager._stop_event.is_set()

    def test_stop_closes_active_sessions(self):
        """Test that stop() closes active sessions."""
        manager = DownloadManager()
        mock_session = MagicMock()

        with manager._sessions_lock:
            manager._active_sessions.add(mock_session)

        manager.stop()

        mock_session.close.assert_called_once()

    def test_stop_calls_cleanup_partials(self, tmp_path):
        """Test that stop() cleans up partial files."""
        manager = DownloadManager()
        partial_path = tmp_path / "video.mp4.partial"
        partial_path.write_text("test data")

        with manager._partials_lock:
            manager._partials.add(partial_path)

        manager.stop()

        # File should be removed (cleanup_partials is called)
        assert not partial_path.exists()

    def test_stop_handles_session_close_errors(self):
        """Test that stop() handles session close errors gracefully."""
        manager = DownloadManager()
        mock_session = MagicMock()
        mock_session.close.side_effect = Exception("Close failed")

        with manager._sessions_lock:
            manager._active_sessions.add(mock_session)

        # Should not raise
        manager.stop()

        assert manager._stop_event.is_set()


class TestDisableProgressAutoDetection:
    """Tests for disable_progress parameter."""

    def test_quiet_true_disables_progress(self):
        """Test that quiet=True disables progress."""
        from media_archive_sync.config import ArchiveConfig

        config = ArchiveConfig(quiet=True)
        manager = DownloadManager(config)

        with patch("media_archive_sync.downloader.download_files") as mock:
            manager.download_batch([])

        call_kwargs = mock.call_args[1]
        assert call_kwargs["disable_progress"] is True

    def test_quiet_false_allows_auto_detection(self):
        """Test that quiet=False passes None for auto-detection."""
        from media_archive_sync.config import ArchiveConfig

        config = ArchiveConfig(quiet=False)
        manager = DownloadManager(config)

        with patch("media_archive_sync.downloader.download_files") as mock:
            manager.download_batch([])

        call_kwargs = mock.call_args[1]
        assert call_kwargs["disable_progress"] is None

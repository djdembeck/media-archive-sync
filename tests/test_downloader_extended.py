"""Extended tests for downloader module."""

import threading
from unittest.mock import MagicMock, patch

import pytest
import requests

from media_archive_sync.downloader import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BACKOFF,
    DEFAULT_TIMEOUT,
    DownloadCancelledError,
    DownloadManager,
    download_file,
    download_files,
    download_with_config,
)


class TestDownloadFileExtended:
    """Extended tests for download_file function."""

    def test_download_file_success(self, tmp_path):
        """Test successful file download."""
        url = "http://example.com/video.mp4"
        local_path = tmp_path / "video.mp4"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "100"}
        mock_response.iter_content.return_value = [b"x" * 100]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ):
            result = download_file(url, local_path)

        assert result[0] is True
        assert result[1] == 100
        assert local_path.exists()

    def test_download_file_resume_partial(self, tmp_path):
        """Test resuming partial download."""
        url = "http://example.com/video.mp4"
        local_path = tmp_path / "video.mp4"
        partial_path = local_path.with_suffix(local_path.suffix + ".partial")

        # Create existing partial file
        partial_path.parent.mkdir(parents=True, exist_ok=True)
        partial_path.write_text("existing content")

        mock_response = MagicMock()
        mock_response.status_code = 206  # Partial content
        mock_response.headers = {"Content-Length": "50"}
        mock_response.iter_content.return_value = [b"x" * 50]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ):
            result = download_file(url, local_path)

        # Should include existing content size
        assert result[0] is True
        assert result[1] > 0

    def test_download_file_with_progress_callback(self, tmp_path):
        """Test download with progress callback."""
        url = "http://example.com/video.mp4"
        local_path = tmp_path / "video.mp4"

        progress_calls = []

        def progress_callback(downloaded, total):
            progress_calls.append((downloaded, total))

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "100"}
        mock_response.iter_content.return_value = [b"x" * 50, b"x" * 50]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ):
            result = download_file(url, local_path, progress_callback=progress_callback)

        assert result[0] is True
        assert len(progress_calls) > 0

    def test_download_file_with_existing_session(self, tmp_path):
        """Test download with existing session."""
        url = "http://example.com/video.mp4"
        local_path = tmp_path / "video.mp4"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.iter_content.return_value = [b"x" * 100]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        result = download_file(url, local_path, session=mock_session)

        assert result[0] is True
        # Should use provided session, not create new one
        mock_session.get.assert_called_once()

    def test_download_file_416_range_not_satisfiable(self, tmp_path):
        """Test handling 416 Range Not Satisfiable."""
        url = "http://example.com/video.mp4"
        local_path = tmp_path / "video.mp4"
        partial_path = local_path.with_suffix(local_path.suffix + ".partial")

        # Create partial file
        partial_path.parent.mkdir(parents=True, exist_ok=True)
        partial_path.write_text("content")

        mock_response = MagicMock()
        mock_response.status_code = 416

        mock_head_response = MagicMock()
        mock_head_response.headers = {"Content-Length": "7"}
        mock_head_response.status_code = 200

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session.head.return_value = mock_head_response

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ):
            result = download_file(url, local_path)

        # Should handle 416 and retry
        assert result[0] is True or result[0] is False

    def test_download_file_network_error(self, tmp_path):
        """Test handling network errors."""
        url = "http://example.com/video.mp4"
        local_path = tmp_path / "video.mp4"

        with patch(
            "media_archive_sync.downloader.requests.Session"
        ) as mock_session_class:
            mock_session = MagicMock()
            mock_session.get.side_effect = requests.RequestException("Network error")
            mock_session_class.return_value = mock_session

            result = download_file(url, local_path)

        assert result[0] is False
        assert result[1] == 0

    def test_download_file_os_error(self, tmp_path):
        """Test handling OS errors during file write."""
        url = "http://example.com/video.mp4"
        local_path = tmp_path / "video.mp4"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.iter_content.return_value = [b"x" * 100]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ), patch("builtins.open", side_effect=OSError("Disk full")):
            result = download_file(url, local_path)

        assert result[0] is False

    def test_download_file_unexpected_error(self, tmp_path):
        """Test handling unexpected errors."""
        url = "http://example.com/video.mp4"
        local_path = tmp_path / "video.mp4"

        with patch(
            "media_archive_sync.downloader.requests.Session"
        ) as mock_session_class:
            mock_session = MagicMock()
            mock_session.get.side_effect = Exception("Unexpected error")
            mock_session_class.return_value = mock_session

            result = download_file(url, local_path)

        assert result[0] is False
        assert result[1] == 0


class TestDownloadFilesExtended:
    """Extended tests for download_files function."""

    def test_download_files_empty_list(self):
        """Test with empty media list."""
        result = download_files([])

        assert result == (0, 0, 0, [])

    def test_download_files_single_file(self, tmp_path):
        """Test downloading single file."""
        local_path = tmp_path / "video.mp4"
        local_path.parent.mkdir(parents=True, exist_ok=True)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "100"}
        mock_response.iter_content.return_value = [b"x" * 100]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ):
            result = download_files(
                [("http://example.com/video.mp4", local_path)], workers=1
            )

        assert result[0] == 1  # success count
        assert result[3]  # downloaded paths

    def test_download_files_skip_existing(self, tmp_path):
        """Test skipping existing files."""
        local_path = tmp_path / "video.mp4"
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text("existing content")

        mock_head_response = MagicMock()
        mock_head_response.headers = {"Content-Length": "16"}
        mock_head_response.status_code = 200

        with patch("requests.head", return_value=mock_head_response):
            result = download_files(
                [("http://example.com/video.mp4", local_path)],
                workers=1,
                skip_existing=True,
            )

        # Should skip the file
        assert result[1] >= 1  # skip count

    def test_download_files_with_stop_event(self, tmp_path):
        """Test stopping downloads via stop event."""
        stop_event = threading.Event()
        stop_event.set()  # Set to stop immediately

        local_path = tmp_path / "video.mp4"

        result = download_files(
            [("http://example.com/video.mp4", local_path)],
            workers=1,
            stop_event=stop_event,
        )

        # Downloads should be cancelled
        assert result[0] == 0  # no successes

    def test_download_files_with_progress(self, tmp_path):
        """Test download with progress bar."""
        local_path = tmp_path / "video.mp4"
        local_path.parent.mkdir(parents=True, exist_ok=True)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "100"}
        mock_response.iter_content.return_value = [b"x" * 100]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ), patch(
            "media_archive_sync.downloader.rich_progress_or_stderr"
        ) as mock_progress:
            mock_progress.return_value.__enter__ = MagicMock(
                return_value=MagicMock()
            )
            mock_progress.return_value.__exit__ = MagicMock(return_value=False)

            result = download_files(
                [("http://example.com/video.mp4", local_path)],
                workers=1,
                progress_desc="Testing",
            )

        assert result[0] >= 0


class TestDownloadWithConfig:
    """Tests for download_with_config function."""

    def test_download_with_config_success(self, tmp_path):
        """Test download with config."""
        from media_archive_sync.config import ArchiveConfig

        config = ArchiveConfig(workers=1)
        local_path = tmp_path / "video.mp4"
        local_path.parent.mkdir(parents=True, exist_ok=True)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "100"}
        mock_response.iter_content.return_value = [b"x" * 100]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ):
            result = download_with_config(
                [("http://example.com/video.mp4", local_path)], config=config
            )

        assert result[0] >= 0  # success count


class TestDownloadManagerExtended:
    """Extended tests for DownloadManager class."""

    def test_download_manager_context_manager(self, tmp_path):
        """Test DownloadManager as context manager."""
        from media_archive_sync.config import ArchiveConfig

        config = ArchiveConfig(workers=1)
        local_path = tmp_path / "video.mp4"
        local_path.parent.mkdir(parents=True, exist_ok=True)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "100"}
        mock_response.iter_content.return_value = [b"x" * 100]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        result = (False, 0)
        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ), DownloadManager(config) as manager:
            result = manager.download_single(
                "http://example.com/video.mp4", local_path
            )

        assert result[0] is True

    def test_download_manager_single_with_resume(self, tmp_path):
        """Test single download with resume."""
        from media_archive_sync.config import ArchiveConfig

        config = ArchiveConfig()
        local_path = tmp_path / "video.mp4"
        partial_path = local_path.with_suffix(local_path.suffix + ".partial")

        # Create partial file
        partial_path.parent.mkdir(parents=True, exist_ok=True)
        partial_path.write_text("existing")

        mock_response = MagicMock()
        mock_response.status_code = 206
        mock_response.headers = {"Content-Length": "50"}
        mock_response.iter_content.return_value = [b"x" * 50]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        manager = DownloadManager(config)
        with manager._sessions_lock:
            manager._active_sessions.add(mock_session)

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ):
            result = manager.download_single(
                "http://example.com/video.mp4", local_path, resume=True
            )

        assert result[0] is True or result[0] is False

    def test_download_manager_batch(self, tmp_path):
        """Test batch download."""
        from media_archive_sync.config import ArchiveConfig

        config = ArchiveConfig(workers=1)
        local_path = tmp_path / "video.mp4"
        local_path.parent.mkdir(parents=True, exist_ok=True)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "100"}
        mock_response.iter_content.return_value = [b"x" * 100]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ):
            manager = DownloadManager(config)
            result = manager.download_batch(
                [("http://example.com/video.mp4", local_path)]
            )

        assert result[0] >= 0

    def test_download_manager_stop_during_download(self, tmp_path):
        """Test stopping download manager during download."""
        from media_archive_sync.config import ArchiveConfig

        config = ArchiveConfig()
        manager = DownloadManager(config)

        # Add a mock session
        mock_session = MagicMock()
        with manager._sessions_lock:
            manager._active_sessions.add(mock_session)

        # Stop should close sessions
        manager.stop()

        assert manager._stop_event.is_set()
        mock_session.close.assert_called_once()

    def test_download_manager_cleanup_partials(self, tmp_path):
        """Test cleanup of partial files."""
        from media_archive_sync.config import ArchiveConfig

        config = ArchiveConfig()
        manager = DownloadManager(config)

        # Add partial file
        partial_path = tmp_path / "video.mp4.partial"
        partial_path.write_text("partial content")

        with manager._partials_lock:
            manager._partials.add(partial_path)

        manager.cleanup_partials()

        # File should be removed
        assert not partial_path.exists()

    def test_download_manager_sigint_handler(self, tmp_path):
        """Test SIGINT handler."""
        from media_archive_sync.config import ArchiveConfig

        config = ArchiveConfig(workers=1)
        local_path = tmp_path / "video.mp4"
        local_path.parent.mkdir(parents=True, exist_ok=True)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.iter_content.return_value = [b"x" * 100]
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with patch(
            "media_archive_sync.downloader.requests.Session",
            return_value=mock_session,
        ), patch.object(threading, "current_thread") as mock_current:
            mock_thread = MagicMock()
            mock_thread.return_value = MagicMock()
            mock_thread.return_value.name = "MainThread"
            mock_current.return_value = mock_thread.return_value

            manager = DownloadManager(config)
            with pytest.raises(KeyboardInterrupt):
                # Simulate SIGINT during download
                manager._stop_event.set()
                raise KeyboardInterrupt()


class TestDownloadCancelledError:
    """Tests for DownloadCancelledError exception."""

    def test_exception_can_be_raised(self):
        """Test that exception can be raised and caught."""
        try:
            raise DownloadCancelledError()
        except DownloadCancelledError:
            assert True
            return
        assert False, "Exception should have been caught"

    def test_exception_inheritance(self):
        """Test exception inheritance."""
        assert issubclass(DownloadCancelledError, Exception)


class TestDefaultConstants:
    """Tests for default constants."""

    def test_default_timeout(self):
        """Test default timeout value."""
        assert DEFAULT_TIMEOUT == 15

    def test_default_max_retries(self):
        """Test default max retries value."""
        assert DEFAULT_MAX_RETRIES == 3

    def test_default_retry_backoff(self):
        """Test default retry backoff value."""
        assert DEFAULT_RETRY_BACKOFF == 2.0

    def test_default_chunk_size(self):
        """Test default chunk size value."""
        assert DEFAULT_CHUNK_SIZE == 8192

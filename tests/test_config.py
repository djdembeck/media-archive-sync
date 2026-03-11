"""Tests for config module."""

from pathlib import Path

from media_archive_sync.config import ArchiveConfig


class TestArchiveConfig:
    """Tests for ArchiveConfig class."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ArchiveConfig()

        assert config.workers == 3
        assert config.request_timeout == 15
        assert config.max_retries == 3
        assert config.partial_extension == ".partial"
        assert config.skip_existing is True
        assert config.quiet is False
        assert config.write_nfo is True
        assert config.use_month_folders is True
        assert config.max_depth == 4

    def test_custom_workers(self):
        """Test custom workers configuration."""
        config = ArchiveConfig(workers=10)

        assert config.workers == 10

    def test_custom_request_timeout(self):
        """Test custom request timeout configuration."""
        config = ArchiveConfig(request_timeout=30)

        assert config.request_timeout == 30

    def test_custom_max_retries(self):
        """Test custom max retries configuration."""
        config = ArchiveConfig(max_retries=5)

        assert config.max_retries == 5

    def test_custom_partial_extension(self):
        """Test custom partial extension configuration."""
        config = ArchiveConfig(partial_extension=".tmp")

        assert config.partial_extension == ".tmp"

    def test_disable_skip_existing(self):
        """Test disabling skip existing."""
        config = ArchiveConfig(skip_existing=False)

        assert config.skip_existing is False

    def test_enable_quiet(self):
        """Test enabling quiet mode."""
        config = ArchiveConfig(quiet=True)

        assert config.quiet is True

    def test_disable_nfo(self):
        """Test disabling NFO writing."""
        config = ArchiveConfig(write_nfo=False)

        assert config.write_nfo is False

    def test_custom_video_extensions(self):
        """Test custom video extensions."""
        config = ArchiveConfig(video_extensions={".mp4", ".mkv"})

        assert config.video_extensions == {".mp4", ".mkv"}

    def test_custom_local_root(self):
        """Test custom local root path."""
        config = ArchiveConfig(local_root=Path("/custom/path"))

        assert config.local_root == Path("/custom/path")

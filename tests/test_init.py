"""Tests for __init__ module exports."""

import media_archive_sync


class TestInitExports:
    """Tests that all exports are available."""

    def test_version_available(self):
        """Test __version__ is exported."""
        assert hasattr(media_archive_sync, "__version__")
        assert media_archive_sync.__version__ == "0.1.0"

    def test_config_exported(self):
        """Test ArchiveConfig is exported."""
        assert hasattr(media_archive_sync, "ArchiveConfig")

    def test_crawler_exports(self):
        """Test crawler functions are exported."""
        assert hasattr(media_archive_sync, "crawl_archive")
        assert hasattr(media_archive_sync, "fetch_directory")
        assert hasattr(media_archive_sync, "fetch_html")

    def test_downloader_exports(self):
        """Test downloader functions are exported."""
        assert hasattr(media_archive_sync, "download_file")
        assert hasattr(media_archive_sync, "download_files")
        assert hasattr(media_archive_sync, "download_with_config")
        assert hasattr(media_archive_sync, "DownloadManager")

    def test_merge_exports(self):
        """Test merge functions are exported."""
        assert hasattr(media_archive_sync, "merge_video_parts")
        assert hasattr(media_archive_sync, "detect_video_parts")
        assert hasattr(media_archive_sync, "get_video_duration")

    def test_organizer_exports(self):
        """Test organizer functions are exported."""
        assert hasattr(media_archive_sync, "extract_epoch_from_name")
        assert hasattr(media_archive_sync, "extract_date_from_epoch")
        assert hasattr(media_archive_sync, "load_local_files")
        assert hasattr(media_archive_sync, "load_local_index")
        assert hasattr(media_archive_sync, "organize_files_by_month")
        assert hasattr(media_archive_sync, "get_target_path")

    def test_strings_exports(self):
        """Test strings functions are exported."""
        assert hasattr(media_archive_sync, "urldecode")
        assert hasattr(media_archive_sync, "normalise_string")
        assert hasattr(media_archive_sync, "sanitize_title_for_filename")

    def test_logging_export(self):
        """Test logging function is exported."""
        assert hasattr(media_archive_sync, "get_logger")

    def test_all_list_complete(self):
        """Test __all__ list is complete."""
        expected = [
            "__version__",
            "ArchiveConfig",
            "crawl_archive",
            "fetch_directory",
            "fetch_html",
            "is_file_too_old_for_download",
            "urldecode",
            "normalise_string",
            "normalise_stem",
            "sanitize_title_for_filename",
            "get_logger",
            "extract_epoch_from_name",
            "extract_date_from_epoch",
            "load_local_files",
            "load_local_index",
            "organize_files_by_month",
            "get_target_path",
            "download_file",
            "download_files",
            "download_with_config",
            "DownloadManager",
            "merge_video_parts",
            "detect_video_parts",
            "get_video_duration",
        ]
        assert media_archive_sync.__all__ == expected

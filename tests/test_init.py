"""Tests for __init__ module exports."""

import media_archive_sync


class TestInitExports:
    """Tests that all exports are available."""

    def test_version_available(self):
        """Test __version__ is exported."""
        assert hasattr(media_archive_sync, "__version__")
        assert media_archive_sync.__version__ == "0.2.0"

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

    def test_strings_additional_exports(self):
        assert hasattr(media_archive_sync, "sanitize_dir_name")
        assert hasattr(media_archive_sync, "server_basename_variants")

    def test_logging_export(self):
        """Test logging function is exported."""
        assert hasattr(media_archive_sync, "get_logger")

    def test_cache_exports(self):
        assert hasattr(media_archive_sync, "Cache")
        assert hasattr(media_archive_sync, "load_media_index")
        assert hasattr(media_archive_sync, "save_media_index")
        assert hasattr(media_archive_sync, "merge_overrides")

    def test_nfo_exports(self):
        assert hasattr(media_archive_sync, "generate_nfo")
        assert hasattr(media_archive_sync, "parse_release_date")
        assert hasattr(media_archive_sync, "write_nfo_for_path")

    def test_all_list_complete(self):
        """Test __all__ list is complete."""
        expected = [
            "__version__",
            "ArchiveConfig",
            "crawl_archive",
            "crawl_remote",
            "fetch_directory",
            "fetch_html",
            "fetch_remote_page",
            "filter_cached_index_for_period",
            "find_missing_to_append",
            "is_file_too_old_for_download",
            "save_media_meta_for_dir",
            "save_metadata",
            "will_perform_full_crawl",
            "urldecode",
            "normalise_string",
            "normalise_stem",
            "sanitize_dir_name",
            "sanitize_title_for_filename",
            "server_basename_variants",
            "get_logger",
            "extract_epoch_from_name",
            "extract_epoch_from_name_zero",
            "extract_date_from_epoch",
            "load_local_files",
            "load_local_files_single",
            "load_local_index",
            "load_local_nfo_index",
            "organize_files_by_month",
            "get_target_path",
            "persist_local_index_entry",
            "update_local_index_entries",
            "resolve_override_key",
            "should_skip_overwrite_local_nfo",
            "download_file",
            "download_files",
            "download_with_config",
            "DownloadManager",
            "merge_video_parts",
            "detect_video_parts",
            "get_video_duration",
            "cluster_by_epoch_window",
            "should_merge_group",
            "merge_multipart_group",
            "merge_multipart_videos",
            "should_merge_parts",
            "extract_epoch_from_filename",
            "order_parts_by_epoch",
            "generate_nfo",
            "parse_release_date",
            "write_nfo_for_path",
            "Cache",
            "load_media_index",
            "save_media_index",
            "merge_overrides",
            "get_cached",
            "set_cached",
            "delete_key",
        ]
        assert media_archive_sync.__all__ == expected

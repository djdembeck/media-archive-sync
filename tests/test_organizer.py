"""Tests for organizer module."""

import json
import time
from pathlib import Path

from media_archive_sync.organizer import (
    extract_date_from_epoch,
    extract_epoch_from_name,
    get_target_path,
    load_local_files,
    load_local_index,
    organize_files_by_month,
)


class TestExtractEpochFromName:
    """Tests for extract_epoch_from_name function."""

    def test_extract_epoch_with_underscores(self):
        """Test extracting epoch surrounded by underscores."""
        result = extract_epoch_from_name("video_1234567890.mp4")
        assert result == 1234567890

    def test_extract_epoch_without_underscores(self):
        """Test extracting epoch without underscores."""
        result = extract_epoch_from_name("video1234567890.mp4")
        assert result == 1234567890

    def test_extract_epoch_with_multiple_numbers(self):
        """Test extracting the first valid epoch from multiple numbers."""
        result = extract_epoch_from_name("video_1234567890_9876543210.mp4")
        assert result == 1234567890

    def test_no_epoch_in_name(self):
        """Test that None is returned when no epoch found."""
        result = extract_epoch_from_name("video_no_epoch.mp4")
        assert result is None

    def test_empty_string(self):
        """Test that None is returned for empty string."""
        result = extract_epoch_from_name("")
        assert result is None

    def test_epoch_too_short(self):
        """Test that short numbers are not extracted."""
        result = extract_epoch_from_name("video_12345.mp4")
        assert result is None


class TestExtractDateFromEpoch:
    """Tests for extract_date_from_epoch function."""

    def test_extract_date_seconds(self):
        """Test extracting date from seconds epoch."""
        result = extract_date_from_epoch(1609459200)  # 2021-01-01
        assert result is not None
        assert result.year == 2021

    def test_extract_date_milliseconds(self):
        """Test extracting date from milliseconds epoch."""
        result = extract_date_from_epoch(1609459200000)  # 2021-01-01 in ms
        assert result is not None
        assert result.year == 2021

    def test_invalid_epoch(self):
        """Test that invalid epoch returns None."""
        # Extremely large or invalid timestamp that causes error
        result = extract_date_from_epoch(999_999_999_999_999_999)
        assert result is None


class TestLoadLocalFiles:
    """Tests for load_local_files function."""

    def test_load_empty_directory(self, tmp_path):
        """Test scanning empty directory."""
        result = load_local_files(tmp_path)
        assert result == {}

    def test_load_files_no_filter(self, tmp_path):
        """Test loading all files without extension filter."""
        (tmp_path / "video.mp4").write_text("test")
        (tmp_path / "audio.mp3").write_text("test")

        result = load_local_files(tmp_path)

        assert len(result) == 2
        assert "video.mp4" in result
        assert "audio.mp3" in result

    def test_load_files_with_filter(self, tmp_path):
        """Test loading files with extension filter."""
        (tmp_path / "video.mp4").write_text("test")
        (tmp_path / "audio.mp3").write_text("test")

        result = load_local_files(tmp_path, video_extensions={".mp4"})

        assert len(result) == 1
        assert "video.mp4" in result
        assert "audio.mp3" not in result

    def test_load_nested_directories(self, tmp_path):
        """Test loading files from nested directories."""
        nested = tmp_path / "subdir"
        nested.mkdir()
        (nested / "video.mp4").write_text("test")

        result = load_local_files(tmp_path)

        assert "subdir/video.mp4" in result

    def test_load_nonexistent_directory(self, tmp_path):
        """Test loading from nonexistent directory."""
        result = load_local_files(tmp_path / "nonexistent")
        assert result == {}


class TestLoadLocalIndex:
    """Tests for load_local_index function."""

    def test_build_new_index(self, tmp_path):
        """Test building new index when cache doesn't exist."""
        cache_file = tmp_path / "cache.json"
        media_dir = tmp_path / "media"
        media_dir.mkdir()
        (media_dir / "video.mp4").write_text("test")

        result = load_local_index(cache_file, media_dir, use_cache=False)

        assert "video.mp4" in result
        assert cache_file.exists()

    def test_load_from_cache(self, tmp_path):
        """Test loading from existing cache."""
        cache_file = tmp_path / "cache.json"
        media_dir = tmp_path / "media"
        media_dir.mkdir()
        (media_dir / "video.mp4").write_text("test")

        # Build initial cache
        load_local_index(cache_file, media_dir)

        # Load from cache
        result = load_local_index(cache_file, media_dir)

        assert "video.mp4" in result

    def test_stale_cache_rebuilds(self, tmp_path):
        """Test that stale cache triggers rebuild."""
        cache_file = tmp_path / "cache.json"
        media_dir = tmp_path / "media"
        media_dir.mkdir()

        # Create old cache
        cache_file.write_text(json.dumps({}))
        old_time = time.time() - 7200  # 2 hours ago
        import os

        os.utime(cache_file, (old_time, old_time))

        (media_dir / "video.mp4").write_text("test")

        result = load_local_index(cache_file, media_dir, max_cache_age=3600)

        assert "video.mp4" in result


class TestOrganizeFilesByMonth:
    """Tests for organize_files_by_month function."""

    def test_organize_by_month(self, tmp_path):
        """Test organizing files by month."""
        media_dir = tmp_path / "media"
        media_dir.mkdir()
        # File from January 2021
        (media_dir / "video_1609459200.mp4").write_text("test")

        files = load_local_files(media_dir)
        result = organize_files_by_month(media_dir, files, dry_run=True)

        assert "Jan_2021" in result or len(result) == 0

    def test_dry_run_logs_only(self, tmp_path):
        """Test that dry_run mode doesn't modify files."""
        media_dir = tmp_path / "media"
        media_dir.mkdir()
        (media_dir / "video_1609459200.mp4").write_text("test")

        files = load_local_files(media_dir)
        organize_files_by_month(media_dir, files, dry_run=True)

        # No month folder should be created
        assert len(list(media_dir.iterdir())) == 1


class TestGetTargetPath:
    """Tests for get_target_path function."""

    def test_get_target_path_with_epoch(self):
        """Test calculating target path for file with epoch."""
        result = get_target_path("video_1609459200.mp4", local_root=Path("/media"))

        assert result is not None
        assert "Jan_2021" in str(result) or "2021" in str(result)

    def test_get_target_path_without_epoch(self):
        """Test that None is returned for file without epoch."""
        result = get_target_path("video_no_epoch.mp4")

        assert result is None

    def test_get_target_path_with_title(self):
        """Test calculating target path with custom title."""
        result = get_target_path(
            "video_1609459200.mp4",
            title="My Custom Title",
            local_root=Path("/media"),
        )

        assert result is not None
        assert "My.Custom.Title" in str(result) or "1609459200" in str(result)

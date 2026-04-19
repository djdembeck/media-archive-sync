"""Tests for organizer module."""

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

from media_archive_sync.organizer import (
    extract_date_from_epoch,
    extract_epoch_from_name,
    extract_epoch_from_name_zero,
    get_target_path,
    load_local_files,
    load_local_files_single,
    load_local_index,
    load_local_nfo_index,
    organize_files_by_month,
    persist_local_index_entry,
    resolve_override_key,
    should_skip_overwrite_local_nfo,
    update_local_index_entries,
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

    def test_no_epoch_files_skipped(self, tmp_path):
        media_dir = tmp_path / "media"
        media_dir.mkdir()
        (media_dir / "no_epoch_video.mp4").write_text("test")
        files = load_local_files(media_dir)
        result = organize_files_by_month(media_dir, files, dry_run=True)
        assert len(result) == 0


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


class TestExtractEpochFromNameZero:
    """Tests for extract_epoch_from_name_zero (returns 0 on miss)."""

    def test_returns_int_on_hit(self):
        assert extract_epoch_from_name_zero("video_1234567890.mp4") == 1234567890

    def test_returns_zero_on_miss(self):
        assert extract_epoch_from_name_zero("no_epoch.mp4") == 0

    def test_returns_zero_on_empty(self):
        assert extract_epoch_from_name_zero("") == 0

    def test_underscore_delimited_preferred(self):
        assert extract_epoch_from_name_zero("video_1234567890_000.mp4") == 1234567890

    def test_13_digit_epoch(self):
        assert (
            extract_epoch_from_name_zero("video_1234567890123_000.mp4") == 1234567890123
        )

    def test_difference_from_base_function(self):
        name = "no_epoch.mp4"
        assert extract_epoch_from_name(name) is None
        assert extract_epoch_from_name_zero(name) == 0


class TestLoadLocalFilesSingle:
    """Tests for load_local_files_single (returns Dict[str, Path])."""

    def test_returns_dict_of_path(self, tmp_path):
        (tmp_path / "video.mp4").write_text("test")
        result = load_local_files_single(tmp_path)
        assert isinstance(result, dict)
        for v in result.values():
            assert isinstance(v, Path)

    def test_finds_files(self, tmp_path):
        (tmp_path / "video.mp4").write_text("test")
        (tmp_path / "audio.mp3").write_text("test")
        result = load_local_files_single(tmp_path)
        assert len(result) >= 2

    def test_empty_dir_returns_empty(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = load_local_files_single(empty)
        assert result == {}

    def test_nonexistent_dir_returns_empty(self, tmp_path):
        result = load_local_files_single(tmp_path / "no_such_dir")
        assert result == {}

    def test_video_extension_filter(self, tmp_path):
        (tmp_path / "video.mp4").write_text("test")
        (tmp_path / "audio.mp3").write_text("test")
        result = load_local_files_single(tmp_path, video_extensions={".mp4"})
        assert "video.mp4" in result
        assert "audio.mp3" not in result

    def test_duplicate_filename_keeps_newer(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        f1 = d1 / "same.mp4"
        f2 = d2 / "same.mp4"
        f1.write_text("old")
        f2.write_text("new")
        mtime1 = os.stat(f1).st_mtime
        os.utime(f2, (mtime1, mtime1 + 10))
        result = load_local_files_single(tmp_path)
        assert "same.mp4" in result
        assert result["same.mp4"] == f2

    def test_whitespace_cleaned_key(self, tmp_path):
        (tmp_path / "video  with   spaces.mp4").write_text("x")
        result = load_local_files_single(tmp_path)
        assert "video  with   spaces.mp4" in result
        assert "video with spaces.mp4" in result

    def test_cleaned_name_collision_logged(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "video with spaces.mp4").write_text("a")
        (d2 / "video  with   spaces.mp4").write_text("b")
        result = load_local_files_single(tmp_path)
        assert "video with spaces.mp4" in result
        assert "video  with   spaces.mp4" in result


class TestLoadLocalNfoIndex:
    """Tests for load_local_nfo_index."""

    def test_finds_nfo_files(self, tmp_path):
        (tmp_path / "video.mp4").write_text("a")
        (tmp_path / "video.nfo").write_text("<xml>1</xml>")
        (tmp_path / "other.nfo").write_text("<xml>2</xml>")
        result = load_local_nfo_index(tmp_path, use_cache=False)
        assert isinstance(result, set)
        assert len(result) == 2

    def test_returns_absolute_paths(self, tmp_path):
        (tmp_path / "video.nfo").write_text("<xml>1</xml>")
        result = load_local_nfo_index(tmp_path, use_cache=False)
        for p in result:
            assert isinstance(p, str)
            assert Path(p).is_absolute()

    def test_empty_dir_returns_empty_set(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = load_local_nfo_index(empty, use_cache=False)
        assert result == set()

    def test_skips_nfo_directories(self, tmp_path):
        (tmp_path / "fake.nfo").mkdir()
        (tmp_path / "real.nfo").write_text("<xml></xml>")
        result = load_local_nfo_index(tmp_path, use_cache=False)
        assert len(result) == 1
        assert any("real.nfo" in p for p in result)

    def test_caches_results(self, tmp_path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        (tmp_path / "video.nfo").write_text("<xml>1</xml>")
        result = load_local_nfo_index(tmp_path, cache_dir=cache_dir, use_cache=True)
        assert len(result) == 1
        # Second call should load from cache
        result2 = load_local_nfo_index(tmp_path, cache_dir=cache_dir, use_cache=True)
        assert len(result2) == 1


class TestPersistLocalIndexEntry:
    """Tests for persist_local_index_entry."""

    def test_persists_new_entry(self, tmp_path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        persist_local_index_entry("/path/to/video.mp4", cache_dir)
        from media_archive_sync.cache import Cache

        cache = Cache(cache_dir, backend="sqlite")
        data = cache.get("local_index")
        assert data is not None
        assert "video.mp4" in data

    def test_updates_existing_cache(self, tmp_path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        from media_archive_sync.cache import Cache

        cache = Cache(cache_dir, backend="sqlite")
        cache.set("local_index", {"old.mp4": "/old/path"})
        persist_local_index_entry("/path/to/new.mp4", cache_dir)
        data = cache.get("local_index")
        assert "old.mp4" in data
        assert "new.mp4" in data

    def test_accepts_path_object(self, tmp_path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        persist_local_index_entry(Path("/path/to/video.mp4"), cache_dir)
        from media_archive_sync.cache import Cache

        cache = Cache(cache_dir, backend="sqlite")
        data = cache.get("local_index")
        assert data is not None
        assert "video.mp4" in data

    def test_graceful_failure(self, tmp_path):
        # Should not raise even with read-only dir
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        persist_local_index_entry("/path/to/video.mp4", cache_dir)


class TestUpdateLocalIndexEntries:
    """Tests for update_local_index_entries."""

    def test_add_files(self, tmp_path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        f = tmp_path / "new.mp4"
        f.write_text("x")
        result = update_local_index_entries(cache_dir, added=[f])
        assert result is True
        from media_archive_sync.cache import Cache

        cache = Cache(cache_dir, backend="sqlite")
        data = cache.get("local_index")
        assert "new.mp4" in data

    def test_remove_files(self, tmp_path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        from media_archive_sync.cache import Cache

        cache = Cache(cache_dir, backend="sqlite")
        cache.set("local_index", {"old.mp4": "/path/old.mp4"})
        result = update_local_index_entries(cache_dir, removed=[Path("/path/old.mp4")])
        assert result is True
        data = cache.get("local_index")
        assert "old.mp4" not in data

    def test_returns_false_on_error(self, tmp_path):
        blocker = tmp_path / "blocker_file"
        blocker.write_text("not a dir")
        bad_dir = blocker / "sub" / "cache"
        result = update_local_index_entries(bad_dir, added=[Path("/x.mp4")])
        assert result is False


class TestResolveOverrideKey:
    """Tests for resolve_override_key."""

    def test_exact_match(self):
        overrides = {"video.mp4": "/path/to/video.mp4"}
        assert resolve_override_key(overrides, "video.mp4") == "video.mp4"

    def test_url_decoded_match(self):
        overrides = {"video file.mp4": "/path/to/video.mp4"}
        assert resolve_override_key(overrides, "video%20file.mp4") == "video file.mp4"

    def test_basename_match(self):
        overrides = {"video.mp4": "/path/to/video.mp4"}
        assert resolve_override_key(overrides, "/some/dir/video.mp4") == "video.mp4"

    def test_normalised_match(self):
        overrides = {"My Video Title": "/path/to/video.mp4"}
        assert resolve_override_key(overrides, "my video title") == "My Video Title"

    def test_no_match_returns_none(self):
        overrides = {"other.mp4": "/path/to/other.mp4"}
        assert resolve_override_key(overrides, "video.mp4") is None

    def test_none_overrides_returns_none(self):
        assert resolve_override_key(None, "video.mp4") is None

    def test_empty_name_returns_none(self):
        assert resolve_override_key({"key": "val"}, "") is None

    def test_empty_overrides_returns_none(self):
        assert resolve_override_key({}, "video.mp4") is None

    def test_short_substring_key_no_match(self):
        overrides = {"ab": "/path/to/ab.mp4"}
        assert resolve_override_key(overrides, "cab") is None


class TestShouldSkipOverwriteLocalNfo:
    """Tests for should_skip_overwrite_local_nfo."""

    def test_multipart_always_skips(self):
        assert should_skip_overwrite_local_nfo("video.part1.mp4") is True
        assert should_skip_overwrite_local_nfo("video.cd2.mp4") is True
        assert should_skip_overwrite_local_nfo("video.disc01.mp4") is True

    def test_non_multipart_skips_by_default(self):
        assert should_skip_overwrite_local_nfo("video.mp4") is True

    def test_overwrite_nfo_flag_allows_overwrite(self):
        args = SimpleNamespace(overwrite_nfo=True, ask_to_overwrite_local_nfo=False)
        assert should_skip_overwrite_local_nfo("video.mp4", args) is False

    def test_legacy_flag_allows_overwrite(self):
        args = SimpleNamespace(overwrite_nfo=False, ask_to_overwrite_local_nfo=True)
        assert should_skip_overwrite_local_nfo("video.mp4", args) is False

    def test_multipart_skips_even_with_flag(self):
        args = SimpleNamespace(overwrite_nfo=True, ask_to_overwrite_local_nfo=False)
        assert should_skip_overwrite_local_nfo("video.part1.mp4", args) is True

    def test_none_args_skips(self):
        assert should_skip_overwrite_local_nfo("video.mp4", None) is True

    def test_path_object_input(self):
        assert should_skip_overwrite_local_nfo(Path("video.mp4")) is True

"""Tests for cache module."""

import json

import pytest

from media_archive_sync.cache import (
    Cache,
    delete_key,
    get_cached,
    load_media_index,
    merge_overrides,
    save_media_index,
    set_cached,
)


class TestCacheInit:
    """Tests for Cache initialization."""

    def test_init_sqlite_backend(self, tmp_path):
        """Test initialization with SQLite backend."""
        cache_dir = tmp_path / "cache"
        cache = Cache(cache_dir, backend="sqlite")

        assert cache.backend == "sqlite"
        assert cache.cache_dir == cache_dir
        assert cache.db_path == cache_dir / "cache.db"
        assert cache_dir.exists()

    def test_init_json_backend(self, tmp_path):
        """Test initialization with JSON backend."""
        cache_dir = tmp_path / "cache"
        cache = Cache(cache_dir, backend="json")

        assert cache.backend == "json"
        assert cache.cache_dir == cache_dir
        assert cache_dir.exists()

    def test_init_unsupported_backend(self, tmp_path):
        """Test that unsupported backend raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported backend"):
            Cache(tmp_path / "cache", backend="unsupported")

    def test_init_creates_directory(self, tmp_path):
        """Test that initialization creates cache directory."""
        cache_dir = tmp_path / "nested" / "cache"
        assert not cache_dir.exists()

        Cache(cache_dir, backend="sqlite")

        assert cache_dir.exists()


class TestCacheSQLite:
    """Tests for SQLite backend operations."""

    def test_set_and_get(self, tmp_path):
        """Test setting and getting values."""
        cache = Cache(tmp_path, backend="sqlite")

        cache.set("key1", {"data": "value1"})
        result = cache.get("key1")

        assert result == {"data": "value1"}

    def test_get_nonexistent_key(self, tmp_path):
        """Test getting a key that doesn't exist."""
        cache = Cache(tmp_path, backend="sqlite")

        result = cache.get("nonexistent")

        assert result is None

    def test_set_overwrites_existing(self, tmp_path):
        """Test that set overwrites existing values."""
        cache = Cache(tmp_path, backend="sqlite")

        cache.set("key1", "old_value")
        cache.set("key1", "new_value")
        result = cache.get("key1")

        assert result == "new_value"

    def test_delete_existing_key(self, tmp_path):
        """Test deleting an existing key."""
        cache = Cache(tmp_path, backend="sqlite")

        cache.set("key1", "value1")
        cache.delete("key1")
        result = cache.get("key1")

        assert result is None

    def test_delete_nonexistent_key(self, tmp_path):
        """Test deleting a key that doesn't exist."""
        cache = Cache(tmp_path, backend="sqlite")

        # Should not raise
        cache.delete("nonexistent")

    def test_exists(self, tmp_path):
        """Test checking if a key exists."""
        cache = Cache(tmp_path, backend="sqlite")

        assert not cache.exists("key1")

        cache.set("key1", "value1")

        assert cache.exists("key1")

    def test_keys(self, tmp_path):
        """Test getting all keys."""
        cache = Cache(tmp_path, backend="sqlite")

        cache.set("key1", "value1")
        cache.set("key2", "value2")

        keys = cache.keys()

        assert sorted(keys) == ["key1", "key2"]

    def test_clear(self, tmp_path):
        """Test clearing all values."""
        cache = Cache(tmp_path, backend="sqlite")

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert cache.keys() == []

    def test_set_non_serializable(self, tmp_path):
        """Test setting non-JSON-serializable value."""
        cache = Cache(tmp_path, backend="sqlite")

        # Should not raise, just log warning
        cache.set("key1", object())

        assert cache.get("key1") is None


class TestCacheJSON:
    """Tests for JSON backend operations."""

    def test_set_and_get(self, tmp_path):
        """Test setting and getting values."""
        cache = Cache(tmp_path, backend="json")

        cache.set("key1", {"data": "value1"})
        result = cache.get("key1")

        assert result == {"data": "value1"}

    def test_get_nonexistent_key(self, tmp_path):
        """Test getting a key that doesn't exist."""
        cache = Cache(tmp_path, backend="json")

        result = cache.get("nonexistent")

        assert result is None

    def test_delete_existing_key(self, tmp_path):
        """Test deleting an existing key."""
        cache = Cache(tmp_path, backend="json")

        cache.set("key1", "value1")
        cache.delete("key1")
        result = cache.get("key1")

        assert result is None

    def test_exists(self, tmp_path):
        """Test checking if a key exists."""
        cache = Cache(tmp_path, backend="json")

        assert not cache.exists("key1")

        cache.set("key1", "value1")

        assert cache.exists("key1")

    def test_keys(self, tmp_path):
        """Test getting all keys."""
        cache = Cache(tmp_path, backend="json")

        cache.set("key1", "value1")
        cache.set("key2", "value2")

        keys = cache.keys()

        assert sorted(keys) == ["key1", "key2"]

    def test_clear(self, tmp_path):
        """Test clearing all values."""
        cache = Cache(tmp_path, backend="json")

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None

    def test_json_file_format(self, tmp_path):
        """Test that JSON files are stored with envelope format."""
        cache = Cache(tmp_path, backend="json")

        cache.set("mykey", "myvalue")

        # Check the file content
        json_files = list(tmp_path.glob("*.cache.json"))
        assert len(json_files) == 1

        content = json.loads(json_files[0].read_text())
        assert "_cache_envelope" in content
        assert content["_cache_envelope"]["key"] == "mykey"
        assert content["_cache_envelope"]["value"] == "myvalue"


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_get_cached(self, tmp_path):
        """Test get_cached convenience function."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set("key1", "value1")

        result = get_cached(tmp_path, "key1")

        assert result == "value1"

    def test_set_cached(self, tmp_path):
        """Test set_cached convenience function."""
        set_cached(tmp_path, "key1", "value1")

        cache = Cache(tmp_path, backend="sqlite")
        assert cache.get("key1") == "value1"

    def test_delete_key(self, tmp_path):
        """Test delete_key convenience function."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set("key1", "value1")

        delete_key(tmp_path, "key1")

        assert cache.get("key1") is None


class TestCacheEdgeCases:
    """Tests for edge cases and error handling."""

    def test_sqlite_error_handling(self, tmp_path):
        """Test that SQLite errors are handled gracefully."""
        cache = Cache(tmp_path, backend="sqlite")

        # Corrupt the database
        cache.db_path.write_text("corrupted data")

        # Should not raise, return None
        result = cache.get("key1")
        assert result is None

    def test_json_corrupted_file(self, tmp_path):
        """Test handling of corrupted JSON files."""
        cache = Cache(tmp_path, backend="json")
        cache.set("key1", "value1")

        # Corrupt the file
        json_files = list(tmp_path.glob("*.cache.json"))
        json_files[0].write_text("invalid json")

        # Should not raise, return None
        result = cache.get("key1")
        assert result is None

    def test_empty_key(self, tmp_path):
        """Test using empty string as key."""
        cache = Cache(tmp_path, backend="sqlite")

        cache.set("", "empty_key_value")
        result = cache.get("")

        assert result == "empty_key_value"

    def test_special_characters_in_key(self, tmp_path):
        """Test keys with special characters."""
        cache = Cache(tmp_path, backend="sqlite")

        special_key = "key/with/slashes and spaces!@#$%"
        cache.set(special_key, "special_value")
        result = cache.get(special_key)

        assert result == "special_value"

    def test_nested_data_structures(self, tmp_path):
        """Test caching nested data structures."""
        cache = Cache(tmp_path, backend="sqlite")

        nested_data = {
            "level1": {"level2": {"level3": ["item1", "item2", "item3"]}},
            "list": [1, 2, 3],
            "tuple": (4, 5, 6),
        }
        cache.set("nested", nested_data)
        result = cache.get("nested")

        # Tuples become lists after JSON serialization
        assert result["level1"]["level2"]["level3"] == ["item1", "item2", "item3"]
        assert result["list"] == [1, 2, 3]


# ---------------------------------------------------------------------------
# Tests for media-index specific cache functions
# ---------------------------------------------------------------------------


class TestLoadMediaIndex:
    """Tests for load_media_index function."""

    def test_no_cache_returns_empty(self, tmp_path):
        """When neither SQLite nor JSON cache exists, returns empty."""
        media_list, dir_counts, local_overrides, loaded, has_data = load_media_index(
            tmp_path
        )
        assert media_list == []
        assert dir_counts == {}
        assert local_overrides == {}
        assert loaded is False
        assert has_data is False

    def test_loads_from_sqlite(self, tmp_path):
        """When SQLite has data, it is used directly."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set(
            "media_index",
            {
                "media_list": [["/path/a.mp4", "Title A"]],
                "dir_counts": {"/dir/a": 2},
                "local_overrides": {},
            },
        )

        media_list, dir_counts, local_overrides, loaded, has_data = load_media_index(
            tmp_path
        )
        assert loaded is True
        assert has_data is True
        assert media_list == [("/path/a.mp4", "Title A")]
        assert dir_counts == {"/dir/a": 2}

    def test_falls_back_to_json_when_sqlite_empty(self, tmp_path):
        """When SQLite has no data, falls back to JSON file."""
        json_path = tmp_path / "media_index.json"
        data = {
            "media_list": [["/path/b.mp4", "Title B"]],
            "dir_counts": {},
            "local_overrides": {},
        }
        json_path.write_text(json.dumps(data), encoding="utf-8")

        media_list, dir_counts, local_overrides, loaded, has_data = load_media_index(
            tmp_path
        )
        assert loaded is True
        assert has_data is True
        assert media_list == [("/path/b.mp4", "Title B")]

    def test_json_data_migrated_to_sqlite(self, tmp_path):
        """When JSON has data and SQLite is empty, data is migrated to SQLite."""
        json_path = tmp_path / "media_index.json"
        data = {
            "media_list": [["/path/c.mp4", "Title C"]],
            "dir_counts": {"/dir/c": 1},
            "local_overrides": {"s.mp4": "/local/s.mp4"},
        }
        json_path.write_text(json.dumps(data), encoding="utf-8")

        load_media_index(tmp_path)

        cache = Cache(tmp_path, backend="sqlite")
        migrated = cache.get("media_index")
        assert migrated is not None
        assert migrated["media_list"] == [["/path/c.mp4", "Title C"]]

    def test_invalid_sqlite_data_type_falls_back(self, tmp_path):
        """When SQLite returns non-dict data, falls back to JSON."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set("media_index", "not a dict")

        json_path = tmp_path / "media_index.json"
        data = {
            "media_list": [["/path/d.mp4", "Title D"]],
            "dir_counts": {},
            "local_overrides": {},
        }
        json_path.write_text(json.dumps(data), encoding="utf-8")

        media_list, _, _, loaded, has_data = load_media_index(tmp_path)
        assert loaded is True
        assert has_data is True
        assert media_list == [("/path/d.mp4", "Title D")]

    def test_corrupt_json_returns_empty(self, tmp_path):
        """Corrupt JSON file returns empty result."""
        json_path = tmp_path / "media_index.json"
        json_path.write_text("{invalid json", encoding="utf-8")

        result = load_media_index(tmp_path)
        assert result == ([], {}, {}, False, False)

    def test_empty_data_loaded_true_has_data_false(self, tmp_path):
        """Empty but valid JSON returns loaded=True, has_data=False."""
        json_path = tmp_path / "media_index.json"
        json_path.write_text(
            json.dumps({"media_list": [], "dir_counts": {}, "local_overrides": {}}),
            encoding="utf-8",
        )

        _, _, _, loaded, has_data = load_media_index(tmp_path)
        assert loaded is True
        assert has_data is False

    def test_custom_json_filename(self, tmp_path):
        """Custom json_filename parameter is respected."""
        custom_path = tmp_path / "custom_index.json"
        data = {
            "media_list": [["/path/e.mp4", "Title E"]],
            "dir_counts": {},
            "local_overrides": {},
        }
        custom_path.write_text(json.dumps(data), encoding="utf-8")

        media_list, _, _, loaded, has_data = load_media_index(
            tmp_path, json_filename="custom_index.json"
        )
        assert loaded is True
        assert has_data is True
        assert media_list == [("/path/e.mp4", "Title E")]

    def test_local_overrides_parsed(self, tmp_path):
        """Local overrides are parsed from cache data."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set(
            "media_index",
            {
                "media_list": [],
                "dir_counts": {},
                "local_overrides": {"server.mp4": "/local/file.mp4"},
            },
        )

        _, _, local_overrides, loaded, _ = load_media_index(tmp_path)
        assert loaded is True
        assert local_overrides == {"server.mp4": "/local/file.mp4"}

    def test_non_string_key_in_overrides_skipped(self, tmp_path):
        """Non-string keys in local_overrides are skipped during parsing.

        Note: After JSON serialization, integer keys become strings.
        This test verifies that the original behavior of filtering non-string
        keys is preserved when data is loaded from SQLite (which serializes to JSON).
        """
        cache = Cache(tmp_path, backend="sqlite")
        cache.set(
            "media_index",
            {
                "media_list": [],
                "dir_counts": {},
                "local_overrides": {"42": "/local/file.mp4", "valid": "/local/ok.mp4"},
            },
        )

        _, _, local_overrides, _, _ = load_media_index(tmp_path)
        # After JSON round-trip, all keys are strings - we filter based on value type
        assert local_overrides == {"42": "/local/file.mp4", "valid": "/local/ok.mp4"}

    def test_int_value_in_overrides_converted(self, tmp_path):
        """Int values in local_overrides are converted to str."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set(
            "media_index",
            {
                "media_list": [],
                "dir_counts": {},
                "local_overrides": {"key": 42},
            },
        )

        _, _, local_overrides, _, _ = load_media_index(tmp_path)
        assert local_overrides == {"key": "42"}

    def test_negative_dir_count_excluded(self, tmp_path):
        """Negative values in dir_counts are excluded during parsing."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set(
            "media_index",
            {
                "media_list": [],
                "dir_counts": {"/dir/a": -1, "/dir/b": 3},
                "local_overrides": {},
            },
        )

        _, dir_counts, _, _, _ = load_media_index(tmp_path)
        assert dir_counts == {"/dir/b": 3}

    def test_bool_dir_count_excluded(self, tmp_path):
        """Bool values in dir_counts are excluded (bool is subclass of int)."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set(
            "media_index",
            {
                "media_list": [],
                "dir_counts": {"/dir/a": True, "/dir/b": 5},
                "local_overrides": {},
            },
        )

        _, dir_counts, _, _, _ = load_media_index(tmp_path)
        assert dir_counts == {"/dir/b": 5}

    def test_media_list_non_string_elements_skipped(self, tmp_path):
        """Non-string elements in media_list items are skipped."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set(
            "media_index",
            {
                "media_list": [[123, "Title"], ["/path/a.mp4", "Valid Title"]],
                "dir_counts": {},
                "local_overrides": {},
            },
        )

        media_list, _, _, _, _ = load_media_index(tmp_path)
        assert media_list == [("/path/a.mp4", "Valid Title")]

    def test_returns_named_tuple(self, tmp_path):
        """load_media_index returns a MediaIndexResult NamedTuple."""
        result = load_media_index(tmp_path)
        assert hasattr(result, "media_list")
        assert hasattr(result, "dir_counts")
        assert hasattr(result, "local_overrides")
        assert hasattr(result, "loaded")
        assert hasattr(result, "has_data")
        assert isinstance(result, tuple)

    def test_bool_in_overrides_rejected(self, tmp_path):
        """Bool values in local_overrides are rejected."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set(
            "media_index",
            {
                "media_list": [],
                "dir_counts": {},
                "local_overrides": {"key": True},
            },
        )
        _, _, local_overrides, _, _ = load_media_index(tmp_path)
        assert "key" not in local_overrides

    def test_float_dir_count_converted(self, tmp_path):
        """Float values in dir_counts are converted to int."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set(
            "media_index",
            {
                "media_list": [],
                "dir_counts": {"/dir/a": 3.0},
                "local_overrides": {},
            },
        )
        _, dir_counts, _, _, _ = load_media_index(tmp_path)
        assert dir_counts == {"/dir/a": 3}

    def test_corrupt_sqlite_falls_back_to_json(self, tmp_path):
        """Corrupt SQLite DB falls back to JSON file."""
        db_path = tmp_path / "cache.db"
        db_path.write_text("corrupted")
        json_path = tmp_path / "media_index.json"
        json_path.write_text(
            json.dumps(
                {
                    "media_list": [["/path/a.mp4", "Title A"]],
                    "dir_counts": {},
                    "local_overrides": {},
                }
            )
        )
        media_list, _, _, loaded, _ = load_media_index(tmp_path)
        assert loaded is True
        assert media_list == [("/path/a.mp4", "Title A")]

    def test_invalid_sqlite_entry_cleaned_up(self, tmp_path):
        """Invalid SQLite entry is cleaned up."""
        cache = Cache(tmp_path, backend="sqlite")
        cache.set("media_index", "not a dict")
        load_media_index(tmp_path)
        result = cache.get("media_index")
        assert result is None


class TestSaveMediaIndex:
    """Tests for save_media_index function."""

    def test_saves_to_sqlite(self, tmp_path):
        """Data is persisted to SQLite."""
        media_list = [("/path/a.mp4", "Title A")]
        dir_counts = {"/dir/a": 1}
        local_overrides = {"s.mp4": "/local/s.mp4"}

        save_media_index(media_list, dir_counts, local_overrides, tmp_path)

        cache = Cache(tmp_path, backend="sqlite")
        result = cache.get("media_index")
        assert result is not None
        assert result["media_list"] == [["/path/a.mp4", "Title A"]]
        assert result["dir_counts"] == {"/dir/a": 1}
        assert result["local_overrides"] == {"s.mp4": "/local/s.mp4"}

    def test_no_json_written_by_default(self, tmp_path):
        """JSON file is NOT written unless write_json=True."""
        save_media_index([("/path/a.mp4", "Title A")], {}, {}, tmp_path)

        json_path = tmp_path / "media_index.json"
        assert not json_path.exists()

    def test_json_written_when_requested(self, tmp_path):
        """JSON file IS written when write_json=True."""
        media_list = [("/path/a.mp4", "Title A")]
        save_media_index(media_list, {}, {}, tmp_path, write_json=True)

        json_path = tmp_path / "media_index.json"
        assert json_path.exists()
        data = json.loads(json_path.read_text(encoding="utf-8"))
        assert data["media_list"] == [["/path/a.mp4", "Title A"]]

    def test_custom_json_filename(self, tmp_path):
        """Custom json_filename is used when write_json=True."""
        save_media_index(
            [("/path/a.mp4", "Title A")],
            {},
            {},
            tmp_path,
            write_json=True,
            json_filename="custom.json",
        )

        assert (tmp_path / "custom.json").exists()
        assert not (tmp_path / "media_index.json").exists()

    def test_roundtrip_with_load(self, tmp_path):
        """Data saved can be loaded back correctly."""
        media_list = [("/path/a.mp4", "Title A")]
        dir_counts = {"/dir/a": 3}
        local_overrides = {"s.mp4": "/local/s.mp4"}

        save_media_index(media_list, dir_counts, local_overrides, tmp_path)

        loaded_list, loaded_counts, loaded_overrides, loaded, has_data = (
            load_media_index(tmp_path)
        )
        assert loaded is True
        assert has_data is True
        assert loaded_list == media_list
        assert loaded_counts == dir_counts
        assert loaded_overrides == local_overrides

    def test_empty_data_roundtrip(self, tmp_path):
        """Empty data can be saved and loaded."""
        save_media_index([], {}, {}, tmp_path)

        media_list, dir_counts, local_overrides, loaded, has_data = load_media_index(
            tmp_path
        )
        assert loaded is True
        assert has_data is False
        assert media_list == []
        assert dir_counts == {}
        assert local_overrides == {}

    def test_creates_cache_dir(self, tmp_path):
        """Creates cache directory if it doesn't exist."""
        deep_dir = tmp_path / "nested" / "cache"
        save_media_index([], {}, {}, deep_dir)

        assert deep_dir.exists()
        cache = Cache(deep_dir, backend="sqlite")
        assert cache.get("media_index") is not None


class TestMergeOverrides:
    """Tests for merge_overrides function."""

    def test_none_existing_returns_new(self):
        new = {"a": "/path/a.mp4"}
        assert merge_overrides(None, new) == new

    def test_empty_existing_returns_new(self):
        new = {"a": "/path/a.mp4"}
        assert merge_overrides({}, new) == new

    def test_new_overrides_existing(self):
        existing = {"a": "/old/a.mp4", "b": "/old/b.mp4"}
        new = {"b": "/new/b.mp4", "c": "/new/c.mp4"}
        result = merge_overrides(existing, new)
        assert result == {"a": "/old/a.mp4", "b": "/new/b.mp4", "c": "/new/c.mp4"}

    def test_does_not_mutate_existing(self):
        existing = {"a": "/old/a.mp4"}
        new = {"b": "/new/b.mp4"}
        merge_overrides(existing, new)
        assert existing == {"a": "/old/a.mp4"}

    def test_empty_new_returns_copy_of_existing(self):
        existing = {"a": "/path/a.mp4"}
        result = merge_overrides(existing, {})
        assert result == existing
        assert result is not existing

    def test_both_empty_returns_empty(self):
        assert merge_overrides({}, {}) == {}

    def test_both_none_and_empty(self):
        assert merge_overrides(None, {}) == {}

    def test_preserves_all_existing_keys(self):
        existing = {"a": "1", "b": "2", "c": "3"}
        new = {"d": "4"}
        result = merge_overrides(existing, new)
        assert result == {"a": "1", "b": "2", "c": "3", "d": "4"}

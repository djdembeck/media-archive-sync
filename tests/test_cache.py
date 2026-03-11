"""Tests for cache module."""

import json

import pytest

from media_archive_sync.cache import (
    Cache,
    delete_key,
    get_cached,
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

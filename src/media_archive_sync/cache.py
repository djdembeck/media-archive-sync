"""Generic cache module supporting JSON file and SQLite backends.

This module provides a simple caching interface that can use either
JSON files or SQLite as the storage backend.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)


class Cache:
    """Generic cache interface supporting JSON file and SQLite backends.

    This class provides a unified interface for caching data using either
    JSON files or SQLite as the storage backend. The backend is selected
    based on configuration or can be explicitly specified.

    Attributes:
        cache_dir: Directory where cache files are stored.
        backend: The storage backend to use ('json' or 'sqlite').
        db_path: Path to the SQLite database (only used when backend='sqlite').
    """

    def __init__(
        self,
        cache_dir: Path | str,
        backend: str = "sqlite",
        db_name: str = "cache.db",
    ) -> None:
        """Initialize the cache.

        Args:
            cache_dir: Directory where cache files will be stored.
            backend: Storage backend to use ('json' or 'sqlite').
            db_name: Name of the SQLite database file (only for sqlite backend).

        Raises:
            ValueError: If an unsupported backend is specified.
        """
        self.cache_dir = Path(cache_dir)
        self.backend = backend.lower()
        self.db_name = db_name

        if self.backend not in ("json", "sqlite"):
            raise ValueError(f"Unsupported backend: {backend}. Use 'json' or 'sqlite'.")

        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if self.backend == "sqlite":
            self.db_path = self.cache_dir / db_name
            self._init_sqlite()

    def _get_json_path(self, key: str) -> Path:
        """Get the file path for a JSON cache entry using hash-based filename.

        Uses SHA256 hash of the full key to ensure collision-safe filenames
        while storing the original key inside the JSON payload.

        Args:
            key: The cache key.

        Returns:
            Path to the JSON cache file.
        """
        # Use SHA256 hash for collision-safe filename
        key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
        # Create a sanitized prefix for readability (first 32 chars)
        safe_prefix = "".join(c for c in key[:32] if c.isalnum() or c in "_-").rstrip()
        if not safe_prefix:
            safe_prefix = "_"
        return self.cache_dir / f"{safe_prefix}_{key_hash}.cache.json"

    def _get_key_from_json(self, path: Path) -> str | None:
        """Extract the original key from a JSON cache file."""
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "_cache_envelope" in data:
                envelope = data["_cache_envelope"]
                return envelope.get("key") if isinstance(envelope, dict) else None
            if isinstance(data, dict) and "_cache_key" in data:
                return data["_cache_key"]
            return path.stem
        except (OSError, json.JSONDecodeError):
            return None

    def _init_sqlite(self) -> None:
        """Initialize the SQLite database with WAL mode."""
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        try:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS kv (
                    k TEXT PRIMARY KEY,
                    v TEXT
                )
                """)
            conn.commit()
        finally:
            conn.close()

    def get(self, key: str) -> Any | None:
        """Retrieve a value from the cache.

        Args:
            key: The cache key.

        Returns:
            The cached value, or None if not found or on error.
        """
        if self.backend == "sqlite":
            return self._get_sqlite(key)
        return self._get_json(key)

    def _get_sqlite(self, key: str) -> Any | None:
        """Retrieve a value from SQLite.

        Args:
            key: The cache key.

        Returns:
            The cached value, or None if not found or on error.
        """
        try:
            if not self.db_path.is_file():
                return None
            conn = sqlite3.connect(str(self.db_path), timeout=30)
            with contextlib.suppress(sqlite3.Error):
                conn.execute("PRAGMA journal_mode=WAL")
            try:
                cur = conn.cursor()
                cur.execute("SELECT v FROM kv WHERE k=?", (key,))
                row = cur.fetchone()
                if not row:
                    return None
                try:
                    return json.loads(row[0])
                except json.JSONDecodeError:
                    return None
            finally:
                conn.close()
        except sqlite3.Error as exc:
            logger.debug("SQLite get failed for key '%s': %s", key, exc)
            return None

    def _get_json(self, key: str) -> Any | None:
        """Retrieve a value from a JSON file."""
        try:
            path = self._get_json_path(key)
            if not path.is_file():
                return None
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                if "_cache_envelope" in data:
                    envelope = data["_cache_envelope"]
                    if isinstance(envelope, dict) and "value" in envelope:
                        return envelope["value"]
                    return None
                if "_cache_value" in data:
                    return data["_cache_value"]
                return {k: v for k, v in data.items() if k != "_cache_key"}
            return data
        except (OSError, json.JSONDecodeError) as exc:
            logger.debug("JSON get failed for key '%s': %s", key, exc)
            return None

    def set(self, key: str, value: Any) -> None:
        """Store a value in the cache.

        Args:
            key: The cache key.
            value: The value to cache (must be JSON serializable).
        """
        if self.backend == "sqlite":
            self._set_sqlite(key, value)
        else:
            self._set_json(key, value)

    def _set_sqlite(self, key: str, value: Any) -> None:
        """Store a value in SQLite.

        Args:
            key: The cache key.
            value: The value to cache.
        """
        try:
            txt = json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            logger.warning("JSON serialization failed for key '%s': %s", key, exc)
            return

        try:
            conn = sqlite3.connect(str(self.db_path), timeout=30)
            with contextlib.suppress(sqlite3.Error):
                conn.execute("PRAGMA journal_mode=WAL")
            try:
                cur = conn.cursor()
                cur.execute(
                    "INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)",
                    (key, txt),
                )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error as exc:
            logger.warning("SQLite set failed for key '%s': %s", key, exc)

    def _set_json(self, key: str, value: Any) -> None:
        """Store a value in a JSON file."""
        try:
            path = self._get_json_path(key)
            wrapped = {"_cache_envelope": {"key": key, "value": value}}
            # Use unique temp file to avoid clashes between concurrent writers
            temp_fd = None
            temp_path = None
            try:
                temp_fd, temp_name = tempfile.mkstemp(
                    suffix=".tmp", prefix=path.stem + "_", dir=path.parent
                )
                temp_path = Path(temp_name)
                with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                    temp_fd = None  # fd ownership transferred to f
                    json.dump(wrapped, f, indent=2, ensure_ascii=False)
                    f.flush()
                    os.fsync(f.fileno())
                temp_path.replace(path)
            except Exception:
                with contextlib.suppress(OSError):
                    if temp_path is not None and temp_path.exists():
                        temp_path.unlink(missing_ok=True)
                if temp_fd is not None:
                    with contextlib.suppress(OSError):
                        os.close(temp_fd)
                raise
        except (OSError, TypeError, ValueError) as exc:
            logger.warning("JSON set failed for key '%s': %s", key, exc)

    def delete(self, key: str) -> None:
        """Delete a value from the cache.

        Args:
            key: The cache key to delete.
        """
        if self.backend == "sqlite":
            self._delete_sqlite(key)
        else:
            self._delete_json(key)

    def _delete_sqlite(self, key: str) -> None:
        """Delete a value from SQLite.

        Args:
            key: The cache key to delete.
        """
        try:
            if not self.db_path.is_file():
                return
            conn = sqlite3.connect(str(self.db_path), timeout=30)
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM kv WHERE k=?", (key,))
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error as exc:
            logger.debug("SQLite delete failed for key '%s': %s", key, exc)

    def _delete_json(self, key: str) -> None:
        """Delete a value from a JSON file.

        Args:
            key: The cache key to delete.
        """
        try:
            path = self._get_json_path(key)
            if path.is_file():
                path.unlink()
        except OSError as exc:
            logger.debug("JSON delete failed for key '%s': %s", key, exc)

    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache.

        Args:
            key: The cache key to check.

        Returns:
            True if the key exists, False otherwise.
        """
        if self.backend == "sqlite":
            return self._exists_sqlite(key)
        return self._exists_json(key)

    def _exists_sqlite(self, key: str) -> bool:
        """Check if a key exists in SQLite.

        Args:
            key: The cache key to check.

        Returns:
            True if the key exists, False otherwise.
        """
        try:
            if not self.db_path.is_file():
                return False
            conn = sqlite3.connect(str(self.db_path), timeout=30)
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM kv WHERE k=?", (key,))
                row = cur.fetchone()
                return row is not None
            finally:
                conn.close()
        except sqlite3.Error as exc:
            logger.debug("SQLite exists check failed for key '%s': %s", key, exc)
            return False

    def _exists_json(self, key: str) -> bool:
        """Check if a key exists as a JSON file.

        Args:
            key: The cache key to check.

        Returns:
            True if the key exists, False otherwise.
        """
        path = self._get_json_path(key)
        return path.is_file()

    def keys(self) -> list[str]:
        """Get all keys in the cache.

        Returns:
            List of all cache keys.
        """
        if self.backend == "sqlite":
            return self._keys_sqlite()
        return self._keys_json()

    def _keys_sqlite(self) -> list[str]:
        """Get all keys from SQLite.

        Returns:
            List of all cache keys.
        """
        try:
            if not self.db_path.is_file():
                return []
            conn = sqlite3.connect(str(self.db_path), timeout=30)
            try:
                cur = conn.cursor()
                cur.execute("SELECT k FROM kv")
                rows = cur.fetchall()
                return [row[0] for row in rows]
            finally:
                conn.close()
        except sqlite3.Error as exc:
            logger.debug("SQLite keys query failed: %s", exc)
            return []

    def _keys_json(self) -> list[str]:
        """Get all keys from JSON cache files."""
        try:
            keys = []
            for path in self.cache_dir.glob("*.cache.json"):
                key = self._get_key_from_json(path)
                if key:
                    keys.append(key)
            return keys
        except OSError as exc:
            logger.debug("JSON keys query failed: %s", exc)
            return []

    def clear(self) -> None:
        """Clear all values from the cache."""
        if self.backend == "sqlite":
            self._clear_sqlite()
        else:
            self._clear_json()

    def _clear_sqlite(self) -> None:
        """Clear all values from SQLite."""
        try:
            if not self.db_path.is_file():
                return
            conn = sqlite3.connect(str(self.db_path), timeout=30)
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM kv")
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error as exc:
            logger.warning("SQLite clear failed: %s", exc)

    def _clear_json(self) -> None:
        """Clear all values from JSON cache files."""
        try:
            for path in self.cache_dir.glob("*.cache.json"):
                path.unlink()
        except OSError as exc:
            logger.warning("JSON clear failed: %s", exc)


# Convenience functions for simple use cases


def get_cached(cache_dir: Path | str, key: str) -> Any | None:
    """Convenience function to get a value from SQLite cache.

    Args:
        cache_dir: Directory where the SQLite cache is stored.
        key: The cache key.

    Returns:
        The cached value, or None if not found.
    """
    cache = Cache(cache_dir, backend="sqlite")
    return cache.get(key)


def set_cached(cache_dir: Path | str, key: str, value: Any) -> None:
    """Convenience function to set a value in SQLite cache.

    Args:
        cache_dir: Directory where the SQLite cache is stored.
        key: The cache key.
        value: The value to cache.
    """
    cache = Cache(cache_dir, backend="sqlite")
    cache.set(key, value)


def delete_key(cache_dir: Path | str, key: str) -> None:
    """Convenience function to delete a key from SQLite cache.

    Args:
        cache_dir: Directory where the SQLite cache is stored.
        key: The cache key to delete.
    """
    cache = Cache(cache_dir, backend="sqlite")
    cache.delete(key)


# Backward compatibility aliases
get_json = get_cached
set_json = set_cached


# ---------------------------------------------------------------------------
# Media-index specific cache functions
# ---------------------------------------------------------------------------

_MEDIA_INDEX_KEY = "media_index"
_MEDIA_INDEX_JSON = "media_index.json"


class MediaIndexResult(NamedTuple):
    """Result of loading media index from cache."""

    media_list: list[tuple[str, str]]
    dir_counts: dict[str, int]
    local_overrides: dict[str, str]
    loaded: bool
    has_data: bool


def _parse_media_list(raw: Any) -> list[tuple[str, str]]:
    """Parse raw media list data into typed list of (path, title) tuples.

    Validates that each element is a tuple/list with at least 2 string elements.

    Args:
        raw: Raw data from cache (expected to be list or tuple).

    Returns:
        List of (path, title) tuples with both elements as str.
    """
    if not isinstance(raw, list | tuple):
        return []
    result: list[tuple[str, str]] = []
    for item in raw:
        if isinstance(item, list | tuple) and len(item) >= 2:
            p0, p1 = item[0], item[1]
            if isinstance(p0, str) and isinstance(p1, str):
                result.append((p0, p1))
    return result


def _parse_dict_counts(raw: Any) -> dict[str, int]:
    """Parse raw directory counts into typed dictionary.

    Validates that keys are strings and values are non-negative integers.
    Only accepts int >= 0 or float values that are integral (v.is_integer())
    and >= 0. Explicitly excludes bool and str values.

    Args:
        raw: Raw data from cache (expected to be dict).

    Returns:
        Dictionary mapping str directory paths to int file counts.
    """
    if not isinstance(raw, dict):
        return {}
    result: dict[str, int] = {}
    for k, v in raw.items():
        if not isinstance(k, str):
            continue
        if type(v) is int and v >= 0:
            result[k] = v
        elif type(v) is float and v.is_integer() and v >= 0:
            result[k] = int(v)
    return result


def _parse_local_overrides(raw: Any) -> dict[str, str]:
    """Parse raw local overrides into typed dictionary.

    Validates that both keys and values are strings (or convertible to str).

    Args:
        raw: Raw data from cache (expected to be dict).

    Returns:
        Dictionary mapping str server basenames to str local paths.
    """
    if not isinstance(raw, dict):
        return {}
    result: dict[str, str] = {}
    for k, v in raw.items():
        if isinstance(k, str) and (
            isinstance(v, str) or (type(v) is int) or (type(v) is float)
        ):
            result[k] = str(v)
    return result


def load_media_index(
    cache_dir: Path | str,
    *,
    json_filename: str = _MEDIA_INDEX_JSON,
) -> MediaIndexResult:
    """Load media index from SQLite cache with JSON file fallback.

    Attempts to load from SQLite first. If SQLite has no data or returns
    an invalid type, falls back to the JSON file. When JSON data is loaded
    and contains entries, it is migrated to SQLite for future lookups.

    Args:
        cache_dir: Directory where cache files are stored.
        json_filename: Name of the JSON fallback file (default: media_index.json).

    Returns:
        MediaIndexResult containing:
            - media_list: List of (path, title) tuples for remote media files.
            - dir_counts: Dictionary mapping directory paths to file counts.
            - local_overrides: Dictionary of filename overrides.
            - loaded: Whether the load operation succeeded (data was read).
            - has_data: Whether any collection actually contains items.
    """
    cache_dir = Path(cache_dir)

    # --- Try SQLite first ---
    try:
        cache = Cache(cache_dir, backend="sqlite")
        data = cache.get(_MEDIA_INDEX_KEY)
        if data is not None:
            if not isinstance(data, dict):
                logger.debug(
                    "Invalid cache data type in SQLite: %s", type(data).__name__
                )
                with contextlib.suppress(Exception):
                    cache.delete(_MEDIA_INDEX_KEY)
            else:
                media_list = _parse_media_list(data.get("media_list", []))
                dir_counts = _parse_dict_counts(data.get("dir_counts", {}))
                local_overrides = _parse_local_overrides(
                    data.get("local_overrides", {})
                )
                has_data = bool(media_list or dir_counts or local_overrides)
                return MediaIndexResult(
                    media_list, dir_counts, local_overrides, True, has_data
                )
    except (OSError, ValueError, TypeError, sqlite3.Error) as exc:
        logger.debug("Failed to load media index from SQLite: %s", exc)

    # --- Fallback to JSON file ---
    json_path = cache_dir / json_filename
    try:
        if json_path.is_file():
            with json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return MediaIndexResult([], {}, {}, False, False)
            media_list = _parse_media_list(data.get("media_list", []))
            dir_counts = _parse_dict_counts(data.get("dir_counts", {}))
            local_overrides = _parse_local_overrides(data.get("local_overrides", {}))
            has_data = bool(media_list or dir_counts or local_overrides)

            # Migrate to SQLite for future lookups
            if has_data:
                try:
                    cache = Cache(cache_dir, backend="sqlite")
                    cache.set(
                        _MEDIA_INDEX_KEY,
                        {
                            "media_list": media_list,
                            "dir_counts": dir_counts,
                            "local_overrides": local_overrides,
                        },
                    )
                except (OSError, ValueError, TypeError, sqlite3.Error) as exc:
                    logger.debug(
                        "Failed to migrate JSON media index to SQLite: %s", exc
                    )

            return MediaIndexResult(
                media_list, dir_counts, local_overrides, True, has_data
            )
    except (OSError, json.JSONDecodeError, ValueError, TypeError, sqlite3.Error) as exc:
        logger.debug("Failed to load media index from JSON: %s", exc)

    return MediaIndexResult([], {}, {}, False, False)


def save_media_index(
    media_list: list[tuple[str, str]],
    dir_counts: dict[str, int],
    local_overrides: dict[str, str],
    cache_dir: Path | str,
    *,
    json_filename: str = _MEDIA_INDEX_JSON,
    write_json: bool = False,
) -> None:
    """Save media index to SQLite cache and optionally to JSON file.

    Args:
        media_list: List of (path, title) tuples for remote media files.
        dir_counts: Dictionary mapping directory paths to file counts.
        local_overrides: Dictionary of filename overrides.
        cache_dir: Directory where cache files are stored.
        json_filename: Name of the JSON file (default: media_index.json).
        write_json: If True, also write a JSON backup file.

    Returns:
        None
    """
    cache_dir = Path(cache_dir)
    payload = {
        "media_list": media_list,
        "dir_counts": dir_counts,
        "local_overrides": local_overrides,
    }

    try:
        cache = Cache(cache_dir, backend="sqlite")
        cache.set(_MEDIA_INDEX_KEY, payload)
        logger.info("Saved media index cache: %d entries", len(media_list))
    except (OSError, ValueError, TypeError, sqlite3.Error) as exc:
        logger.warning("Failed to save media index to SQLite: %s", exc)

    if write_json:
        try:
            json_path = cache_dir / json_filename
            json_path.parent.mkdir(parents=True, exist_ok=True)
            import tempfile as _tf

            fd, tmp_name = _tf.mkstemp(
                suffix=".tmp", prefix=json_path.stem + "_", dir=json_path.parent
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, ensure_ascii=False)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_name, json_path)
            except Exception:
                with contextlib.suppress(OSError):
                    os.unlink(tmp_name)
                raise
        except (OSError, TypeError, ValueError, sqlite3.Error) as exc:
            logger.debug("Failed to write JSON media index file: %s", exc)


def merge_overrides(
    existing: dict[str, str] | None,
    new: dict[str, str],
) -> dict[str, str]:
    """Merge new overrides into existing overrides dictionary.

    Creates a new dictionary containing all entries from the existing overrides
    (if any) updated with entries from the new overrides. Values in the new
    dictionary take precedence over existing values for duplicate keys.

    Args:
        existing: Existing overrides dictionary, or None if no previous overrides.
        new: New overrides to merge, with values taking precedence.

    Returns:
        Dictionary containing merged overrides (existing values + new values).
    """
    merged = dict(existing or {})
    merged.update(new)
    return merged

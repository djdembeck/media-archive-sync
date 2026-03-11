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
from typing import Any

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

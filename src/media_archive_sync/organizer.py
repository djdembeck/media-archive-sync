"""Local file organization utilities for media archive synchronization.

This module provides utilities for scanning, indexing, and organizing local media files.
Key functions include file system crawling, epoch extraction from filenames, file index
management, and organization by month folders.

Key functions:
    - extract_epoch_from_name: Extract 9-13 digit epoch from filename (returns None on miss)
    - extract_epoch_from_name_zero: Extract epoch, returning 0 on miss (local compat)
    - load_local_files: Scan and index files, returning Dict[str, list[Path]]
    - load_local_files_single: Scan and index files, returning Dict[str, Path]
    - load_local_nfo_index: Build index of NFO sidecar files
    - load_local_index: Cached version of load_local_files with JSON persistence
    - persist_local_index_entry: Persist a single entry to cache
    - update_local_index_entries: Update cache with added/removed files
    - resolve_override_key: Resolve filename against local override mappings
    - should_skip_overwrite_local_nfo: Check if NFO overwrite should be skipped
    - organize_files_by_month: Organize files into month-based folders

Usage notes:
    - All file paths use Path objects with UTF-8 encoding
    - Progress output uses logging for terminal compatibility
"""

import json
import os
import re
import sqlite3
import time
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .cache import Cache
from .logging import get_logger
from .strings import normalise_string, sanitize_title_for_filename

logger = get_logger(__name__)


def extract_epoch_from_name(name: str) -> int | None:
    """Extract a 9-13 digit epoch timestamp from a filename.

    Searches for a numeric pattern in the filename, preferring values
    surrounded by underscores (e.g., video_1234567890.mkv).

    Args:
        name: The filename to search.

    Returns:
        The extracted epoch as an integer, or None if not found.

    Example:
        >>> extract_epoch_from_name("video_1234567890.mkv")
        1234567890
        >>> extract_epoch_from_name("somefile_9999999999.txt")
        9999999999
        >>> extract_epoch_from_name("nofilehere.txt")
        None
    """
    if not name:
        return None

    try:
        # Prefer epoch surrounded by underscores
        m = re.search(r"_(\d{9,13})_", name)
        if m:
            return int(m.group(1))
        # Fallback to any 9-13 digit sequence
        m2 = re.search(r"(\d{9,13})", name)
        if m2:
            return int(m2.group(1))
    except (TypeError, ValueError, re.error) as e:
        logger.debug(
            "extract_epoch_from_name: failed to extract epoch from '%s': %s", name, e
        )

    return None


def extract_epoch_from_name_zero(name: str) -> int:
    """Extract a 9-13 digit epoch timestamp from a filename, returning 0 on miss.

    This is the local-compatible variant of extract_epoch_from_name.
    The library's extract_epoch_from_name returns None on miss; this
    returns 0, matching the original local implementation's contract.

    Args:
        name: The filename to search.

    Returns:
        The extracted epoch as an integer, or 0 if not found.
    """
    result = extract_epoch_from_name(name)
    return result if result is not None else 0


def resolve_override_key(
    local_overrides: dict[str, str] | None, name: str
) -> str | None:
    """Resolve a filename against local override mappings.

    Tries exact match, URL-decoded match, basename match, and
    normalised substring match in order.

    Args:
        local_overrides: Mapping of override keys to values.
        name: The filename to resolve.

    Returns:
        The matching override key, or None if no match found.
    """
    if not local_overrides or not name:
        return None
    try:
        candidates = [
            name,
            urllib.parse.unquote(name),
            os.path.basename(name),
        ]
        for c in candidates:
            if c in local_overrides:
                return c

        n_norm = normalise_string(name)
        n_decoded_norm = normalise_string(urllib.parse.unquote(name))
        n_basename_norm = normalise_string(os.path.basename(name))
        for k in local_overrides:
            try:
                k_norm = normalise_string(k)
                for target in [n_norm, n_decoded_norm, n_basename_norm]:
                    if k_norm == target:
                        return k if isinstance(k, str) else str(k)
                for n_target, k_target in [
                    (n_norm, k_norm),
                    (n_decoded_norm, k_norm),
                    (n_basename_norm, k_norm),
                ]:
                    if len(k_target) >= 4 and (
                        k_target in n_target or n_target in k_target
                    ):
                        return k if isinstance(k, str) else str(k)
            except (TypeError, ValueError) as e:
                logger.debug(
                    "resolve_override_key: normalization failed for key '%s': %s",
                    k,
                    e,
                )
                continue
    except (TypeError, AttributeError, ValueError) as e:
        logger.debug(
            "resolve_override_key: failed to resolve key for '%s': %s", name, e
        )
        return None
    return None


def should_skip_overwrite_local_nfo(
    media_candidate: Path | str, args_obj: Any | None = None
) -> bool:
    """Check whether NFO overwrite should be skipped for the given media file.

    Multipart files (part/cd/disc) always skip overwrite regardless of flags.
    For non-multipart files, returns True (skip) unless either overwrite_nfo
    or ask_to_overwrite_local_nfo is truthy.

    Args:
        media_candidate: Path or string of the media file to check.
        args_obj: Optional args namespace with overwrite_nfo and/or
            ask_to_overwrite_local_nfo attributes.

    Returns:
        True if overwrite should be skipped, False otherwise.
    """
    try:
        p_stem = Path(media_candidate).stem if media_candidate else ""
    except (TypeError, ValueError):
        p_stem = str(media_candidate or "")
    try:
        if re.search(r"(?:[_.-](?:part|cd|disc)0*\d+$)", p_stem, re.IGNORECASE):
            return True
    except (re.error, ValueError):
        pass
    try:
        overwrite = getattr(args_obj, "overwrite_nfo", False)
        legacy = getattr(args_obj, "ask_to_overwrite_local_nfo", False)
        return not (bool(overwrite) or bool(legacy))
    except AttributeError:
        return True


def extract_date_from_epoch(epoch: int) -> datetime | None:
    """Convert an epoch timestamp to a datetime object.

    Handles both seconds and milliseconds since epoch.

    Args:
        epoch: The epoch timestamp (seconds or milliseconds).

    Returns:
        A datetime object in UTC, or None if conversion fails.
    """
    try:
        if epoch > 1_000_000_000_000:
            epoch = epoch // 1000
        return datetime.fromtimestamp(epoch, tz=UTC)
    except (ValueError, OSError, OverflowError) as e:
        logger.debug("extract_date_from_epoch: failed to convert %d: %s", epoch, e)
        return None


def load_local_files(
    local_root: Path,
    video_extensions: set[str] | None = None,
    use_mtime: bool = True,
) -> dict[str, list[Path]]:
    """Scan local directories and build a filename-to-paths mapping.

    Walks the directory tree starting from local_root, indexing all
    files. Optionally filters by video extensions. Duplicate filenames
    from different directories are preserved.

    Args:
        local_root: Base local directory to scan.
        video_extensions: Optional set of extensions to filter by
            (e.g., {'.mp4', '.mkv'}). If None, all files are indexed.
        use_mtime: If True, when duplicate filenames exist, keep only
            the one with the most recent modification time per basename.

    Returns:
        Dictionary mapping filenames to lists of Path objects.
    """
    mapping: dict[str, list[Path]] = {}

    if not local_root.exists():
        logger.warning("Local root does not exist: %s", local_root)
        return mapping

    file_count = 0

    try:
        for dirpath, _dirnames, filenames in os.walk(str(local_root)):
            for fname in filenames:
                try:
                    # Filter by extension if specified
                    if video_extensions:
                        ext = Path(fname).suffix.lower()
                        if ext not in video_extensions:
                            continue

                    p = Path(dirpath) / fname
                    if not p.is_file():
                        continue

                    # Use relative path from local_root as key to preserve
                    # files with same name in different subdirectories
                    try:
                        rel_key = str(p.relative_to(local_root))
                    except ValueError:
                        rel_key = p.name

                    if rel_key in mapping:
                        if use_mtime:
                            try:
                                existing_list = mapping[rel_key]
                                # Keep only the most recent file per relative path
                                if p.stat().st_mtime > existing_list[0].stat().st_mtime:
                                    mapping[rel_key] = [p]
                            except (OSError, PermissionError):
                                mapping[rel_key].append(p)
                        else:
                            mapping[rel_key].append(p)
                    else:
                        mapping[rel_key] = [p]

                    file_count += 1
                    if file_count % 1000 == 0:
                        logger.info(
                            "Indexed %d files so far (scanning %s)",
                            file_count,
                            local_root,
                        )
                except (OSError, PermissionError):
                    continue
    except KeyboardInterrupt:
        logger.info(
            "Local scan interrupted by user; returning partial index (%d files)",
            file_count,
        )
    except (OSError, PermissionError) as e:
        logger.warning("Error scanning local files: %s", e)

    logger.info("Found %d existing local file entries in %s", len(mapping), local_root)
    return mapping


def load_local_files_single(
    local_root: Path,
    video_extensions: set[str] | None = None,
) -> dict[str, Path]:
    """Scan local directories and build a filename-to-path mapping (single path per name).

    Unlike load_local_files which returns Dict[str, list[Path]], this returns
    Dict[str, Path] — one path per filename. When duplicates exist, the file
    with the most recent modification time wins. Also adds cleaned-name aliases
    (collapsed whitespace) as additional keys.

    This matches the local implementation's contract for callers that only
    need a single path per filename.

    Args:
        local_root: Base local directory to scan.
        video_extensions: Optional set of extensions to filter by
            (e.g., {'.mp4', '.mkv'}). If None, all files are indexed.

    Returns:
        Dictionary mapping filenames to single Path objects.
    """
    mapping: dict[str, Path] = {}

    if not local_root.exists():
        logger.warning("Local root does not exist: %s", local_root)
        return mapping

    file_count = 0

    try:
        for dirpath, _dirnames, filenames in os.walk(str(local_root)):
            for fname in filenames:
                try:
                    if video_extensions:
                        ext = Path(fname).suffix.lower()
                        if ext not in video_extensions:
                            continue

                    p = Path(dirpath) / fname
                    if not p.is_file():
                        continue

                    if p.name in mapping:
                        try:
                            prev = mapping[p.name]
                            if p.stat().st_mtime > prev.stat().st_mtime:
                                mapping[p.name] = p
                        except (OSError, PermissionError):
                            mapping[p.name] = p
                    else:
                        mapping[p.name] = p

                    try:
                        cleaned = re.sub(r"\s+", " ", p.name).replace("\n", " ").strip()
                        if cleaned and cleaned != p.name:
                            if cleaned in mapping and mapping[cleaned] != p:
                                # Compare mtime: keep the one with newer modification time
                                try:
                                    existing_path = mapping[cleaned]
                                    current_mtime = p.stat().st_mtime
                                    existing_mtime = existing_path.stat().st_mtime
                                    if current_mtime > existing_mtime:
                                        logger.debug(
                                            "Cleaned-name collision: %r maps to %s, overwriting %s (newer mtime)",
                                            cleaned,
                                            p,
                                            mapping[cleaned],
                                        )
                                        mapping[cleaned] = p
                                    else:
                                        logger.debug(
                                            "Cleaned-name collision: %r maps to %s, keeping %s (newer mtime)",
                                            cleaned,
                                            mapping[cleaned],
                                            p,
                                        )
                                except (OSError, PermissionError):
                                    mapping[cleaned] = p
                            else:
                                mapping[cleaned] = p
                    except (re.error, ValueError):
                        pass

                    file_count += 1
                    if file_count % 1000 == 0:
                        logger.info(
                            "Indexed %d files so far (scanning %s)",
                            file_count,
                            local_root,
                        )
                except (OSError, PermissionError):
                    continue
    except KeyboardInterrupt:
        logger.info(
            "Local scan interrupted by user; returning partial index (%d files)",
            file_count,
        )
    except (OSError, PermissionError) as e:
        logger.warning("Error scanning local files: %s", e)

    logger.info("Found %d existing local files in %s", len(mapping), local_root)
    return mapping


def load_local_nfo_index(
    local_root: Path,
    cache_dir: Path | None = None,
    use_cache: bool = True,
) -> set[str]:
    """Build an index of NFO sidecar files in the local directory.

    Scans for .nfo files and optionally caches the results via the
    library's Cache (SQLite) backend.

    Args:
        local_root: Base local directory to scan.
        cache_dir: Optional cache directory for SQLite backend.
        use_cache: Whether to use/load from cache.

    Returns:
        Set of absolute NFO file paths as strings.
    """
    nfo_paths: set[str] = set()
    cache: Cache | None = None

    if use_cache and cache_dir is not None:
        try:
            cache = Cache(cache_dir, backend="sqlite")
            cached_data = cache.get("local_nfo_index")
            if cached_data and isinstance(cached_data, list):
                nfo_paths = set(cached_data)
                logger.debug("Loaded NFO index from cache: %d entries", len(nfo_paths))
                return nfo_paths
        except Exception as err:
            logger.debug("Failed to load NFO index from cache: %s", err)
            cache = None

    try:
        logger.info("Building NFO index for %s...", local_root)
        for nfo in local_root.rglob("*.nfo"):
            try:
                if nfo.is_file():
                    nfo_paths.add(str(nfo.resolve()))
            except (OSError, PermissionError):
                continue
        logger.info("Built NFO index: %d files", len(nfo_paths))

        if use_cache and cache_dir is not None:
            try:
                if cache is None:
                    cache = Cache(cache_dir, backend="sqlite")
                cache.set("local_nfo_index", list(nfo_paths))
                logger.debug("Cached NFO index (%d entries)", len(nfo_paths))
            except Exception as err:
                logger.debug("Failed to cache NFO index: %s", err)
    except (OSError, PermissionError) as exc:
        logger.warning("Failed to build NFO index: %s", exc)

    return nfo_paths


def persist_local_index_entry(
    local_path: str | Path,
    cache_dir: Path,
) -> None:
    """Persist a single local file entry to the cache.

    Args:
        local_path: Path to the local file to index.
        cache_dir: Cache directory for SQLite backend.
    """
    try:
        cache = Cache(cache_dir, backend="sqlite")
        current = cache.get("local_index") or {}
        if not isinstance(current, dict):
            current = {}
        current[Path(local_path).name] = str(local_path)
        cache.set("local_index", current)
    except Exception as e:
        logger.debug("persist_local_index_entry failed for %s: %s", local_path, e)


def update_local_index_entries(
    cache_dir: Path,
    added: list[Path] | None = None,
    removed: list[Path] | None = None,
) -> bool:
    """Update local index with specific file changes.

    Efficiently updates the cache with only the files that changed,
    rather than doing a full filesystem rescan.

    Args:
        cache_dir: Cache directory for SQLite backend.
        added: List of file paths that were added.
        removed: List of file paths that were removed.

    Returns:
        True if the index was updated successfully, False otherwise.
    """
    try:
        cache = Cache(cache_dir, backend="sqlite")
        current = cache.get("local_index") or {}
        if not isinstance(current, dict):
            current = {}

        if removed:
            for p in removed:
                key = p.name
                if key in current:
                    del current[key]

        if added:
            for p in added:
                if p.exists():
                    current[p.name] = str(p)

        cache.set("local_index", current)
        logger.debug(
            "Updated local index: +%d added, -%d removed",
            len(added or []),
            len(removed or []),
        )
        return True
    except (
        OSError,
        PermissionError,
        AttributeError,
        KeyError,
        TypeError,
        sqlite3.Error,
    ) as e:
        logger.warning("Failed to update local index entries: %s", e)
        return False


def load_local_index(
    cache_file: Path,
    local_root: Path,
    video_extensions: set[str] | None = None,
    use_cache: bool = True,
    max_cache_age: int | None = 3600,
) -> dict[str, list[Path]]:
    """Load or build a cached index of local files.

    Attempts to load from JSON cache first; if unavailable or stale,
    rescans using load_local_files and persists results.

    Args:
        cache_file: Path to the JSON cache file.
        local_root: Base local directory to scan.
        video_extensions: Optional set of extensions to filter by.
        use_cache: Whether to use/load from cache.
        max_cache_age: Maximum cache age in seconds (default: 1 hour).

    Returns:
        Dictionary mapping filenames to lists of Path objects.
    """
    if use_cache and cache_file.exists():
        try:
            cache_stat = cache_file.stat()
            cache_age = time.time() - cache_stat.st_mtime
            if max_cache_age is not None and cache_age > max_cache_age:
                logger.debug(
                    "Cache is stale (age=%.0fs, max=%ds)", cache_age, max_cache_age
                )
            else:
                with cache_file.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                mapping: dict[str, list[Path]] = {}
                for k, v in data.items():
                    if isinstance(v, list):
                        mapping[k] = [Path(p) for p in v]
                    else:
                        # Legacy format: single path
                        mapping[k] = [Path(v)]
                logger.debug("Loaded local file index: %d entries", len(mapping))
                return mapping
        except (OSError, PermissionError, json.JSONDecodeError) as e:
            logger.debug("Failed to load local index cache: %s", e)

    mapping = load_local_files(local_root, video_extensions)

    try:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(
                {k: [str(p) for p in v] for k, v in mapping.items()},
                f,
                indent=2,
                ensure_ascii=False,
            )
        logger.info("Saved local file index to %s", cache_file)
    except (OSError, PermissionError) as e:
        logger.debug("Failed to write local index cache: %s", e)

    return mapping


def organize_files_by_month(
    local_root: Path,
    files: dict[str, list[Path]] | None = None,
    month_format: str = "%b_%Y",
    video_extensions: set[str] | None = None,
    dry_run: bool = False,
) -> dict[str, list[Path]]:
    """Organize files into month-based folders.

    Groups files by the month extracted from their epoch timestamp,
    then returns a mapping of month folders to files.

    Args:
        local_root: Base local directory for organization.
        files: Optional pre-built file mapping. If None, scans local_root.
        month_format: strftime format for month folder names
            (default: "%b_%Y" -> "Jan_2024").
        video_extensions: Optional set of extensions to include.
        dry_run: If True, only log what would be done without moving files.

    Returns:
        Dictionary mapping month folder names to lists of file paths.
    """
    if files is None:
        files = load_local_files(local_root, video_extensions)

    organized: dict[str, list[Path]] = {}

    for filename, filepath_list in files.items():
        for filepath in filepath_list:
            # Extract epoch from filename
            epoch = extract_epoch_from_name(filename)
            if epoch is None:
                logger.debug("No epoch found in filename: %s", filename)
                continue

            # Convert to datetime
            dt = extract_date_from_epoch(epoch)
            if not dt:
                logger.debug(
                    "Could not extract date from epoch %d: %s", epoch, filename
                )
                continue

            # Format month folder name
            month_folder = dt.strftime(month_format)

            if month_folder not in organized:
                organized[month_folder] = []

            organized[month_folder].append(filepath)

    if dry_run:
        logger.info(
            "[DRY-RUN] Would organize %d files into %d month folders:",
            sum(len(v) for v in organized.values()),
            len(organized),
        )
        for month, file_list in sorted(organized.items()):
            logger.info("  %s: %d files", month, len(file_list))
    else:
        logger.info(
            "Organized %d files into %d month folders",
            sum(len(v) for v in organized.values()),
            len(organized),
        )

    return organized


def get_target_path(
    filename: str,
    title: str | None = None,
    local_root: Path = Path("./media"),
    month_format: str = "%b_%Y",
    video_extensions: set[str] | None = None,
) -> Path | None:
    """Calculate the organized target path for a file.

    Given a filename (which should contain an epoch), determine where
    it should be placed within the month-based folder structure.

    Args:
        filename: The original filename (should contain epoch timestamp).
        title: Optional title to use for renaming (if not using original name).
        local_root: Base local directory.
        month_format: strftime format for month folder names.
        video_extensions: Set of valid video extensions for renaming.

    Returns:
        The target Path where the file should be located, or None
        if no epoch can be extracted.
    """
    epoch = extract_epoch_from_name(filename)
    if epoch is None:
        return None

    dt = extract_date_from_epoch(epoch)
    if not dt:
        return None

    month_folder = dt.strftime(month_format)
    target_dir = local_root / month_folder

    # Determine the target filename
    if title:
        ext = Path(filename).suffix.lower()
        if video_extensions and ext not in video_extensions:
            ext = ".mp4"  # Default extension
        sanitized = sanitize_title_for_filename(title)
        target_name = f"{sanitized}_{epoch}{ext}"
    else:
        target_name = filename

    return target_dir / target_name

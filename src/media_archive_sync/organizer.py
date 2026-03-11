"""Local file organization utilities for media archive synchronization.

This module provides utilities for scanning, indexing, and organizing local media files.
Key functions include file system crawling, epoch extraction from filenames, file index
management, and organization by month folders.

Key functions:
    - extract_epoch_from_name: Extract 9-13 digit epoch from filename
    - load_local_files: Scan and index files in local directories
    - load_local_index: Cached version of load_local_files with JSON persistence
    - organize_files_by_month: Organize files into month-based folders

Usage notes:
    - All file paths use Path objects with UTF-8 encoding
    - Progress output uses logging for terminal compatibility
"""

import json
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path

from .logging import get_logger
from .strings import sanitize_title_for_filename

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

                    if p.name in mapping:
                        if use_mtime:
                            try:
                                existing_list = mapping[p.name]
                                # Keep only the most recent file per basename
                                if p.stat().st_mtime > existing_list[0].stat().st_mtime:
                                    mapping[p.name] = [p]
                            except (OSError, PermissionError):
                                mapping[p.name].append(p)
                        else:
                            mapping[p.name].append(p)
                    else:
                        mapping[p.name] = [p]

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
            if not epoch:
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
    if not epoch:
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

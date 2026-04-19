"""Web crawler for remote archive interaction.

This module contains functions for fetching and parsing remote Apache directory
listings, crawling the archive structure, and managing HTTP requests.

Two families of functions are provided:

* **Core functions** (``crawl_archive``, ``fetch_directory``, ``save_metadata``,
  etc.) offer full control over every parameter.
* **Convenience wrappers** (``crawl_remote``, ``fetch_remote_page``,
  ``save_media_meta_for_dir``) supply sensible defaults so callers can get
  started quickly.

Several functions accept a ``match_by`` or ``normalize_keys`` parameter that
switches between the library's strict matching semantics and the more lenient
behaviour used by the legacy local implementation.
"""

import hashlib
import json
import re
import urllib.parse
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from .logging import get_logger
from .strings import urldecode

logger = get_logger(__name__)


def fetch_html(url: str) -> str:
    """Fetch HTML from a URL with timeout.

    Performs a GET request with a 15-second timeout. Returns empty string
    on network errors or non-2xx responses.

    Args:
        url: The URL to GET.

    Returns:
        The response HTML content, or empty string on failure.
    """
    try:
        resp = requests.get(
            url, timeout=15, headers={"User-Agent": "media-archive-sync/1.0"}
        )
        resp.raise_for_status()
        return resp.text
    except (requests.RequestException, TimeoutError, ConnectionError) as exc:
        logger.debug("Failed to fetch %s: %s", url, exc)
        return ""
    except Exception as exc:
        logger.exception("Unexpected error fetching %s: %s", url, exc)
        raise


def crawl_archive(
    start_dir: str | None = None,
    remote_base: str | None = None,
    max_depth: int = 10,
    video_extensions: set | None = None,
    progress_callback: Callable[..., Any] | None = None,
) -> tuple[list[tuple[str, str]], dict[str, int]]:
    """Walk the remote Apache index and collect media entries.

    If `start_dir` is provided the crawl will be seeded from that
    directory only (useful for periodic checks that should inspect a
    single month folder instead of the whole archive).

    This function performs a breadth‑first walk of the remote Apache
    directory listing starting at `remote_base`. It collects every media
    file URL and the decoded filename (percent‑decoded) and also keeps a
    simple per‑directory count of media files for reporting purposes.

    The returned `media_list` is a list of tuples `(full_url, decoded
    filename)` which is used by later steps to find the expected local
    placement of files. `dir_counts` maps the remote directory URL to the
    number of media files discovered there and is printed to the user as
    a quick summary of what's on the server.

    Args:
        start_dir: Starting directory URL (defaults to remote_base)
        remote_base: Base URL of the remote archive
        max_depth: Maximum directory depth to crawl
        video_extensions: Set of video file extensions to look for
        progress_callback: Optional callback function(dir_url, depth) for progress

    Returns:
        Tuple of (media_list, dir_counts) where media_list is a list of
        (url, decoded_name) tuples and dir_counts maps directory URLs
        to file counts.

    Raises:
        ValueError: If neither start_dir nor remote_base is provided.
    """
    if not remote_base and not start_dir:
        raise ValueError("Either remote_base or start_dir must be provided")

    base_url = (start_dir or remote_base).rstrip("/") + "/"
    remote_base_normalized = (remote_base or base_url).rstrip("/") + "/"
    # Use base_url as prefix when start_dir is provided to limit scope
    prefix = base_url if start_dir else remote_base_normalized

    # Default video extensions if none provided
    if video_extensions is None:
        video_extensions = {".mp4", ".mkv", ".avi", ".mov", ".webm"}
    extensions = video_extensions
    ext_pattern = "|".join(re.escape(ext.lstrip(".")) for ext in extensions)

    media_list: list[tuple[str, str]] = []
    dir_counts: dict[str, int] = {}

    queue: deque[str] = deque([base_url])
    visited: set[str] = {base_url}

    while queue:
        dir_url = queue.popleft().rstrip("/") + "/"
        depth = dir_url.rstrip("/").count("/") - base_url.rstrip("/").count("/")

        if progress_callback:
            progress_callback(dir_url, depth)

        if depth > max_depth:
            logger.debug(
                "Depth %d exceeds max_depth (%d) – skipping %s",
                depth,
                max_depth,
                dir_url,
            )
            continue

        logger.info("Crawling (depth=%d): %s", depth, dir_url)
        html = fetch_html(dir_url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")

        # Find subdirectories
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href == "../":
                continue
            if href.endswith("/"):
                sub_url = urllib.parse.urljoin(dir_url, href)
                normalized_url = sub_url.rstrip("/") + "/"
                if normalized_url.startswith(prefix) and normalized_url not in visited:
                    visited.add(normalized_url)
                    queue.append(sub_url)

        # Count and collect media files
        dir_counts[dir_url] = 0
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full_url = urllib.parse.urljoin(dir_url, href)
            parsed_url = urllib.parse.urlparse(full_url)
            parsed_path = parsed_url.path
            if re.search(rf"\.({ext_pattern})$", parsed_path, re.I):
                # Filter to only include URLs under the intended archive root
                # Use prefix (which respects start_dir when provided) for path check
                parsed_base = urllib.parse.urlparse(prefix)
                if parsed_url.netloc != parsed_base.netloc:
                    continue
                # Normalize paths for comparison to handle leading/trailing slashes
                effective_root = parsed_base.path.rstrip("/") + "/"
                if not parsed_url.path.startswith(effective_root):
                    continue
                decoded_name = urldecode(Path(parsed_path).name)
                media_list.append((full_url, decoded_name))
                dir_counts[dir_url] += 1

    return media_list, dir_counts


def fetch_directory(
    dir_url: str, allowed_extensions: set[str] | None = None
) -> list[tuple[str, str]]:
    """Fetch a single remote directory listing.

    Returns a list of (full_url, decoded_basename) tuples for media files only.

    This is a lightweight alternative to crawling the whole archive when
    we only need to check one directory for new files.

    Args:
        dir_url: The URL of the directory to fetch.
        allowed_extensions: Set of allowed file extensions (e.g., {'.mp4', '.mkv'}).
            If None, defaults to common video extensions.

    Returns:
        List of (full_url, decoded_basename) tuples for media files.
    """
    out: list[tuple[str, str]] = []
    if allowed_extensions is None:
        allowed_extensions = {".mp4", ".mkv", ".avi", ".mov", ".webm"}
    extensions = allowed_extensions
    ext_pattern = "|".join(re.escape(ext.lstrip(".")) for ext in extensions)
    try:
        # Normalize dir_url to ensure it ends with a slash for safe urljoin
        normalized_dir_url = dir_url.rstrip("/") + "/"
        parsed_base = urllib.parse.urlparse(normalized_dir_url)
        html = fetch_html(normalized_dir_url)
        if not html:
            return out
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Skip parent directory and self references
            if href in ("../", "./"):
                continue
            if href.endswith("/"):
                continue
            # Resolve the href against the normalized base URL
            full = urllib.parse.urljoin(normalized_dir_url, href)
            parsed_full = urllib.parse.urlparse(full)
            # Validate: must stay within the same host and base path
            if parsed_full.netloc != parsed_base.netloc:
                continue
            base_path = parsed_base.path.rstrip("/") + "/"
            if not parsed_full.path.startswith(base_path):
                continue
            # Filter to only include media files with allowed extensions
            # Parse the path to ignore query strings when checking extensions
            href_path = urllib.parse.urlparse(href).path
            if not re.search(rf"\.({ext_pattern})$", href_path, re.I):
                continue
            dec = urldecode(Path(parsed_full.path).name)
            out.append((full, dec))
    except requests.RequestException as e:
        logger.debug("Failed to fetch directory listing %s: %s", dir_url, e)
    return out


def save_metadata(dir_url: str, media_meta_file: Path) -> None:
    """Fetch HEAD and HTML for a directory and persist metadata.

    Saves ETag, Last-Modified, and html_hash into media_meta_file for
    quick future freshness checks.

    Args:
        dir_url: The directory URL to fetch metadata for.
        media_meta_file: Path to the JSON file for storing metadata.
    """
    try:
        meta = {}
        if media_meta_file.is_file():
            with media_meta_file.open("r", encoding="utf-8") as mf:
                meta = json.load(mf)
        # Validate that loaded meta is a dict; reset if not
        if not isinstance(meta, dict):
            logger.debug("Media meta cache was not a dict, resetting")
            meta = {}
    except (json.JSONDecodeError, TypeError, OSError) as e:
        logger.debug("Failed to load existing media meta: %s", e)
        meta = {}

    headers = {"User-Agent": "media-archive-sync/1.0"}
    try:
        head = requests.head(dir_url, timeout=8, allow_redirects=True, headers=headers)
    except requests.RequestException:
        head = None

    etag = head.headers.get("ETag") if head and head.status_code < 400 else None
    lm = head.headers.get("Last-Modified") if head and head.status_code < 400 else None

    html_hash = None
    try:
        resp = requests.get(dir_url, timeout=10, headers=headers)
        if resp.status_code < 400:
            h = hashlib.sha256(resp.content).hexdigest()
            html_hash = h
    except requests.RequestException:
        html_hash = None

    meta[dir_url] = {
        "etag": etag,
        "last_modified": lm,
        "html_hash": html_hash,
    }

    try:
        media_meta_file.parent.mkdir(parents=True, exist_ok=True)
        with media_meta_file.open("w", encoding="utf-8") as mf:
            json.dump(meta, mf, indent=2, ensure_ascii=False)
        logger.debug("Saved media meta for %s", dir_url)
    except Exception:
        logger.debug("Failed to write media meta file")


def will_perform_full_crawl(
    media_list: list[tuple[str, str]] | None,
    media_list_prepared: bool | None,
) -> bool:
    """Return True when the code path would perform a full crawl.

    The main code checks whether the media list was prepared (from cache
    or a targeted per-month listing) and whether the media_list is empty.
    This helper mirrors that logic so tests can assert the decision
    without invoking network or running the full CLI.

    Args:
        media_list: List of (url, decoded_name) tuples or None.
        media_list_prepared: Boolean indicating if list was prepared from cache.

    Returns:
        True if a full crawl would be performed, False otherwise.
    """
    prepared = bool(media_list_prepared)
    has_media = bool(media_list)
    return (not prepared) and (not has_media)


def filter_cached_index_for_period(
    media_list: list[tuple[str, str]] | None,
    dir_counts: dict[str, int] | None,
    periodic_dir: str | None,
    normalize_keys: bool = True,
) -> tuple[list[tuple[str, str]], dict[str, int], bool]:
    """Return (media_list, dir_counts, prepared) filtered to period.

    Does not perform network I/O; simply filters provided structures
    when scoping cache to `periodic_dir`.

    Args:
        media_list: List of (url, decoded_name) tuples or None.
        dir_counts: Mapping of directory URLs to file counts or None.
        periodic_dir: Directory URL to filter to, or None for no filtering.
        normalize_keys: If True (default), normalize dir_counts keys by
            ensuring trailing slashes before lookup. If False, keys are
            used as-is (matching the legacy local implementation).

    Returns:
        Tuple of (filtered_media_list, filtered_dir_counts, prepared_flag).
        prepared_flag is True if filtering was applied successfully.
        When periodic_dir is not found in dir_counts, returns the original
        media_list, dir_counts, and False (prepared_flag=False), rather than
        raising an exception.
    """
    if not periodic_dir:
        return media_list or [], dict(dir_counts or {}), False
    media_list = media_list or []
    dir_counts = dict(dir_counts or {})

    if normalize_keys:
        normalized_periodic = periodic_dir.rstrip("/") + "/"
        normalized_dir_counts = {k.rstrip("/") + "/": v for k, v in dir_counts.items()}
        if (
            normalized_periodic in normalized_dir_counts
            and normalized_dir_counts[normalized_periodic] > 0
        ):
            filtered = [
                it for it in media_list if it[0].startswith(normalized_periodic)
            ]
            return filtered, {normalized_periodic: len(filtered)}, True
    else:
        if periodic_dir in dir_counts and dir_counts.get(periodic_dir, 0) > 0:
            filtered = [it for it in media_list if it[0].startswith(periodic_dir)]
            return filtered, {periodic_dir: len(filtered)}, True

    return media_list, dir_counts, False


def find_missing_to_append(
    cached_media: list[tuple[str, str]] | None,
    month_items: list[tuple[str, str]] | None,
    match_by: str = "tuple",
) -> list[tuple[str, str]]:
    """Return month_items that are missing from cached_media.

    Items are tuples (full_url, decoded_name). The returned list is in
    the same order as month_items (oldest-first if caller sorted).

    Args:
        cached_media: List of (url, decoded_name) tuples representing
            already cached media, or None.
        month_items: List of (url, decoded_name) tuples from the current
            month directory, or None.
        match_by: How to determine whether an item is "existing".
            ``"tuple"`` (default) matches by the full (url, name) tuple,
            so the same filename at a different URL is considered new.
            ``"name"`` matches by decoded_name only, so any cached entry
            with the same name suppresses the new item regardless of URL.

    Returns:
        List of (url, decoded_name) tuples that are in month_items
        but not in cached_media.
    """
    if match_by == "name":
        existing = {n for _, n in (cached_media or [])}
        to_append: list[tuple[str, str]] = []
        for full, dec in month_items or []:
            if dec not in existing:
                to_append.append((full, dec))
        return to_append
    elif match_by == "tuple":
        existing = set(cached_media or [])
        return [item for item in (month_items or []) if item not in existing]
    else:
        raise ValueError(
            f"Unknown match_by value: {match_by!r}. Expected 'tuple' or 'name'."
        )


def is_file_too_old_for_download(
    url: str,
    decoded_name: str,
    max_age_days: int | None = None,
    allow_old_downloads: bool = False,
    fail_closed: bool = False,
) -> bool:
    """Check if a file is too old to be downloaded based on age limits.

    Extracts timestamp from URL or filename and compares against
    max_age_days. Returns True if file should be skipped.

    Args:
        url: The URL of the remote file to check.
        decoded_name: The decoded filename to extract timestamp from.
        max_age_days: Maximum age in days (None or 0 = no limit, positive
            integer = age limit in days).
        allow_old_downloads: If True, skip the age check entirely.
        fail_closed: If True, return True (skip download) when
            max_age_days or allow_old_downloads are not explicitly
            provided and default to their "no-op" values. This mirrors
            the legacy local implementation's config-sentinel behaviour
            where missing configuration causes downloads to be blocked.

    Returns:
        bool: True if the file should be skipped because it exceeds
            max_age_days, False otherwise.
    """
    if fail_closed and max_age_days is None and not allow_old_downloads:
        return True

    if allow_old_downloads:
        return False

    if max_age_days is None or max_age_days <= 0:
        return False

    try:
        epoch = None
        m = re.search(r"/(\d{9,13})/", url)
        if m:
            epoch = int(m.group(1))
        if epoch is None:
            m = re.search(r"_(\d{9,13})_", decoded_name)
            if m:
                epoch = int(m.group(1))
        if epoch is None:
            m = re.search(r"(\d{9,13})", url)
            if m:
                epoch = int(m.group(1))

        if epoch is None:
            return False

        try:
            if epoch > 10_000_000_000:
                file_date = datetime.fromtimestamp(epoch / 1000, tz=UTC)
            else:
                file_date = datetime.fromtimestamp(epoch, tz=UTC)
        except (OverflowError, OSError) as e:
            logger.debug("Failed to convert epoch %s to datetime: %s", epoch, e)
            return False

        now = datetime.now(UTC)
        age_days = (now - file_date).days

        return age_days > max_age_days

    except (TypeError, ValueError) as e:
        logger.debug("Error checking file age: %s", e)
        return False


def crawl_remote(
    remote_base: str,
    start_dir: str | None = None,
    max_depth: int = 4,
    video_extensions: set[str] | None = None,
    progress_callback: Callable[..., Any] | None = None,
) -> tuple[list[tuple[str, str]], dict[str, int]]:
    """Crawl a remote archive with sensible defaults.

    Convenience wrapper around :func:`crawl_archive` that supplies
    commonly-used defaults (``max_depth=4``, default video extensions).

    Args:
        remote_base: Base URL of the remote archive.
        start_dir: Optional starting directory URL (defaults to remote_base).
        max_depth: Maximum directory depth to crawl (default 4).
        video_extensions: Set of video file extensions to look for.
        progress_callback: Optional callback function(dir_url, depth).

    Returns:
        Tuple of (media_list, dir_counts) — same as :func:`crawl_archive`.
    """
    return crawl_archive(
        start_dir=start_dir,
        remote_base=remote_base,
        max_depth=max_depth,
        video_extensions=video_extensions,
        progress_callback=progress_callback,
    )


def fetch_remote_page(dir_url: str) -> list[tuple[str, str]]:
    """Fetch a single remote directory listing without extension filtering.

    Unlike :func:`fetch_directory`, this function returns **all** non-directory
    entries (not just those with video extensions), matching the behaviour of
    the legacy local ``fetch_dir_listing`` implementation.

    Args:
        dir_url: The URL of the directory to fetch.

    Returns:
        List of (full_url, decoded_basename) tuples for all files.
    """
    out: list[tuple[str, str]] = []
    try:
        normalized_dir_url = dir_url.rstrip("/") + "/"
        parsed_base = urllib.parse.urlparse(normalized_dir_url)
        html = fetch_html(normalized_dir_url)
        if not html:
            return out
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href in ("../", "./"):
                continue
            if href.endswith("/"):
                continue
            full = urllib.parse.urljoin(normalized_dir_url, href)
            parsed_full = urllib.parse.urlparse(full)
            if parsed_full.netloc != parsed_base.netloc:
                continue
            base_path = parsed_base.path.rstrip("/") + "/"
            if not parsed_full.path.startswith(base_path):
                continue
            dec = urldecode(Path(parsed_full.path).name)
            out.append((full, dec))
    except (requests.RequestException, ValueError, OSError) as e:
        logger.debug("Failed to fetch remote page %s: %s", dir_url, e)
    return out


def save_media_meta_for_dir(dir_url: str, media_meta_file: Path) -> None:
    """Fetch HEAD and HTML for a directory and persist metadata.

    Alias for :func:`save_metadata` that matches the legacy local
    naming convention.  See :func:`save_metadata` for full
    documentation.
    """
    save_metadata(dir_url, media_meta_file)

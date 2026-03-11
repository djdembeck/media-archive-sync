"""Web crawler for remote archive interaction.

This module contains functions for fetching and parsing remote Apache directory
listings, crawling the archive structure, and managing HTTP requests.
"""

import hashlib
import json
import re
import urllib.parse
from collections import deque
from datetime import UTC, datetime
from pathlib import Path

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
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.text
    except (requests.RequestException, TimeoutError, ConnectionError, Exception) as exc:
        logger.debug("Failed to fetch %s: %s", url, exc)
        return ""


def crawl_archive(
    start_dir: str | None = None,
    remote_base: str | None = None,
    max_depth: int = 10,
    video_extensions: set | None = None,
    progress_callback: callable | None = None,
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

    # Default video extensions if none provided
    extensions = video_extensions or {".mp4", ".mkv", ".avi", ".mov", ".webm"}
    ext_pattern = "|".join(re.escape(ext.lstrip(".")) for ext in extensions)

    media_list: list[tuple[str, str]] = []
    dir_counts: dict[str, int] = {}

    queue: deque[str] = deque([base_url])
    visited: set[str] = {base_url}

    while queue:
        dir_url = queue.popleft().rstrip("/") + "/"

        if progress_callback:
            depth = dir_url.rstrip("/").count("/") - base_url.rstrip("/").count("/")
            progress_callback(dir_url, depth)

        depth = dir_url.rstrip("/").count("/") - base_url.rstrip("/").count("/")
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
                if (
                    normalized_url.startswith(remote_base_normalized)
                    and normalized_url not in visited
                ):
                    visited.add(normalized_url)
                    queue.append(sub_url)

        # Count and collect media files
        dir_counts[dir_url] = 0
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(rf"\.({ext_pattern})$", href, re.I):
                full_url = urllib.parse.urljoin(dir_url, href)
                decoded_name = urldecode(Path(href).name)
                media_list.append((full_url, decoded_name))
                dir_counts[dir_url] += 1

    return media_list, dir_counts


def fetch_directory(dir_url: str) -> list[tuple[str, str]]:
    """Fetch a single remote directory listing.

    Returns a list of (full_url, decoded_basename) tuples.

    This is a lightweight alternative to crawling the whole archive when
    we only need to check one directory for new files.

    Args:
        dir_url: The URL of the directory to fetch.

    Returns:
        List of (full_url, decoded_basename) tuples.
    """
    out: list[tuple[str, str]] = []
    try:
        html = fetch_html(dir_url)
        if not html:
            return out
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href in ("../", "./"):
                continue
            if href.endswith("/"):
                continue
            full = urllib.parse.urljoin(dir_url, href)
            dec = urldecode(Path(urllib.parse.urlparse(full).path).name)
            out.append((full, dec))
    except (requests.RequestException, Exception) as e:
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
    except (json.JSONDecodeError, TypeError, OSError) as e:
        logger.debug("Failed to load existing media meta: %s", e)
        meta = {}

    try:
        head = requests.head(dir_url, timeout=8, allow_redirects=True)
    except Exception:
        head = None

    etag = head.headers.get("ETag") if head and head.status_code < 400 else None
    lm = head.headers.get("Last-Modified") if head and head.status_code < 400 else None

    html_hash = None
    try:
        resp = requests.get(dir_url, timeout=10)
        if resp is not None and resp.status_code < 400:
            h = hashlib.sha256(resp.content).hexdigest()
            html_hash = h
    except Exception:
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
) -> tuple[list[tuple[str, str]], dict[str, int], bool]:
    """Return (media_list, dir_counts, prepared) filtered to period.

    Does not perform network I/O; simply filters provided structures
    when scoping cache to `periodic_dir`.

    Args:
        media_list: List of (url, decoded_name) tuples or None.
        dir_counts: Mapping of directory URLs to file counts or None.
        periodic_dir: Directory URL to filter to, or None for no filtering.

    Returns:
        Tuple of (filtered_media_list, filtered_dir_counts, prepared_flag).
        prepared_flag is True if filtering was applied successfully.

    Raises:
        ValueError: If periodic_dir is not found in dir_counts.
    """
    if not periodic_dir:
        return media_list or [], dict(dir_counts or {}), False
    media_list = media_list or []
    dir_counts = dict(dir_counts or {})
    normalized_periodic = periodic_dir.rstrip("/") + "/"
    if normalized_periodic in dir_counts:
        filtered = [it for it in media_list if it[0].startswith(normalized_periodic)]
        return filtered, {normalized_periodic: len(filtered)}, True
    return media_list, dir_counts, False


def find_missing_to_append(
    cached_media: list[tuple[str, str]] | None,
    month_items: list[tuple[str, str]] | None,
    periodic_dir: str,
) -> list[tuple[str, str]]:
    """Return month_items that are missing from cached_media.

    Items are tuples (full_url, decoded_name). The returned list is in
    the same order as month_items (oldest-first if caller sorted).

    Args:
        cached_media: List of (url, decoded_name) tuples representing
            already cached media, or None.
        month_items: List of (url, decoded_name) tuples from the current
            month directory, or None.
        periodic_dir: Directory URL being processed (used for context).

    Returns:
        List of (url, decoded_name) tuples that are in month_items
        but not in cached_media.
    """
    existing = set(cached_media or [])
    to_append: list[tuple[str, str]] = []
    for item in month_items or []:
        if item not in existing:
            to_append.append(item)
    return to_append


def is_file_too_old_for_download(
    url: str,
    decoded_name: str,
    max_age_days: int = 0,
    allow_old_downloads: bool = False,
) -> bool:
    """Check if a file is too old to be downloaded based on age limits.

    Extracts timestamp from URL or filename and compares against
    max_age_days. Returns True if file should be skipped.

    Args:
        url: The URL of the remote file to check.
        decoded_name: The decoded filename to extract timestamp from.
        max_age_days: Maximum age in days (0 = no limit).
        allow_old_downloads: If True, skip the age check entirely.

    Returns:
        bool: True if the file should be skipped because it exceeds
            max_age_days, False otherwise.
    """
    # Skip check if old downloads are explicitly allowed
    if allow_old_downloads:
        return False

    if max_age_days <= 0:
        return False

    try:
        # Extract epoch from URL or filename
        epoch = None
        # Try URL first
        m = re.search(r"/(\d{9,13})/", url)
        if m:
            epoch = int(m.group(1))
        # Try filename
        if epoch is None:
            m = re.search(r"_(\d{9,13})_", decoded_name)
            if m:
                epoch = int(m.group(1))
        # Last resort: any 9-13 digit sequence
        if epoch is None:
            m = re.search(r"(\d{9,13})", url)
            if m:
                epoch = int(m.group(1))

        if epoch is None:
            # Can't determine age, allow download
            return False

        # Convert epoch to datetime
        try:
            if epoch > 1_000_000_000_000:  # Milliseconds
                file_date = datetime.fromtimestamp(epoch / 1000, tz=UTC)
            else:
                file_date = datetime.fromtimestamp(epoch, tz=UTC)
        except (OverflowError, OSError) as e:
            logger.debug("Failed to convert epoch %s to datetime: %s", epoch, e)
            return False

        # Calculate age
        now = datetime.now(UTC)
        age_days = (now - file_date).days

        return age_days > max_age_days

    except (TypeError, ValueError) as e:
        logger.debug("Error checking file age: %s", e)
        return False

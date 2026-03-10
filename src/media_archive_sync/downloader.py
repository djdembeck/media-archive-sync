"""Downloader module for media archive synchronization.

This module provides generic file download functionality with support for:
- Resumable downloads (via HTTP Range requests)
- Parallel downloads using thread pools
- Progress tracking with Rich or tqdm fallbacks
- Configurable retry logic with exponential backoff
"""

from __future__ import annotations

import concurrent.futures
import os
import signal
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import requests
from requests.adapters import HTTPAdapter

from .config import ArchiveConfig
from .display import rich_progress_or_stderr, safe_print
from .logging import get_logger

logger = get_logger(__name__)

# Default configuration values
DEFAULT_TIMEOUT = 15
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF = 2.0
DEFAULT_CHUNK_SIZE = 8192


def download_file(
    url: str,
    local_path: Path,
    timeout: int = DEFAULT_TIMEOUT,
    resume: bool = True,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Tuple[bool, int]:
    """Download a single file from URL to local path.

    Supports resumable downloads via HTTP Range requests. Writes to a
    temporary file first, then renames to the target path on success.

    Args:
        url: The URL to download from.
        local_path: The local path to save the file to.
        timeout: Request timeout in seconds.
        resume: If True, attempt to resume partial downloads.
        chunk_size: Size of download chunks in bytes.
        progress_callback: Optional callback(bytes_downloaded, total_bytes).

    Returns:
        Tuple of (success: bool, bytes_downloaded: int).
    """
    temp_path = local_path.with_suffix(local_path.suffix + ".partial")
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    headers: Dict[str, str] = {}
    start_byte = 0

    # Check for existing partial file for resuming
    if resume and temp_path.is_file():
        start_byte = temp_path.stat().st_size
        if start_byte > 0:
            headers["Range"] = f"bytes={start_byte}-"
            logger.debug("Resuming download from byte %d for %s", start_byte, url)

    try:
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=1))

        with session.get(url, stream=True, timeout=timeout, headers=headers) as response:
            if response.status_code == 416:
                # Range unsatisfiable - file already complete or invalid range
                temp_path.unlink(missing_ok=True)
                return True, start_byte

            response.raise_for_status()

            # Determine write mode and total size
            mode = "ab" if headers.get("Range") else "wb"
            total_size = -1
            try:
                total_size = int(response.headers.get("Content-Length", "-1"))
                if total_size >= 0 and start_byte > 0:
                    total_size += start_byte
            except (ValueError, TypeError):
                total_size = -1

            downloaded = start_byte

            with open(temp_path, mode) as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_callback:
                            progress_callback(downloaded, total_size)

        # Move temp file to final location
        local_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.replace(local_path)
        logger.debug("Successfully downloaded %s -> %s", url, local_path)
        return True, downloaded

    except requests.exceptions.RequestException as exc:
        logger.warning("Download failed for %s: %s", url, exc)
        return False, 0
    except OSError as exc:
        logger.error("File operation failed for %s: %s", local_path, exc)
        return False, 0
    except Exception as exc:
        logger.error("Unexpected error downloading %s: %s", url, exc)
        return False, 0


def download_files(
    media_list: List[Tuple[str, Path]],
    workers: int = 3,
    skip_existing: bool = True,
    partial_ext: str = ".partial",
    timeout: int = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff: float = DEFAULT_RETRY_BACKOFF,
    progress_desc: str = "Downloading",
    disable_progress: bool = False,
) -> Tuple[int, int, int, List[Path]]:
    """Download multiple files using a thread pool.

    Args:
        media_list: List of (url, local_path) tuples to download.
        workers: Number of parallel download workers.
        skip_existing: If True, skip files that already exist.
        partial_ext: Extension for partial download files.
        timeout: Request timeout in seconds per download.
        max_retries: Maximum number of retry attempts per file.
        retry_backoff: Base seconds between retries (multiplied by attempt).
        progress_desc: Description for the progress bar.
        disable_progress: If True, disable progress bar display.

    Returns:
        Tuple of (success_count, skip_count, fail_count, downloaded_paths).
    """
    if not media_list:
        return 0, 0, 0, []

    total = len(media_list)
    workers = max(1, min(workers, total))

    # Track results
    success_count = 0
    skip_count = 0
    fail_count = 0
    downloaded_paths: List[Path] = []

    # Threading control
    stop_event = threading.Event()
    active_sessions: set = set()
    sessions_lock = threading.Lock()
    partials: set[Path] = set()
    partials_lock = threading.Lock()

    def worker(item: Tuple[str, Path]) -> Tuple[bool, bool]:
        """Download a single file. Returns (success, skipped)."""
        url, local_path = item
        session = None

        if stop_event.is_set():
            return False, False

        # Check if file exists
        if skip_existing and local_path.is_file():
            try:
                # Verify size match via HEAD request
                head_response = requests.head(url, timeout=10, allow_redirects=True)
                remote_size = int(head_response.headers.get("Content-Length", "-1"))
                local_size = local_path.stat().st_size

                if remote_size < 0 or local_size == remote_size:
                    logger.debug("Skipping existing file: %s", local_path)
                    return True, True
            except Exception:
                # If HEAD fails, assume file exists and skip
                return True, True

        # Prepare temp path
        temp_path = local_path.with_suffix(local_path.suffix + partial_ext)

        # Attempt download with retries
        attempt = 0
        while attempt < max_retries:
            if stop_event.is_set():
                return False, False

            attempt += 1
            session = None

            try:
                session = requests.Session()
                session.mount("https://", HTTPAdapter(max_retries=1))
                with sessions_lock:
                    active_sessions.add(session)

                # Check for resume capability
                headers: Dict[str, str] = {}
                start_byte = 0
                if temp_path.is_file():
                    start_byte = temp_path.stat().st_size
                    if start_byte > 0:
                        headers["Range"] = f"bytes={start_byte}-"

                with session.get(
                    url, stream=True, timeout=timeout, headers=headers
                ) as response:
                    if response.status_code == 416:
                        # Range unsatisfiable - restart
                        temp_path.unlink(missing_ok=True)
                        continue

                    response.raise_for_status()

                    mode = "ab" if headers.get("Range") else "wb"

                    # Track partial file
                    with partials_lock:
                        partials.add(temp_path)

                    with open(temp_path, mode) as f:
                        for chunk in response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                            if stop_event.is_set():
                                raise KeyboardInterrupt()
                            if chunk:
                                f.write(chunk)

                # Move to final location
                local_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path.replace(local_path)

                # Cleanup tracking
                with partials_lock:
                    partials.discard(temp_path)

                logger.info("Downloaded: %s", local_path.name)
                return True, False

            except (requests.exceptions.RequestException, OSError) as exc:
                logger.warning(
                    "Download attempt %d/%d failed for %s: %s",
                    attempt,
                    max_retries,
                    url,
                    exc,
                )
                if attempt < max_retries:
                    time.sleep(retry_backoff * attempt)
                else:
                    logger.error("Download failed after %d retries: %s", max_retries, url)
            except KeyboardInterrupt:
                logger.info("Download interrupted: %s", url)
                return False, False
            finally:
                if session is not None:
                    with sessions_lock:
                        active_sessions.discard(session)
                    session.close()

        return False, False

    # Set up SIGINT handler for graceful shutdown
    old_handler = signal.getsignal(signal.SIGINT)

    def _sigint_handler(sig: int, frame: Any) -> None:
        """Handle Ctrl+C by stopping downloads and cleaning up partials."""
        logger.info("Download cancelled by user. Cleaning up...")
        stop_event.set()

        # Close active sessions
        with sessions_lock:
            for s in list(active_sessions):
                try:
                    s.close()
                except Exception:
                    pass

        # Remove partial files
        with partials_lock:
            for p in list(partials):
                try:
                    if p.exists():
                        p.unlink()
                except Exception:
                    pass

        # Raise KeyboardInterrupt to allow proper cleanup
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, _sigint_handler)

    try:
        with rich_progress_or_stderr(
            desc=progress_desc, total=total, disable=disable_progress, unit="files"
        ) as pbar:

            def worker_with_progress(item: Tuple[str, Path]) -> Tuple[bool, bool]:
                result = worker(item)
                pbar.update(1)
                return result

            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(worker_with_progress, item): item
                    for item in media_list
                }

                for future in concurrent.futures.as_completed(futures):
                    item = futures[future]
                    try:
                        success, skipped = future.result()
                        if skipped:
                            skip_count += 1
                        elif success:
                            success_count += 1
                            downloaded_paths.append(item[1])
                        else:
                            fail_count += 1
                    except Exception as exc:
                        logger.error("Worker exception for %s: %s", item, exc)
                        fail_count += 1

    finally:
        signal.signal(signal.SIGINT, old_handler)

    return success_count, skip_count, fail_count, downloaded_paths


class DownloadManager:
    """Manages download operations with progress tracking.

    This class provides a higher-level interface for managing file downloads
    with support for configuration, progress tracking, and result handling.
    """

    def __init__(
        self,
        config: Optional[ArchiveConfig] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
    ):
        """Initialize the download manager.

        Args:
            config: ArchiveConfig instance with download settings.
            progress_callback: Optional callback(filename, bytes, total_bytes).
        """
        self.config = config or ArchiveConfig()
        self.progress_callback = progress_callback
        self._stop_event = threading.Event()
        self._active_sessions: set = set()
        self._sessions_lock = threading.Lock()
        self._partials: set[Path] = set()
        self._partials_lock = threading.Lock()

    def download_single(
        self,
        url: str,
        local_path: Path,
        resume: bool = True,
    ) -> Tuple[bool, int]:
        """Download a single file.

        Args:
            url: The URL to download from.
            local_path: The local path to save to.
            resume: If True, attempt to resume partial downloads.

        Returns:
            Tuple of (success: bool, bytes_downloaded: int).
        """
        if self._stop_event.is_set():
            return False, 0

        # Wrap progress callback to include filename
        wrapped_callback: Optional[Callable[[int, int], None]] = None
        if self.progress_callback:

            def wrapped(bytes_done: int, total: int) -> None:
                self.progress_callback(local_path.name, bytes_done, total)

            wrapped_callback = wrapped

        return download_file(
            url=url,
            local_path=local_path,
            timeout=self.config.request_timeout,
            resume=resume,
            progress_callback=wrapped_callback,
        )

    def download_batch(
        self,
        media_list: List[Tuple[str, Path]],
        progress_desc: str = "Downloading",
    ) -> Tuple[int, int, int, List[Path]]:
        """Download a batch of files.

        Args:
            media_list: List of (url, local_path) tuples.
            progress_desc: Description for progress display.

        Returns:
            Tuple of (success_count, skip_count, fail_count, downloaded_paths).
        """
        return download_files(
            media_list=media_list,
            workers=self.config.workers,
            skip_existing=self.config.skip_existing,
            partial_ext=self.config.partial_extension,
            timeout=self.config.request_timeout,
            max_retries=self.config.max_retries,
            progress_desc=progress_desc,
            disable_progress=self.config.quiet,
        )

    def stop(self) -> None:
        """Signal all downloads to stop."""
        self._stop_event.set()

    def cleanup_partials(self) -> None:
        """Clean up any remaining partial download files."""
        with self._partials_lock:
            for p in list(self._partials):
                try:
                    if p.exists():
                        p.unlink()
                except Exception:
                    pass

    def __enter__(self) -> "DownloadManager":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Context manager exit - cleanup partial files."""
        self.cleanup_partials()
        return False


def download_with_config(
    media_list: List[Tuple[str, Path]],
    config: ArchiveConfig,
    progress_desc: str = "Downloading",
) -> Tuple[int, int, int, List[Path]]:
    """Download files using ArchiveConfig settings.

    Convenience function that creates a DownloadManager with the given
    configuration and downloads all files.

    Args:
        media_list: List of (url, local_path) tuples to download.
        config: ArchiveConfig with download settings.
        progress_desc: Description for progress display.

    Returns:
        Tuple of (success_count, skip_count, fail_count, downloaded_paths).
    """
    with DownloadManager(config) as manager:
        return manager.download_batch(media_list, progress_desc=progress_desc)


__all__ = [
    "download_file",
    "download_files",
    "download_with_config",
    "DownloadManager",
]

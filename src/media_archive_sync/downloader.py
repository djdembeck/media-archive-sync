"""Downloader module for media archive synchronization.

This module provides generic file download functionality with support for:
- Resumable downloads (via HTTP Range requests)
- Parallel downloads using thread pools
- Progress tracking with Rich or tqdm fallbacks
- Configurable retry logic with exponential backoff
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import signal
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter

from .config import ArchiveConfig
from .display import rich_progress_or_stderr
from .logging import get_logger

logger = get_logger(__name__)


class DownloadCancelled(Exception):
    """Raised when a download is cancelled via stop event."""

    pass


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
    progress_callback: Callable[[int, int], None] | None = None,
    stop_event: threading.Event | None = None,
    session: requests.Session | None = None,
    partial_ext: str = ".partial",
) -> tuple[bool, int]:
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
        stop_event: Optional threading.Event to check for cancellation.
        session: Optional requests.Session to use (creates new one if None).
        partial_ext: Extension for partial download files.

    Returns:
        Tuple of (success: bool, bytes_downloaded: int).
    """
    temp_path = local_path.with_suffix(local_path.suffix + partial_ext)
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    headers: dict[str, str] = {}
    start_byte = 0

    # Check for existing partial file for resuming
    if resume and temp_path.is_file():
        start_byte = temp_path.stat().st_size
        if start_byte > 0:
            headers["Range"] = f"bytes={start_byte}-"
            logger.debug("Resuming download from byte %d for %s", start_byte, url)

    _stop_event = stop_event or threading.Event()
    own_session = session is None

    try:
        if own_session:
            session = requests.Session()
            session.mount("https://", HTTPAdapter(max_retries=1))

        def _do_download(_session: requests.Session) -> tuple[bool, int]:
            with _session.get(
                url, stream=True, timeout=timeout, headers=headers
            ) as response:
                if response.status_code == 416:
                    try:
                        head = _session.head(url, timeout=timeout, allow_redirects=True)
                        expected_size = int(head.headers.get("Content-Length", "-1"))
                        actual_size = temp_path.stat().st_size
                        if expected_size > 0 and actual_size == expected_size:
                            temp_path.replace(local_path)
                            return True, actual_size
                    except Exception:
                        pass
                    temp_path.unlink(missing_ok=True)
                    return download_file(
                        url,
                        local_path,
                        timeout,
                        False,
                        chunk_size,
                        progress_callback,
                        _stop_event,
                        _session,
                        partial_ext,
                    )

                response.raise_for_status()

                mode = "ab" if response.status_code == 206 else "wb"
                total_size = -1
                try:
                    total_size = int(response.headers.get("Content-Length", "-1"))
                    if (
                        response.status_code == 206
                        and total_size >= 0
                        and start_byte > 0
                    ):
                        total_size += start_byte
                except (ValueError, TypeError):
                    total_size = -1

                downloaded = 0 if response.status_code == 200 else start_byte

                with open(temp_path, mode) as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if _stop_event.is_set():
                            raise DownloadCancelled()
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

        if own_session:
            with session:
                result = _do_download(session)
                return result
        else:
            result = _do_download(session)
            return result

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
    media_list: list[tuple[str, Path]],
    workers: int = 3,
    skip_existing: bool = True,
    partial_ext: str = ".partial",
    timeout: int = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff: float = DEFAULT_RETRY_BACKOFF,
    progress_desc: str = "Downloading",
    disable_progress: bool | None = None,
    stop_event: threading.Event | None = None,
    active_sessions: set | None = None,
    sessions_lock: threading.Lock | None = None,
    partials: set[Path] | None = None,
    partials_lock: threading.Lock | None = None,
) -> tuple[int, int, int, list[Path]]:
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
        disable_progress: If True, disable progress bar display. If None, auto-detect.
        stop_event: Optional threading.Event for cancellation.
        active_sessions: Optional set to track active sessions.
        sessions_lock: Optional lock for active_sessions.
        partials: Optional set to track partial files.
        partials_lock: Optional lock for partials.

    Returns:
        Tuple of (success_count, skip_count, fail_count, downloaded_paths).
    """
    if not media_list:
        return 0, 0, 0, []

    total = len(media_list)
    workers = max(1, min(workers, total))

    success_count = 0
    skip_count = 0
    fail_count = 0
    downloaded_paths: list[Path] = []

    _stop_event = stop_event or threading.Event()
    _active_sessions = active_sessions if active_sessions is not None else set()
    _sessions_lock = sessions_lock or threading.Lock()
    _partials = partials if partials is not None else set()
    _partials_lock = partials_lock or threading.Lock()

    def worker(item: tuple[str, Path]) -> tuple[bool, bool]:
        """Download a single file. Returns (success, skipped)."""
        url, local_path = item
        session = None

        if _stop_event.is_set():
            return False, False

        # Check if file exists
        if skip_existing and local_path.is_file():
            try:
                # Verify size match via HEAD request
                head_response = requests.head(
                    url, timeout=timeout, allow_redirects=True
                )
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
            if _stop_event.is_set():
                return False, False

            attempt += 1
            session = None

            try:
                session = requests.Session()
                session.mount("https://", HTTPAdapter(max_retries=1))
                with _sessions_lock:
                    _active_sessions.add(session)

                # Check for resume capability
                headers: dict[str, str] = {}
                start_byte = 0
                if temp_path.is_file():
                    start_byte = temp_path.stat().st_size
                    if start_byte > 0:
                        headers["Range"] = f"bytes={start_byte}-"

                # Ensure parent directory exists before download
                local_path.parent.mkdir(parents=True, exist_ok=True)

                with session.get(
                    url, stream=True, timeout=timeout, headers=headers
                ) as response:
                    if response.status_code == 416:
                        # Range unsatisfiable - verify size before deleting
                        try:
                            head = session.head(
                                url, timeout=timeout, allow_redirects=True
                            )
                            expected_size = int(
                                head.headers.get("Content-Length", "-1")
                            )
                            if (
                                temp_path.exists()
                                and expected_size > 0
                                and temp_path.stat().st_size == expected_size
                            ):
                                # File is complete, promote it
                                temp_path.replace(local_path)
                                return True, False
                        except Exception:
                            pass
                        temp_path.unlink(missing_ok=True)
                        continue

                    response.raise_for_status()

                    # Use status_code to determine mode (206 = partial content)
                    mode = "ab" if response.status_code == 206 else "wb"

                    with _partials_lock:
                        _partials.add(temp_path)

                    with open(temp_path, mode) as f:
                        for chunk in response.iter_content(
                            chunk_size=DEFAULT_CHUNK_SIZE
                        ):
                            if _stop_event.is_set():
                                raise DownloadCancelled()
                            if chunk:
                                f.write(chunk)

                temp_path.replace(local_path)

                with _partials_lock:
                    _partials.discard(temp_path)

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
                    logger.error(
                        "Download failed after %d retries: %s", max_retries, url
                    )
            except DownloadCancelled:
                logger.info("Download cancelled: %s", url)
                return False, False
            finally:
                if session is not None:
                    with _sessions_lock:
                        _active_sessions.discard(session)
                    session.close()

        return False, False

    old_handler = None
    _sigint_handler = None

    if threading.current_thread() is threading.main_thread():
        old_handler = signal.getsignal(signal.SIGINT)

        def _sigint_handler(sig: int, frame: Any) -> None:
            logger.info("Download cancelled by user. Cleaning up...")
            _stop_event.set()

            with _sessions_lock:
                for s in list(_active_sessions):
                    with contextlib.suppress(Exception):
                        s.close()

            with _partials_lock:
                for p in list(_partials):
                    with contextlib.suppress(Exception):
                        if p.exists():
                            p.unlink()

            raise KeyboardInterrupt()

        signal.signal(signal.SIGINT, _sigint_handler)

    try:
        with rich_progress_or_stderr(
            desc=progress_desc, total=total, disable=disable_progress, unit="files"
        ) as pbar:

            def worker_with_progress(item: tuple[str, Path]) -> tuple[bool, bool]:
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
        if (
            old_handler is not None
            and threading.current_thread() is threading.main_thread()
        ):
            signal.signal(signal.SIGINT, old_handler)

    return success_count, skip_count, fail_count, downloaded_paths


class DownloadManager:
    """Manages download operations with progress tracking.

    This class provides a higher-level interface for managing file downloads
    with support for configuration, progress tracking, and result handling.
    """

    def __init__(
        self,
        config: ArchiveConfig | None = None,
        progress_callback: Callable[[str, int, int], None] | None = None,
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
    ) -> tuple[bool, int]:
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

        session: requests.Session | None = None
        temp_path = local_path.with_suffix(
            local_path.suffix + self.config.partial_extension
        )

        try:
            with self._sessions_lock:
                session = requests.Session()
                session.mount("https://", HTTPAdapter(max_retries=1))
                self._active_sessions.add(session)

            with self._partials_lock:
                self._partials.add(temp_path)

            # Wrap progress callback to include filename
            wrapped_callback: Callable[[int, int], None] | None = None
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
                stop_event=self._stop_event,
                session=session,
                partial_ext=self.config.partial_extension,
            )
        finally:
            if session is not None:
                with self._sessions_lock:
                    self._active_sessions.discard(session)
                session.close()
            with self._partials_lock:
                self._partials.discard(temp_path)

    def download_batch(
        self,
        media_list: list[tuple[str, Path]],
        progress_desc: str = "Downloading",
    ) -> tuple[int, int, int, list[Path]]:
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
            stop_event=self._stop_event,
            active_sessions=self._active_sessions,
            sessions_lock=self._sessions_lock,
            partials=self._partials,
            partials_lock=self._partials_lock,
        )

    def stop(self) -> None:
        """Signal all downloads to stop."""
        self._stop_event.set()

    def cleanup_partials(self) -> None:
        """Clean up any remaining partial download files."""
        with self._partials_lock:
            for p in list(self._partials):
                with contextlib.suppress(Exception):
                    if p.exists():
                        p.unlink()

    def __enter__(self) -> DownloadManager:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Context manager exit - cleanup partial files."""
        self.cleanup_partials()
        return False


def download_with_config(
    media_list: list[tuple[str, Path]],
    config: ArchiveConfig,
    progress_desc: str = "Downloading",
) -> tuple[int, int, int, list[Path]]:
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

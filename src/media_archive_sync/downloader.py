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
import json
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter

from .config import ArchiveConfig
from .display import rich_progress_or_stderr
from .logging import get_logger

logger = get_logger(__name__)


class DownloadCancelledError(Exception):
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
                            raise DownloadCancelledError()
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

    except DownloadCancelledError:
        logger.debug("Download cancelled for %s", url)
        return False, 0
    except requests.exceptions.RequestException as exc:
        logger.warning("Download failed for %s: %s", url, exc)
        return False, 0
    except OSError as exc:
        logger.error("File operation failed for %s: %s", local_path, exc)
        return False, 0
    except Exception as exc:
        logger.error("Unexpected error downloading %s: %s", url, exc)
        return False, 0


@dataclass(frozen=True)
class TimeoutPolicy:
    """Network timeout and retry policy for verified downloads."""

    connect_seconds: float = 5
    read_seconds: float = 30
    retry_attempts: int = 3
    backoff_seconds: float = 2.0


@dataclass(frozen=True)
class VerifiedDownload:
    """Structured outcome of a verified staged download."""

    ok: bool
    bytes_downloaded: int
    resumed: bool
    final_path: Path | None
    expected_size: int | None
    remote_identity: dict | None
    error_class: str | None = None
    error_message: str | None = None


def _verified_identity(path: Path) -> dict | None:
    """Load a previously recorded source identity from a sidecar manifest."""
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _record_identity(path: Path, identity: dict, *, complete: bool) -> None:
    """Persist the source identity sidecar atomically."""
    payload = {
        "content_length": identity.get("content_length"),
        "etag": identity.get("etag"),
        "last_modified": identity.get("last_modified"),
        "complete": complete,
    }
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _head_identity(session: requests.Session, url: str, timeout: tuple) -> dict:
    """Discover the current remote identity via a HEAD request."""
    response = session.head(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    raw_length = response.headers.get("Content-Length")
    try:
        length = int(raw_length) if raw_length is not None else None
    except (TypeError, ValueError):
        length = None
    return {
        "content_length": length,
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
    }


def _range_start(response: requests.Response) -> int | None:
    """Extract the first offset from a Content-Range header, if present."""
    header = response.headers.get("Content-Range")
    if not header or "-" not in header:
        return None
    start = header.split("-", 1)[0].rsplit(" ", 1)[-1]
    try:
        return int(start)
    except ValueError:
        return None


def _range_total(response: requests.Response) -> int | None:
    """Extract the total length from a Content-Range header, if present."""
    header = response.headers.get("Content-Range")
    if not header or "/" not in header:
        return None
    total = header.split("/", 1)[1].strip()
    try:
        return int(total)
    except ValueError:
        return None


def _validate_resume(
    recorded: dict | None, current: dict, partial_size: int, expected_size: int | None
) -> bool:
    """Decide whether an existing partial may be resumed from its offset.

    A partial is discarded (False) when the remote identity cannot be
    reconciled with the recorded sidecar: the current remote length is
    <= the partial length, a recorded identifier (ETag/Last-Modified)
    changed, the current length differs from the recorded expected length,
    or the current length differs from the caller's expected length.
    """
    if recorded is None:
        return False
    recorded_length = recorded.get("content_length")
    current_length = current.get("content_length")
    if current_length is not None and current_length <= partial_size:
        return False
    if (
        recorded_length is not None
        and current_length is not None
        and current_length != recorded_length
    ):
        return False
    if (
        expected_size is not None
        and current_length is not None
        and current_length != expected_size
    ):
        return False
    for key in ("etag", "last_modified"):
        previous = recorded.get(key)
        if previous in (None, "") or previous == current.get(key):
            continue
        return False
    return True


def _read_body(
    response: requests.Response,
    staging_path: Path,
    mode: str,
    base: int,
    chunk_size: int,
    progress: Callable[[int, int], None] | None,
    expected_size: int | None,
) -> int:
    """Stream a response body into the staging file; return total bytes."""
    total = base
    if expected_size is not None:
        reported_total = expected_size
    else:
        header_size = int(response.headers.get("Content-Length", -1) or -1)
        reported_total = base + header_size if header_size > 0 else -1
    with open(staging_path, mode) as handle:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                handle.write(chunk)
                total += len(chunk)
                if progress:
                    progress(total, reported_total)
    return total


def _resume_decision(
    recorded: dict | None,
    current: dict,
    partial_size: int,
    expected_size: int | None,
) -> bool:
    """Whether a non-empty partial must be discarded before this attempt."""
    if partial_size <= 0:
        return False
    if recorded is None:
        return True
    return not _validate_resume(recorded, current, partial_size, expected_size)


def _range_outcome(
    response: requests.Response, partial_size: int, expected_size: int | None
) -> tuple[str, str | None]:
    """Validate a resume response; return (action, error).

    Actions: "read" (use this body), "restart" (safe to retry from byte
    zero without consuming a retry attempt), "fail" (immediate, unretryable
    error; ``error`` carries the error class), "raise" (HTTP error to raise).
    """
    status = response.status_code
    if status == 416:
        return "fail", "RangeNotSatisfiable"
    if partial_size <= 0:
        if status not in (200, 206):
            return "raise", None
        return "read", None
    if status != 206:
        if status != 200:
            return "raise", None
        # Range ignored: the body is the full object, restart from zero.
        return "restart", None
    header_start = _range_start(response)
    if header_start != partial_size:
        logger.warning(
            "Content-Range start %r != %d; restarting", header_start, partial_size
        )
        return "restart", None
    total = _range_total(response)
    if expected_size is not None and total is not None and total != expected_size:
        logger.warning("Content-Range total %d != %d; restarting", total, expected_size)
        return "restart", None
    return "read", None


def _verified_failure(
    expected_size: int | None,
    remote_identity: dict | None,
    error_class: str,
    error_message: str,
    *,
    ok_bytes: int = 0,
    final_path: Path | None = None,
    resumed: bool = False,
) -> VerifiedDownload:
    """Build a structured failure outcome."""
    return VerifiedDownload(
        ok=False,
        bytes_downloaded=ok_bytes,
        resumed=resumed,
        final_path=final_path,
        expected_size=expected_size,
        remote_identity=remote_identity,
        error_class=error_class,
        error_message=error_message,
    )


def _verified_attempt(
    session: requests.Session,
    url: str,
    staging_path: Path,
    manifest_path: Path,
    current: dict,
    expected_size: int | None,
    timeout_pair: tuple,
    progress: Callable[[int, int], None] | None,
) -> tuple[bool, str | None]:
    """Run one full download pass (resume or fresh) until it completes.

    Returns ``(resumed, fail_class)``. ``fail_class`` is ``None`` on
    success or ``"RangeNotSatisfiable"`` when a 416 makes the partial
    unrecoverable. Connection errors propagate to the caller, which owns
    the retry/backoff budget. Range-not-honored cases are handled here
    (safe restart from byte zero) without consuming a retry attempt.
    """
    while True:
        partial_size = staging_path.stat().st_size if staging_path.is_file() else 0
        recorded = _verified_identity(manifest_path)
        if _resume_decision(recorded, current, partial_size, expected_size):
            logger.warning(
                "Discarding %s: source identity not reconciled", staging_path
            )
            staging_path.unlink(missing_ok=True)
            partial_size = 0
        _record_identity(manifest_path, current, complete=False)
        resumed = False
        headers: dict[str, str] = {}
        if partial_size > 0:
            headers["Range"] = f"bytes={partial_size}-"
        with session.get(
            url, stream=True, timeout=timeout_pair, headers=headers
        ) as response:
            action, fail_class = _range_outcome(response, partial_size, expected_size)
            if action == "restart":
                logger.warning(
                    "Range resume not honored (%s); restarting",
                    response.status_code,
                )
                response.close()
                staging_path.unlink(missing_ok=True)
                continue
            if action == "fail":
                staging_path.unlink(missing_ok=True)
                return resumed, fail_class
            if action == "raise":
                response.raise_for_status()
            mode = "ab" if response.status_code == 206 else "wb"
            if mode == "ab":
                resumed = True
            _read_body(
                response,
                staging_path,
                mode,
                partial_size,
                DEFAULT_CHUNK_SIZE,
                progress,
                expected_size,
            )
        _record_identity(manifest_path, current, complete=True)
        return resumed, None


def download_verified(
    url: str,
    staging_path: Path,
    *,
    expected_size: int | None,
    timeout: TimeoutPolicy | None = None,
    progress: Callable[[int, int], None] | None = None,
    session: requests.Session | None = None,
    identity_manifest_path: Path | None = None,
) -> VerifiedDownload:
    """Perform a resumable, identity-verified staged download.

    Writes to ``staging_path`` (a .partial file) and never renames it to a
    final media path; callers perform that rename themselves. The current
    remote identity (Content-Length, ETag, Last-Modified) is recorded in an
    identity sidecar so resumptions can be validated.

    Args:
        url: The URL to download from.
        staging_path: The .partial target to write to.
        expected_size: Expected final byte count, or None when unknown.
        timeout: Timeout/retry policy (defaults to TimeoutPolicy()).
        progress: Optional callback(downloaded_total, expected_or_-1).
        session: Optional requests.Session (creates one if None).
        identity_manifest_path: Sidecar JSON path (defaults to
            staging_path + '.identity.json').

    Returns:
        A structured VerifiedDownload outcome; errors never escape.
    """
    policy = timeout or TimeoutPolicy()
    timeout_pair: tuple = (policy.connect_seconds, policy.read_seconds)
    own_session = session is None
    if own_session:
        session = requests.Session()
    manifest_path = (
        identity_manifest_path
        if identity_manifest_path is not None
        else staging_path.parent / (staging_path.name + ".identity.json")
    )
    staging_path.parent.mkdir(parents=True, exist_ok=True)

    current: dict | None = None
    error_class: str | None = None
    error_message: str | None = None
    try:
        for attempt in range(policy.retry_attempts):
            if attempt > 0:
                time.sleep(policy.backoff_seconds)
            try:
                current = _head_identity(session, url, timeout_pair)
                resumed, fail_class = _verified_attempt(
                    session,
                    url,
                    staging_path,
                    manifest_path,
                    current,
                    expected_size,
                    timeout_pair,
                    progress,
                )
            except (requests.RequestException, OSError) as exc:
                error_class = type(exc).__name__
                error_message = str(exc)
                logger.warning(
                    "Attempt %d/%d failed for %s: %s: %s",
                    attempt + 1,
                    policy.retry_attempts,
                    url,
                    error_class,
                    error_message,
                )
                continue
            if fail_class is not None:
                return _verified_failure(
                    expected_size,
                    current,
                    fail_class,
                    "HTTP 416: partial not resolvable",
                )
            final_size = staging_path.stat().st_size
            if expected_size is not None and final_size != expected_size:
                return _verified_failure(
                    expected_size,
                    current,
                    "SizeMismatch",
                    f"final size {final_size} != expected {expected_size}",
                    ok_bytes=final_size,
                    final_path=staging_path,
                    resumed=resumed,
                )
            return VerifiedDownload(
                ok=True,
                bytes_downloaded=final_size,
                resumed=resumed,
                final_path=staging_path,
                expected_size=expected_size,
                remote_identity=current,
            )
    finally:
        if own_session and session is not None:
            session.close()
    final_path = staging_path if staging_path.is_file() else None
    return _verified_failure(
        expected_size,
        current,
        error_class or "RetriesExhausted",
        error_message or "all retry attempts failed",
        ok_bytes=final_path.stat().st_size if final_path is not None else 0,
        final_path=final_path,
    )


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
                                raise DownloadCancelledError()
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
            except DownloadCancelledError:
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
        disable_progress = None if not self.config.quiet else True
        return download_files(
            media_list=media_list,
            workers=self.config.workers,
            skip_existing=self.config.skip_existing,
            partial_ext=self.config.partial_extension,
            timeout=self.config.request_timeout,
            max_retries=self.config.max_retries,
            progress_desc=progress_desc,
            disable_progress=disable_progress,
            stop_event=self._stop_event,
            active_sessions=self._active_sessions,
            sessions_lock=self._sessions_lock,
            partials=self._partials,
            partials_lock=self._partials_lock,
        )

    def stop(self) -> None:
        """Signal all downloads to stop and interrupt active sessions."""
        self._stop_event.set()
        # Interrupt active requests to unblock threads waiting on network I/O
        with self._sessions_lock:
            for session in list(self._active_sessions):
                with contextlib.suppress(Exception):
                    session.close()
        # Clean up partial files to match SIGINT handler behavior
        self.cleanup_partials()

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
    "TimeoutPolicy",
    "VerifiedDownload",
    "download_file",
    "download_files",
    "download_with_config",
    "download_verified",
    "DownloadManager",
]

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
import functools
import json
import os
import signal
import socket
import stat
import threading
import time
from collections.abc import Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address, ip_address
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urljoin, urlparse

import requests
import urllib3.connection
import urllib3.connectionpool
from requests.adapters import HTTPAdapter

from .config import ArchiveConfig
from .display import rich_progress_or_stderr
from .logging import get_logger

logger = get_logger(__name__)


class DownloadCancelledError(Exception):
    """Raised when a download is cancelled via stop event."""

    pass


class _StagingFileChangedError(OSError):
    """The staging file was modified by another writer (single-writer
    invariant broken); detected before an append. Handled as a clean
    restart from byte zero by _verified_attempt."""

    pass


class URLPolicyError(requests.RequestException):
    """A URL (or redirect target) violated the public-address policy.

    Raised as a subclass of :class:`requests.RequestException` so callers
    treat it as a normal network failure.
    """

    pass


class IdentityConflictError(requests.RequestException):
    """Identity conflicts persist past the restart budget for one pass.

    Raised by :func:`_verified_attempt` after the same HEAD/GET identity
    conflict forces more restarts than :data:`_MAX_IDENTITY_CONFLICT_RESTARTS`
    allows (a server rotating ETag/Last-Modified on every 206 would otherwise
    restart forever, bypassing the retry budget). It is a
    :class:`requests.RequestException` subclass so the per-attempt handler in
    :func:`download_verified` consumes a retry attempt and reports a structured
    :class:`~.VerifiedDownload` failure instead of an endless loop. Internal;
    not exported from the package.
    """

    pass


def _is_disallowed_address(address: object) -> bool:
    """True when an IP address must never be fetched from (SSRF guard)."""
    if isinstance(address, (IPv4Address, IPv6Address)):
        return (
            address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_reserved
            or address.is_multicast
            or address.is_unspecified
        )
    return True  # not an IPv4/IPv6 literal: fail closed


def _is_local_address(address: object) -> bool:
    """True when an IP address is private, loopback, or link-local."""
    if not isinstance(address, (IPv4Address, IPv6Address)):
        return False
    return address.is_private or address.is_loopback or address.is_link_local


def _assert_public_url(url: str, *, allow_local: bool = False) -> None:
    """Raise URLPolicyError unless ``url`` satisfies the fetch policy.

    Requires an http/https scheme and no userinfo. The hostname must
    resolve, and *every* resolved address must be acceptable: by default
    only public addresses pass (private, loopback, link-local, reserved,
    multicast and unspecified ranges are rejected, fail closed). With
    ``allow_local=True`` — used only for the operator-chosen initial
    URL, which may legitimately be loopback (a local media server) —
    private/loopback/link-local addresses are additionally permitted;
    reserved/multicast/unspecified remain rejected.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise URLPolicyError(f"URL scheme not allowed (need http/https): {url}")
    if parsed.username is not None or parsed.password is not None:
        raise URLPolicyError(f"URL userinfo not allowed: {url}")
    hostname = parsed.hostname
    if not hostname:
        raise URLPolicyError(f"URL has no hostname: {url}")
    try:
        infos = socket.getaddrinfo(hostname, parsed.port or 80)
    except (OSError, ValueError) as exc:
        raise URLPolicyError(f"Cannot resolve hostname for {url}: {exc}") from exc
    for info in infos:
        literal = info[4][0]
        try:
            ip = ip_address(literal)
        except ValueError as exc:
            raise URLPolicyError(f"Unparseable resolved address for {url}") from exc
        if _is_disallowed_address(ip) and not (allow_local and _is_local_address(ip)):
            raise URLPolicyError(f"Resolved address not public for {url}: {ip}")


# Pinned parent-directory fds for a confined download, keyed by the resolved
# parent ``Path``. Set for the duration of one :func:`download_verified` call
# (confinement enabled) and read by the confined leaf helpers so they can
# open, stat, unlink and rename a leaf relative to the pinned parent inode.
# See :func:`_open_confined_leaf` / :func:`_confined_stat` /
# :func:`_confined_unlink`.
_confined_dirs: ContextVar[dict[Path, int] | None] = ContextVar(
    "_mas_confined_dirs", default=None
)


def _open_confined_leaf(leaf: Path, flags: int, mode: int) -> int:
    """Open ``leaf`` with ``O_NOFOLLOW``; pin its parent dir when confined.

    ``O_NOFOLLOW`` always guards the leaf itself: a symlink swapped in at the
    leaf path between confinement validation and this open is refused (ELOOP)
    instead of written through. When a parent-directory fd is pinned
    (confined download), the leaf is opened RELATIVE to that fd via
    ``dir_fd``: the parent then resolves against the inode captured at open
    time, so a parent directory swapped for a symlink after validation cannot
    redirect the write outside the root (it fails the open, not escapes it).
    Without confinement the plain absolute-path open is used.
    """
    parent_fd = _confined_parent_fd(leaf)
    if parent_fd is not None:
        return os.open(leaf.name, flags | os.O_NOFOLLOW, mode, dir_fd=parent_fd)
    return os.open(leaf, flags | os.O_NOFOLLOW, mode)


def _open_confined_parent_dirs(root: Path, *parents: Path) -> dict[Path, int]:
    """Pin each parent directory to an ``O_NOFOLLOW | O_DIRECTORY`` fd.

    Every component between ``root`` and a ``parent`` is opened
    incrementally — the root first, then each remaining component
    ``os.open``-ed relative to the previous component's fd (``dir_fd``) with
    ``O_RDONLY | O_DIRECTORY | O_NOFOLLOW``. A single open of the full path
    would only apply ``O_NOFOLLOW`` to the FINAL component: any intermediate
    directory symlinked after the upfront checks (or ``root`` itself) would
    be followed during traversal, so the pinned fd could capture an external
    directory and every subsequent confined write would escape the root.
    Walking component by component makes each check-then-use step atomic
    per component: a symlinked intermediate is refused at the open site
    (ELOOP, or ENOTDIR on Linux where O_NOFOLLOW|O_DIRECTORY on a
    symlink-to-dir finds the unfollowed link is not a directory) instead of
    followed. A component that is a symlink even before the upfront checks
    ran is caught here too — the pre-check is defense-in-depth, the walk is
    the race-tight gate.

    Returns ``{parent: fd}`` (de-duplicated; both parents being the same
    directory pins it once). The held fd pins the real directory inode for
    the lifetime of the download so :func:`_open_confined_leaf` can resolve
    leaves against it.

    Fd accounting: intermediate component fds are closed as soon as the
    next component opens — only the final (pinned) fd per parent is held,
    so a walk of depth N leaks zero fds of its own. When a pinned parent
    IS the root, the root's own fd is that parent's pinned fd and is
    returned to the caller instead of closed here. The caller must
    ``os.close`` every returned fd — including the root's when it was
    handed over (see ``download_verified``); otherwise the helper closes
    its own root fd once every parent is pinned. A failure to open a
    later parent (or component) closes every fd that walk opened
    (intermediates plus any newly opened final), in addition to the
    already-pinned parents' fds (no fd leak on any path).
    """
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    root_fd = os.open(root, flags)
    fds: dict[Path, int] = {}
    # Intermediate fds of the parent walk currently in progress (see below);
    # bound before the try so the except handler can always iterate it.
    walk_fds: list[int] = []
    try:
        for parent in parents:
            if parent not in fds:
                rel = Path(os.path.abspath(parent)).relative_to(
                    Path(os.path.abspath(root))
                )
                # rel is always relative (root == parent included, where it
                # is "."): walk it component by component, each open
                # dir_fd-relative to the component pinned just before it.
                fd = root_fd
                for part in rel.parts:
                    fd = os.open(part, flags, dir_fd=fd)
                    if walk_fds:
                        # The previous component is pinned no more: the
                        # next open (and the final pin) resolves against
                        # the new fd, so release it immediately.
                        os.close(walk_fds.pop())
                    walk_fds.append(fd)
                # The pinned fd (a just-opened component, or root_fd when
                # rel.parts is empty) is held by the caller, not by the
                # walk; drop it from the in-progress set.
                fds[parent] = fd
                walk_fds.clear()
    except BaseException:
        # Close every fd this parent's in-progress walk opened (the
        # intermediates still held plus the final if it opened);
        # walk_fds cannot contain root_fd, which the helper closes itself.
        for fd in walk_fds:
            os.close(fd)
        for fd in fds.values():
            os.close(fd)
        raise
    finally:
        # The root fd is the caller's to close only when it was handed
        # over as a pinned parent (parent == root); every other path
        # (including failures — see except — which never leave it pinned)
        # closes it here.
        if root_fd not in fds.values():
            os.close(root_fd)
    return fds


def _confined_parent_fd(leaf: Path) -> int | None:
    """Pinned fd for ``leaf``'s parent directory, or None when the parent
    is not pinned (confinement inactive, or the leaf sits in a
    non-pinned directory).

    Every confined-leaf helper consults the ContextVar — never a captured
    dict — so once ``download_verified`` closes the fds and resets the
    ContextVar in its finally, these helpers fall back to the plain Path
    API and can never touch a closed fd.
    """
    confined = _confined_dirs.get()
    return confined.get(leaf.parent) if confined is not None else None


def _confined_stat(leaf: Path) -> os.stat_result:
    """stat ``leaf`` through the pinned parent fd when confined, so the
    lookup resolves against the pinned directory inode and a parent
    symlink swap cannot redirect it. Plain ``leaf.stat()`` otherwise.
    Raises OSError (including FileNotFoundError) as ``leaf.stat()`` would.
    """
    fd = _confined_parent_fd(leaf)
    if fd is not None:
        return os.stat(leaf.name, dir_fd=fd)
    return leaf.stat()


def _confined_is_file(leaf: Path) -> bool:
    """Regular-file check on ``leaf`` through the pinned parent fd when
    confined; plain ``leaf.is_file()`` otherwise."""
    fd = _confined_parent_fd(leaf)
    if fd is not None:
        try:
            st = os.stat(leaf.name, dir_fd=fd)
        except OSError:
            return False
        return stat.S_ISREG(st.st_mode)
    return leaf.is_file()


def _confined_unlink(leaf: Path) -> None:
    """Remove ``leaf`` through the pinned parent fd when confined — it
    deletes the directory entry itself and can never follow a swapped
    parent symlink to unlink the target — with ``missing_ok=True``
    semantics; plain ``leaf.unlink(missing_ok=True)`` otherwise."""
    fd = _confined_parent_fd(leaf)
    if fd is not None:
        try:
            os.unlink(leaf.name, dir_fd=fd)
        except FileNotFoundError:
            return
        return
    leaf.unlink(missing_ok=True)


def _assert_no_symlinked_dirs_under_root(root: Path, *parents: Path) -> None:
    """Reject a symlinked directory in the chain between ``root`` and each
    ``parent`` (the parent itself included, the root excluded).

    ``_open_confined_parent_dirs`` pins parents with an incremental
    per-component O_NOFOLLOW walk, which refuses a symlink at every level —
    including intermediates — as of the open. This up-front check remains as
    defense in depth: it rejects the same condition earlier (before any fd is
    opened) with a named error, and keeps the strict policy self-consistent
    with ``_enforce_confinement``: under confinement, no symlinked directory
    anywhere between the root and a confined parent.
    """
    abs_root = Path(os.path.abspath(root))
    for parent in parents:
        current = Path(os.path.abspath(parent))
        while current != abs_root and current != current.parent:
            if current.is_symlink():
                raise ValueError(
                    f"Symlinked directory not allowed under download root "
                    f"{root} (O_NOFOLLOW pinning): {current}"
                )
            current = current.parent


def _url_policy_hook(
    response: requests.Response,
    *,
    allow_local: bool = False,
    **_kwargs: object,
) -> requests.Response:
    """Session hook: validate each fetched URL and redirect target.

    This hook is defense-in-depth for redirect ``Location`` targets and the
    fetched response URL. The primary SSRF barrier is the connection-time
    peer guard (see :func:`_install_peer_check`), which checks the actual TCP
    peer on every connect, so a rebinding resolver cannot bypass the policy.
    The hook cannot see the resolved peer, so it complements — not replaces —
    that guard: a redirect to an internal *host* is still caught here even
    though the guard would reject it at connect time, and it re-validates the
    fetched URL after each hop.

    Runs after every response — including each redirect hop, which is a
    separate request through this same session (verified empirically:
    with ``allow_redirects=True`` the hook fires per hop, and an
    exception raised inside propagates out of the request). ``response.next``
    is ``None`` under ``allow_redirects=True``, so the redirect target
    must be derived from the ``Location`` header, relative to the
    response URL, via :func:`urllib.parse.urljoin`.

    Trust split: the response to the *initial* request is checked with
    ``allow_local`` — that address is the operator-chosen initial URL, and the
    caller's ``allow_local`` decision applies to it. Every response after the
    first was fetched at a redirect target, so its URL is checked strictly:
    the operator never chose redirect targets, so a server must never bounce
    the download to an internal address.
    """
    if response.url:
        # Post-redirect responses were fetched at a Location target:
        # strictly public-only. Only the very first response (no history)
        # may be the operator-chosen local URL (per allow_local).
        _assert_public_url(
            response.url,
            allow_local=(len(response.history) == 0) and allow_local,
        )
    if response.is_redirect:
        location = response.headers.get("Location")
        if location:
            _assert_public_url(urljoin(response.url, location))
    return response


def _install_url_policy(session: requests.Session, allow_local: bool = False) -> None:
    """Register the redirect policy hook on ``session`` (idempotent).

    ``allow_local`` is bound to the hook so the first (operator-chosen)
    response is checked with the caller's local-access decision; redirect
    hops are always checked strictly regardless. Reusing a session re-binds
    the *existing* hook to the new ``allow_local`` rather than stacking a
    second hook, so a session first installed strict and later reused with
    ``allow_local=True`` is not locked out by the stale strict binding.
    """
    hook = getattr(session, "_mas_url_policy_hook", None)
    if hook is None:
        hook = functools.partial(_url_policy_hook, allow_local=allow_local)
        session.hooks = {
            "response": [
                *(session.hooks.get("response", ())),
                hook,
            ]
        }
        session._mas_url_policy_hook = hook  # type: ignore[attr-defined]
        session._mas_url_policy_installed = True  # type: ignore[attr-defined]
    elif hook.keywords.get("allow_local") != allow_local:
        # Update the captured allow_local in place; the stored partial is the
        # same object the hooks list references, so this is the value the hook
        # actually uses.
        hook.keywords["allow_local"] = allow_local


def _check_peer(conn: object) -> None:
    """Reject a connected socket whose TCP peer violates the fetch policy.

    The actual peer is the source of truth, so a rebinding resolver (which
    passes the pre-connect :func:`_assert_public_url` but then serves an
    internal address for the same hostname) cannot bypass the policy. Local
    (private/loopback/link-local) peers are rejected unless the connection's
    host is in the connection's ``_mas_allowed_local`` set (installed by
    :func:`_peer_checked_connection`). Unparseable peers fail closed
    (rejected) for every non-allowed host, and a missing ``sock`` or ``host``
    (peer uninspectable) fails closed for every host — a guarded connection
    that cannot be verified is never allowed to proceed. ``conn`` is a
    urllib3 connection; ``sock``/``host`` are read via ``getattr`` so this
    helper is independent of the concrete class.
    """
    sock = getattr(conn, "sock", None)
    host_attr = getattr(conn, "host", None)
    allowed_obj: object = getattr(conn, "_mas_allowed_local", frozenset())
    allowed = allowed_obj if isinstance(allowed_obj, frozenset) else frozenset()
    # Fail closed: if either attribute is missing the peer CANNOT be
    # inspected, so a silent pass would let a guarded connection connect to
    # an unverified peer. Raise (and close the socket if one exists) instead.
    if sock is None or host_attr is None:
        if sock is not None:
            with contextlib.suppress(Exception):
                sock.close()
        raise URLPolicyError("Unable to inspect connected peer")
    host = str(host_attr).strip("[]").lower()
    host_allowed = host in allowed
    peer = sock.getpeername()[0]
    try:
        ip = ip_address(peer)
    except ValueError:
        if not host_allowed:
            sock.close()
            raise URLPolicyError(
                f"Unparseable connect peer {peer!r} for {host}"
            ) from None
        return
    if _is_disallowed_address(ip) and not (host_allowed and _is_local_address(ip)):
        sock.close()
        raise URLPolicyError(f"Connect peer {peer} for {host} is not allowed")


def _peer_checked_connection(
    base: type[urllib3.connection.HTTPConnection],
    allowed: frozenset[str],
) -> type[urllib3.connection.HTTPConnection]:
    """A connection subclass of ``base`` that enforces the peer policy.

    ``connect`` first runs ``base.connect`` (establishing ``self.sock``), then
    validates the peer against the allowed-local set stored on the
    connection. Built with ``base`` in scope so ``connect`` can call
    ``base.connect(self)`` directly, keeping the guard mypy-checkable and
    free of a separate mixin's ``super()``-in-isolation problem.
    """

    def _connect(self: urllib3.connection.HTTPConnection) -> None:
        base.connect(self)
        _check_peer(self)

    return type(
        f"_PeerChecked_{base.__name__}",
        (base,),
        {"_mas_allowed_local": allowed, "connect": _connect},
    )


class _PeerCheckedHTTPAdapter(HTTPAdapter):
    """HTTPAdapter that mounts peer-checked connection classes.

    On pool-manager init it replaces each pool's ``ConnectionCls`` with a
    guarded subclass carrying the allowed-local set, so every TCP peer is
    validated at connect time. It also unwraps a policy rejection: urllib3
    re-wraps any error raised in ``connect`` into a ``ConnectionError``
    (``requests.RequestException`` is an ``OSError``), so ``send`` walks the
    cause/context chain and re-raises the original :class:`URLPolicyError` to
    preserve the structured failure identity.
    """

    def __init__(self, allowed_local: frozenset[str]) -> None:
        self._mas_allowed_local = allowed_local
        super().__init__()

    def _guarded_pool_classes(self) -> dict[str, type]:
        """Peer-checked pool classes (http + https) for this allowed set.

        Reused by both :meth:`init_poolmanager` (the main pool manager) and
        :meth:`proxy_manager_for` (the per-proxy pool manager) so proxy
        connections carry the same connection-time peer guard as direct ones.
        """
        return {
            "http": type(
                "_PeerCheckedHTTPPool",
                (urllib3.connectionpool.HTTPConnectionPool,),
                {
                    "ConnectionCls": _peer_checked_connection(
                        urllib3.connection.HTTPConnection,
                        self._mas_allowed_local,
                    )
                },
            ),
            "https": type(
                "_PeerCheckedHTTPSPool",
                (urllib3.connectionpool.HTTPSConnectionPool,),
                {
                    "ConnectionCls": _peer_checked_connection(
                        urllib3.connection.HTTPSConnection,
                        self._mas_allowed_local,
                    )
                },
            ),
        }

    def init_poolmanager(
        self, connections: int, maxsize: int, block: bool = False, **kwargs: object
    ) -> None:
        super().init_poolmanager(connections, maxsize, block, **kwargs)
        # Replace the instance attribute, NOT the shared global: urllib3's
        # PoolManager assigns the module-level pool_classes_by_scheme dict
        # directly (no copy), so mutating it would leak the guard into every
        # other session's PoolManager.
        self.poolmanager.pool_classes_by_scheme = self._guarded_pool_classes()

    def proxy_manager_for(self, proxy: str, **proxy_kwargs: object) -> object:
        manager = super().proxy_manager_for(proxy, **proxy_kwargs)
        # A proxy request opens a SEPARATE ProxyManager whose pool classes are
        # otherwise never guarded, so proxy connections would bypass the peer
        # guard. Apply the same instance-attribute replacement (not the shared
        # global) so every scheme the proxy pool may connect through is
        # peer-checked.
        manager.pool_classes_by_scheme = self._guarded_pool_classes()
        return manager

    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,
        timeout: float | tuple[float, float] | tuple[float, None] | None = None,
        verify: bool | str = True,
        cert: bytes | str | tuple[bytes | str, bytes | str] | None = None,
        proxies: Mapping[str, str] | None = None,
    ) -> requests.Response:
        try:
            return super().send(
                request,
                stream=stream,
                timeout=timeout,
                verify=verify,
                cert=cert,
                proxies=proxies,
            )
        except requests.exceptions.ConnectionError as exc:
            current: BaseException | None = exc
            seen: set[int] = set()
            while current is not None and id(current) not in seen:
                seen.add(id(current))
                if isinstance(current, URLPolicyError):
                    raise URLPolicyError(str(current)) from current
                current = current.__cause__ or current.__context__
            raise


def _install_peer_check(
    session: requests.Session, allowed_local: frozenset[str]
) -> None:
    """Mount the connection-time peer guard on ``session`` (http + https).

    ``allowed_local`` is the set of hosts whose loopback/private/link-local
    peers are permitted — by default empty (strict). Passing a caller session
    in opts it into this guard as well as the :func:`_install_url_policy`
    redirect hook.
    """
    adapter = _PeerCheckedHTTPAdapter(allowed_local)
    session.mount("http://", adapter)
    session.mount("https://", adapter)


# Default configuration values
DEFAULT_TIMEOUT = 15
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF = 2.0
DEFAULT_CHUNK_SIZE = 8192
# Maximum identity-conflict restarts within a single _verified_attempt pass.
# Beyond this, a server rotating identity on every 206 would otherwise restart
# forever, bypassing the retry budget, so we raise IdentityConflictError and
# let the caller consume a retry attempt instead.
_MAX_IDENTITY_CONFLICT_RESTARTS = 2


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

        # The parameter is guaranteed non-None here: callers pass their own
        # session, or we created one above when it was None.
        assert session is not None
        # SSRF guards (strict: no local addresses) on our own session and on
        # any caller-provided one; both installs are idempotent. The
        # connection-time peer guard is the primary barrier.
        _install_peer_check(session, frozenset())
        _install_url_policy(session)
        if own_session:
            with session:
                return _do_download(session)
        return _do_download(session)

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
    """Structured outcome of a verified staged download.

    ``bytes_downloaded`` is the total bytes present in the final staging
    file, NOT the bytes transferred by this call: on a resumed download
    it includes bytes already staged by prior calls.
    """

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
    if not _confined_is_file(path):
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
    # O_NOFOLLOW on the tmp sibling as well, so a symlink planted at the
    # sibling path cannot be written through. When confined, the tmp leaf is
    # also opened relative to the pinned parent directory fd so a
    # parent-directory symlink swap cannot redirect it. The final rename is
    # dir_fd-relative as well (tmp and final are siblings under the same
    # pinned manifest parent); rename(2) replaces the final symlink itself
    # rather than following the target component, in either case.
    fd = _open_confined_leaf(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, indent=2) + "\n")
    parent_fd = _confined_parent_fd(path)
    if parent_fd is not None:
        os.replace(tmp_path.name, path.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    else:
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

    Resumption requires a matching persisted validator: at least one of
    ETag/Last-Modified must be present (non-empty) in the recorded sidecar,
    currently present, and equal — length alone never suffices, because a
    same-length replacement would otherwise be appended to the old partial.
    A partial is discarded (False) when the recorded sidecar is missing,
    the current remote length is <= the partial length, a recorded
    identifier (ETag/Last-Modified) changed, the current length differs
    from the recorded expected length, the current length differs from the
    caller's expected length, or no such validator exists. A recorded
    empty-string identifier is not an identifier at all and can never
    satisfy the validator rule. A manifest that claims complete=True
    requires the partial to actually hold the recorded full length; a
    truncated "complete" partial is discarded.
    """
    if recorded is None:
        return False
    if recorded.get("complete") is True:
        recorded_full_length = recorded.get("content_length")
        if recorded_full_length != partial_size:
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
    validator_matched = False
    for key in ("etag", "last_modified"):
        previous = recorded.get(key)
        if previous in (None, ""):
            continue
        current_value = current.get(key)
        if current_value is None:
            continue  # validator unavailable now: untrusted, but not a conflict
        if previous == current_value:
            validator_matched = True
        else:
            return False
    return validator_matched


def _read_body(
    response: requests.Response,
    staging_path: Path,
    mode: str,
    base: int,
    chunk_size: int,
    progress: Callable[[int, int], None] | None,
    expected_size: int | None,
) -> int:
    """Stream a response body into the staging file; return total bytes.

    Single-writer invariant: exactly one download owns ``staging_path``
    at a time (callers give each download its own staging path). The
    size check below guards the append: in "ab" mode the file must still
    hold exactly ``base`` bytes, otherwise something else modified it and
    the append would corrupt the partial, so raise
    _StagingFileChangedError and let the caller restart from zero.
    """
    total = base
    if mode == "ab":
        try:
            # _confined_stat resolves through the pinned parent fd when
            # confined, so a parent symlink swap cannot redirect this
            # size guard to the swapped target's contents.
            actual = _confined_stat(staging_path).st_size
        except OSError as exc:
            raise _StagingFileChangedError(
                f"Staging file {staging_path} vanished before append"
            ) from exc
        if actual != base:
            raise _StagingFileChangedError(
                f"Staging file {staging_path} changed before append: "
                f"expected {base} bytes, found {actual}"
            )
    if expected_size is not None:
        reported_total = expected_size
    else:
        header_size = int(response.headers.get("Content-Length", -1) or -1)
        reported_total = base + header_size if header_size > 0 else -1
    # O_NOFOLLOW refuses to open the staging path if it is a symlink, so a
    # leaf-symlink swap between the confinement check and this open is denied
    # at open time (the existing is_symlink pre-check is defense in depth).
    # When confined, the leaf is also opened RELATIVE to the pinned parent
    # directory fd, so a parent-directory symlink swap after validation
    # cannot redirect the write outside the root.
    open_flags = (
        os.O_WRONLY | os.O_CREAT | os.O_APPEND
        if mode == "ab"
        else os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    )
    fd = _open_confined_leaf(staging_path, open_flags, 0o644)
    with os.fdopen(fd, mode) as handle:
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


def _get_identity_conflicts(response: requests.Response, current: dict) -> bool:
    """True when the GET's identity headers contradict the HEAD identity.

    The resume decision is based on the HEAD identity (``current``), but the
    object may be replaced between HEAD and GET while keeping its length. In
    that case a 206 (or 200) carries the NEW object's ETag/Last-Modified;
    appending it would mix content from different objects while the final
    byte-count check still passes. Only an actual conflict disqualifies: the
    header must be present in both and differ. A header absent from the GET
    (servers may omit it on ranged reads) must NOT force a restart.
    """
    for key in ("ETag", "Last-Modified"):
        recorded = current.get(key.lower())
        provided = response.headers.get(key)
        if recorded and provided and recorded != provided:
            return True
    return False


def _adopt_identity(current: dict, response: requests.Response) -> None:
    """Re-sync ``current`` to the identity carried by ``response``.

    Used when the GET conflicts with the HEAD identity: the object was
    replaced in between. Adopting the GET's identifiers keeps the
    recorded manifest, the outcome's ``remote_identity``, and any further
    comparison aligned with the object actually served, and bounds the
    restart to a single extra request (without this, a persistent
    replacement would restart forever against the stale HEAD identity).
    """
    for header, key in (("ETag", "etag"), ("Last-Modified", "last_modified")):
        value = response.headers.get(header)
        if value is not None:
            current[key] = value
    if response.status_code == 206:
        total = _range_total(response)
    else:
        raw = response.headers.get("Content-Length")
        try:
            total = int(raw) if raw is not None else None
        except (TypeError, ValueError):
            total = None
    if total is not None:
        current["content_length"] = total


def _enforce_confinement(download_root: Path, *paths: Path) -> None:
    """Require each path to stay under ``download_root`` (resolved).

    Symlinks are followed during resolution, so a path whose parent
    chain escapes the root (directly or through a symlinked directory)
    raises ValueError; this check runs before any mkdir/IO.

    Symlinked-directory policy (strict): this resolve() check only verifies
    where the path lands. It does NOT accept symlinked intermediate
    directories under the root: the O_NOFOLLOW parent pinning enforces the
    same policy at open time with an incremental per-component walk (each
    component opened dir_fd-relative with O_NOFOLLOW), so a symlinked
    component — intermediate or final — is refused at the open site rather
    than followed: the pin can never capture a different inode than this
    resolved-path check saw. download_verified additionally rejects any
    symlinked directory between the root and a confined parent up front
    (see _assert_no_symlinked_dirs_under_root), naming the condition early
    with a ValueError before any fd is opened. A symlinked leaf is refused
    the same way (O_NOFOLLOW at open time). So under confinement, symlinks
    are rejected at every level.

    Race closure (F-F): this resolve() is validation only; the *open* side of
    the race is closed two ways. A symlink swapped in at a leaf path
    (staging/manifest/tmp-sibling) after this check is refused at open time
    by O_NOFOLLOW (see _read_body / _record_identity). A symlink swapped in
    at a PARENT directory (or any intermediate under the root) after this
    check cannot redirect a write or a metadata operation either:
    download_verified pins each parent directory to an fd before the first
    write, by an incremental per-component O_NOFOLLOW|O_DIRECTORY walk from
    the root (see _open_confined_parent_dirs), and the confined leaf-opens,
    stats, unlinks and manifest renames all resolve against that fd (see
    _open_confined_leaf / _confined_stat / _confined_unlink), so they land on
    the validated directory inode and a swapped directory fails the open
    instead of escaping the root.
    """
    root = download_root.resolve()
    for path in paths:
        resolved = path.resolve()
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise ValueError(
                f"Path {path} escapes download root {download_root}"
            ) from exc


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

    Identity-conflict restarts are bounded per pass: a 206 whose identity
    contradicts the HEAD identity discards the partial and restarts, up to
    :data:`_MAX_IDENTITY_CONFLICT_RESTARTS` times per pass; beyond that an
    :class:`IdentityConflictError` propagates so the caller consumes a retry
    attempt instead of restarting forever. A conflicting full 200 (which only
    reaches this check with an empty partial) is adopted and consumed as-is,
    not restarted.

    Where a pass restarts (range-not-honored, identity conflict, or a
    mid-append staging change) the already-open streamed response is closed
    WITHOUT draining: an unread streamed response closes the underlying
    connection rather than returning it dirty to the pool, so there is nothing
    to drain (draining a full 200 would fetch the whole object for no reason).
    """
    conflict_restarts = 0
    while True:
        # Both checks resolve through the pinned parent fd when confined,
        # so a parent symlink swap cannot make the size read and the
        # existence check disagree (or point at the swapped target).
        partial_size = (
            _confined_stat(staging_path).st_size
            if _confined_is_file(staging_path)
            else 0
        )
        recorded = _verified_identity(manifest_path)
        if _resume_decision(recorded, current, partial_size, expected_size):
            logger.warning(
                "Discarding %s: source identity not reconciled", staging_path
            )
            _confined_unlink(staging_path)
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
                # Close without draining (see docstring); then restart.
                response.close()
                _confined_unlink(staging_path)
                continue
            if action == "fail":
                _confined_unlink(staging_path)
                return resumed, fail_class
            if action == "raise":
                response.raise_for_status()
            if action == "read" and _get_identity_conflicts(response, current):
                # The resume decision was based on the HEAD identity, but the
                # GET may serve an object replaced in between (same length,
                # different ETag/Last-Modified). Two cases:
                #   * A conflicting full 200 can only reach this check with an
                #     empty partial (the only case where a 200 is a "read"),
                #     so it is written fresh with "wb". Adopt the served
                #     identity and consume the body as-is: it is a complete
                #     representation of the served object, and the final
                #     byte-count check plus adopted identity keep the manifest
                #     consistent.
                #   * A conflicting 206 would be appended to the old partial,
                #     mixing content while the final byte-count check still
                #     passes. Discard the partial and restart from zero; adopt
                #     the served identity so the retry does not conflict
                #     against the stale HEAD identity forever. Restart budget
                #     is bounded per pass (see _MAX_IDENTITY_CONFLICT_RESTARTS).
                _adopt_identity(current, response)
                if response.status_code == 200:
                    logger.warning(
                        "GET identity conflicts with HEAD identity on full 200; "
                        "adopting served identity and consuming as-is",
                    )
                else:
                    conflict_restarts += 1
                    if conflict_restarts > _MAX_IDENTITY_CONFLICT_RESTARTS:
                        response.close()
                        raise IdentityConflictError(
                            "Identity conflicts persist past the restart "
                            f"budget of {_MAX_IDENTITY_CONFLICT_RESTARTS}"
                        )
                    logger.warning(
                        "GET identity conflicts with HEAD identity; restarting "
                        "(%d/%d)",
                        conflict_restarts,
                        _MAX_IDENTITY_CONFLICT_RESTARTS,
                    )
                    # Close without draining (see docstring); then restart.
                    response.close()
                    _confined_unlink(staging_path)
                    continue
            mode = "ab" if response.status_code == 206 else "wb"
            if mode == "ab":
                resumed = True
            try:
                _read_body(
                    response,
                    staging_path,
                    mode,
                    partial_size,
                    DEFAULT_CHUNK_SIZE,
                    progress,
                    expected_size,
                )
            except _StagingFileChangedError:
                # Another writer touched the partial after the resume
                # decision; the appended body would be corrupt. Discard
                # it and restart from byte zero in the next pass.
                logger.warning(
                    "Staging file %s modified before append; restarting",
                    staging_path,
                )
                # Close without draining (see docstring); then restart.
                response.close()
                _confined_unlink(staging_path)
                continue
        # The download is byte-complete but NOT yet verified: the final
        # size check runs in download_verified, which owns the complete
        # mark. Recording complete=True here would let a SizeMismatch
        # failure leave a manifest claiming completion it never earned.
        _record_identity(manifest_path, current, complete=False)
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
    download_root: Path | None = None,
    allow_local: bool = False,
) -> VerifiedDownload:
    """Perform a resumable, identity-verified staged download.

    Writes to ``staging_path`` (a .partial file) and never renames it to a
    final media path; callers perform that rename themselves. The current
    remote identity (Content-Length, ETag, Last-Modified) is recorded in an
    identity sidecar so resumptions can be validated.

    URL policy (SSRF guard): the initial ``url`` must be http(s) without
    userinfo, and every resolved address must be public. Local-network
    (loopback/private/link-local) destinations are rejected by default;
    pass ``allow_local=True`` only when the caller trusts the URL source
    (e.g. an operator intentionally downloading from a local media server).
    The pre-connect check is a fast pre-filter only: the primary SSRF
    barrier is a connection-time peer guard that rejects the ACTUAL TCP peer
    on every connect, so a rebinding resolver cannot bypass the policy.
    Every redirect hop is strictly public-only: a server that bounces the
    download to an internal address fails the download as a network error
    (URLPolicyError, a requests.RequestException).

    Path confinement: when ``download_root`` is given, ``staging_path``,
    the identity manifest, and the manifest's tmp sibling must all
    resolve inside it (ValueError before any mkdir/IO), and a symlink
    staging path is refused. When it is None, confinement is disabled
    and a warning is logged.

    Race window (accepted trade-off): the identity used for the resume
    decision is discovered via a HEAD request, and the body is fetched in
    a separate GET. If the object is *replaced between the HEAD and the
    GET* while keeping its length, a 206 for the new object would be
    appended to the old partial. That replacement is detected only when the
    GET carries an ETag/Last-Modified header that conflicts with the HEAD
    identity (the GET-header check in _verified_attempt forces a restart).
    A GET that omits identity headers entirely cannot be distinguished from
    a legitimate same-length re-fetch, so it cannot be detected; this is
    the accepted residual risk of the HEAD-then-GET protocol.

    Args:
        url: The URL to download from.
        staging_path: The .partial target to write to.
        expected_size: Expected final byte count, or None when unknown.
        timeout: Timeout/retry policy (defaults to TimeoutPolicy()).
        progress: Optional callback(downloaded_total, expected_or_-1).
        session: Optional requests.Session (creates one if None). Passing a
            caller session in opts it into the redirect hook and the
            connection-time peer guard as well.
        identity_manifest_path: Sidecar JSON path (defaults to
            staging_path + '.identity.json').
        download_root: When given, staging/manifest paths must resolve
            under it and symlinks at the staging path are refused;
            None disables confinement (warns).
        allow_local: When False (default), local-network (loopback/
            private/link-local) initial URLs are rejected. Enable only when
            the caller trusts the URL source.

    Returns:
        A structured VerifiedDownload outcome; errors never escape.
        ``bytes_downloaded`` is the total bytes present in the final
        staging file — NOT the bytes transferred by this call: on a
        resumed download it includes bytes written by prior calls.
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
    if download_root is not None:
        _enforce_confinement(
            download_root,
            staging_path,
            manifest_path,
            manifest_path.with_suffix(manifest_path.suffix + ".tmp"),
        )
        if staging_path.is_symlink():
            raise ValueError(f"Staging path {staging_path} is a symlink")
        # Strict symlink-parent policy: any symlinked directory between the
        # root and a confined parent is rejected up front, before any fd is
        # opened. The O_NOFOLLOW parent pinning enforces the same policy at
        # open time with an incremental per-component walk (refusing a
        # symlink at every level, ELOOP), so this up-front check is
        # defense in depth — but it gives the same condition a named error
        # early and keeps _enforce_confinement and the pinning consistent.
        _assert_no_symlinked_dirs_under_root(
            download_root, staging_path.parent, manifest_path.parent
        )
    else:
        logger.warning(
            "download_verified without download_root; path confinement "
            "disabled for %s",
            staging_path,
        )
    staging_path.parent.mkdir(parents=True, exist_ok=True)

    current: dict | None = None
    last_error_class: str | None = None
    last_error_message: str | None = None
    assert session is not None  # narrowed by own_session branch above
    pinned_dirs: dict[Path, int] | None = None
    confined_token = None
    # Connection-time peer guard: the allowed-local set is the initial
    # hostname only (when local access was opted in), so a rebinding resolver
    # cannot turn a hostname into an internal peer.
    initial_hostname = urlparse(url).hostname
    allowed_local = (
        frozenset({initial_hostname.lower()})
        if allow_local and initial_hostname
        else frozenset()
    )
    try:
        # The pinned parent-directory fds are acquired INSIDE this
        # try/finally: if adapter installation or anything else below raises
        # before the main body, the finally still closes every opened fd and
        # resets the contextvar, so no fd leaks and _confined_dirs is never
        # left set for a caller context. Confined leaf-opens are bound to
        # these fds so a parent-directory symlink swap after
        # _enforce_confinement cannot redirect a write outside the root. The
        # staging parent and the manifest parent are both pinned (they are
        # the same directory unless a custom identity_manifest_path places
        # the manifest elsewhere), so every leaf write resolves against a
        # real directory inode.
        if download_root is not None:
            pinned_dirs = _open_confined_parent_dirs(
                download_root, staging_path.parent, manifest_path.parent
            )
            confined_token = _confined_dirs.set(pinned_dirs)
        _install_peer_check(session, allowed_local)
        _install_url_policy(session, allow_local=allow_local)
        # The initial URL is operator-chosen; whether its loopback/private/
        # link-local targets are permitted is the caller's allow_local
        # decision. Redirect hops are validated strictly by the session hook
        # and the connection-time peer guard instead. A URL that fails here
        # fails identically on every retry, so report the structured outcome
        # immediately instead of burning the budget.
        try:
            _assert_public_url(url, allow_local=allow_local)
        except URLPolicyError as exc:
            logger.warning("Initial URL rejected by policy for %s: %s", url, exc)
            return _verified_failure(
                expected_size,
                None,
                "URLPolicyError",
                str(exc),
            )
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
            except (requests.RequestException, OSError, ValueError) as exc:
                last_error_class = type(exc).__name__
                last_error_message = str(exc)
                logger.warning(
                    "Attempt %d/%d failed for %s: %s: %s",
                    attempt + 1,
                    policy.retry_attempts,
                    url,
                    last_error_class,
                    last_error_message,
                )
                continue
            if fail_class is not None:
                return _verified_failure(
                    expected_size,
                    current,
                    fail_class,
                    "HTTP 416: partial not resolvable",
                )
            final_size = _stat_size(staging_path)
            # When the caller did not pin an expected size, verify against
            # the length the HEAD request discovered; only skip the check
            # when both are unknown.
            target = (
                expected_size
                if expected_size is not None
                else current.get("content_length")
            )
            if target is not None and final_size != target:
                return _verified_failure(
                    expected_size,
                    current,
                    "SizeMismatch",
                    f"final size {final_size} != expected {target}",
                    ok_bytes=final_size,
                    final_path=staging_path,
                    resumed=resumed,
                )
            # Verified: only now may the manifest claim completion. The
            # verified bytes are complete and correct in the staging file;
            # only the manifest write is left, and a failure there (OSError)
            # must not escape — report a structured failure carrying the
            # verified bytes. The staging file remains valid for caller-side
            # finalization.
            try:
                _record_identity(manifest_path, current, complete=True)
            except OSError as exc:
                logger.warning(
                    "Failed to record complete identity for %s: %s: %s",
                    manifest_path,
                    type(exc).__name__,
                    exc,
                )
                return _verified_failure(
                    expected_size,
                    current,
                    type(exc).__name__,
                    str(exc),
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
        # Release the pinned parent-directory fds and reset the confinement
        # contextvar (both no-ops when confinement was disabled). Done before
        # the session close so no leaf write can outlive the pin.
        if pinned_dirs is not None:
            for fd in pinned_dirs.values():
                os.close(fd)
        if confined_token is not None:
            _confined_dirs.reset(confined_token)
        if own_session and session is not None:
            session.close()
    final_path = staging_path if staging_path.is_file() else None
    # Retries are exhausted: report the exhaustion, carrying the last
    # attempt's concrete error for diagnostics.
    last_error = (
        f"last error: {last_error_class}: {last_error_message}"
        if last_error_class is not None
        else "all retry attempts failed"
    )
    return _verified_failure(
        expected_size,
        current,
        "RetriesExhausted",
        last_error,
        ok_bytes=_stat_size(final_path) if final_path is not None else 0,
        final_path=final_path,
    )


def _stat_size(path: Path) -> int:
    """Staging file size, or 0 when it is gone or unstatable.

    Final verification and failure reporting run outside the per-attempt
    error guard; a stat failure there must not violate "errors never
    escape". When confinement is still active (the ContextVar is reset in
    ``download_verified``'s finally only after the exhaustion report), the
    stat resolves through the pinned parent fd like every other confined
    leaf operation; otherwise it is a plain ``path.stat()``.
    """
    try:
        return _confined_stat(path).st_size
    except OSError:
        return 0


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
            # Verify size match via HEAD request on a fresh session carrying
            # the SAME SSRF guards as the GET path below: a module-level
            # requests.head() would bypass the peer guard and URL policy
            # (loopback/private reachable), so this HEAD must go through the
            # guarded session or the skip check itself is an SSRF hole.
            head_session = requests.Session()
            _install_peer_check(head_session, frozenset())
            _install_url_policy(head_session)
            try:
                # Verify size match via HEAD request
                head_response = head_session.head(
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
            finally:
                head_session.close()

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
                # SSRF guards (strict: no local addresses); the
                # connection-time peer guard is the primary barrier.
                _install_peer_check(session, frozenset())
                _install_url_policy(session)
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

            # Wrap progress callback to include filename. Capture in a local
            # so the closure can be narrowed (mypy can't narrow ``self.x``
            # inside a nested function).
            progress_cb = self.progress_callback
            wrapped_callback: Callable[[int, int], None] | None = None
            if progress_cb is not None:

                def wrapped(bytes_done: int, total: int) -> None:
                    progress_cb(local_path.name, bytes_done, total)

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

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
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

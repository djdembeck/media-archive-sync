"""Tests for download_verified: identity-aware, resumable staged downloads.

Uses a local ThreadingHTTPServer (no external network) that can be configured
per test to exercise the full range of resume / restart / verify behavior.
"""

import errno
import http.server
import json
import logging
import os
import socket
import threading
import time
from contextlib import suppress
from pathlib import Path

import pytest
import requests
import urllib3.connection
import urllib3.connectionpool
from requests.adapters import HTTPAdapter

import media_archive_sync.downloader as downloader
from media_archive_sync import (
    TimeoutPolicy,
    VerifiedDownload,
    download_file,
    download_files,
    download_verified,
)
from media_archive_sync.downloader import (
    URLPolicyError,
    _check_peer,
    _install_peer_check,
    _peer_checked_connection,
    _PeerCheckedHTTPAdapter,
    _read_body,
    _record_identity,
    _StagingFileChangedError,
)

# 1000-byte deterministic body.
BODY_1000 = bytes(range(256)) * 4
assert len(BODY_1000) == 1024
BODY_1000 = BODY_1000[:1000]
LM = "Wed, 07 Sep 2026 00:00:00 GMT"
# Bounded wait for the serve thread to exit after shutdown.
_SERVER_JOIN_TIMEOUT_SECONDS = 5


class _BaseHandler(http.server.BaseHTTPRequestHandler):
    # HTTP/1.0 closes each connection after the response, which keeps abort
    # (mid-body close) deterministic and avoids pooled-connection reuse.
    protocol_version = "HTTP/1.0"

    # Per-instance config is supplied via a fresh subclass created in
    # LocalHttp.__init__; all handlers read it off type(self).
    _content = b""
    _etag = '"v1"'
    _last_modified = LM
    _ignore_range = False
    _wrong_range_start = False
    _wrong_range_total = False
    _abort_gets = 0
    _abort_bytes = 0
    _status = 200
    _broken_get_content_length = False
    _redirect_to = ""
    _head_no_identity = False
    _head_content_length = None
    _range_not_satisfiable = False
    _get_no_identity = False
    _get_count = 0
    _lock = threading.Lock()
    _rotating_206_etag = False
    _always_206 = False

    def log_message(self, *_args):
        pass

    @classmethod
    def _get_tick(cls):
        with cls._lock:
            cls._get_count += 1
            return cls._get_count

    def _should_abort(self):
        cls = type(self)
        return cls._get_count <= cls._abort_gets and cls._abort_bytes < len(
            cls._content
        )

    def _abort_connection(self):
        """Hard-drop the connection so the client sees EOF mid-body.

        A plain socket close under HTTP/1.1 can leave the client blocked in
        recv until the read timeout; a shutdown(SHUT_RDWR) sends FIN first.
        """
        with suppress(OSError):
            self.wfile.flush()
        with suppress(OSError):
            self.connection.shutdown(socket.SHUT_RDWR)
        with suppress(OSError):
            self.connection.close()

    def _error(self):
        cls = type(self)
        self.send_response(cls._status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _maybe_redirect(self) -> bool:
        location = type(self)._redirect_to
        if not location:
            return False
        self.send_response(302)
        self.send_header("Location", location)
        self.end_headers()
        return True

    def do_HEAD(self):
        if self._maybe_redirect():
            return
        if type(self)._status >= 400:
            self._error()
            return
        content = type(self)._content
        self.send_response(200)
        head_length = type(self)._head_content_length
        self.send_header(
            "Content-Length",
            head_length if head_length is not None else str(len(content)),
        )
        if not type(self)._head_no_identity:
            head_etag = type(self)._head_etag
            head_lm = type(self)._head_last_modified
            self.send_header("ETag", head_etag or type(self)._etag)
            self.send_header("Last-Modified", head_lm or type(self)._last_modified)
        self.end_headers()

    def do_GET(self):
        self._get_tick()
        if type(self)._status >= 400:
            self._error()
            return
        cls = type(self)
        content = cls._content
        total = len(content)
        abort = self._should_abort()
        abort_bytes = min(cls._abort_bytes, total - 1) if abort else total

        range_hdr = self.headers.get("Range")
        if range_hdr and cls._range_not_satisfiable:
            self.send_response(416)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        # F-D: always_206 answers every GET as a 206 (from a Range header, or
        # from zero when absent) so the 206 conflict-restart branch is the one
        # under test; combined with rotating_206_etag it models a rebinding
        # server that rotates identity on every ranged response.
        effective_range = range_hdr
        if effective_range is None and cls._always_206:
            effective_range = "bytes=0-"
        if effective_range and not cls._ignore_range:
            start = int(effective_range.split("=", 1)[1].split("-")[0])
            body = content[start:]
            reported_start = start if not cls._wrong_range_start else max(0, start - 1)
            reported_total = total if not cls._wrong_range_total else total + 1
            self.send_response(206)
            self.send_header(
                "Content-Range",
                f"bytes {reported_start}-{reported_start + len(body) - 1}/{reported_total}",
            )
            self.send_header("Content-Length", str(len(body)))
            if not cls._get_no_identity:
                # F-D: a server rotating its ETag on every 206 forces a
                # conflict-restart on every GET. Only meaningful with the
                # rotating_206_etag toggle; reuses the GET-tick counter.
                etag = f'"r{cls._get_count}"' if cls._rotating_206_etag else cls._etag
                self.send_header("ETag", etag)
                self.send_header("Last-Modified", cls._last_modified)
            self.end_headers()
            self.wfile.write(body[:abort_bytes])
            self.wfile.flush()
            if abort:
                self._abort_connection()
                return
            self.wfile.write(body[abort_bytes:])
            return

        self.send_response(200)
        declared = str(total) if not cls._broken_get_content_length else "not-a-number"
        self.send_header("Content-Length", declared)
        self.send_header("ETag", cls._etag)
        self.send_header("Last-Modified", cls._last_modified)
        self.end_headers()
        self.wfile.write(content[:abort_bytes])
        self.wfile.flush()
        if abort:
            self.connection.close()
            return
        self.wfile.write(content[abort_bytes:])


class LocalHttp:
    """A local HTTP server with per-test behavior toggles.

    A fresh handler subclass is created for each instance, so class-attribute
    state never leaks between tests.
    """

    def __init__(
        self,
        content=BODY_1000,
        etag='"v1"',
        last_modified=LM,
        head_etag=None,
        head_last_modified=None,
        **behavior,
    ):
        overrides = {f"_{key}": value for key, value in behavior.items()}
        self.handler = type(
            "Handler",
            (_BaseHandler,),
            {
                "_content": content,
                "_etag": etag,
                "_last_modified": last_modified,
                "_head_etag": head_etag,
                "_head_last_modified": head_last_modified,
                **overrides,
            },
        )
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), self.handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def url(self, path="/media.bin"):
        return f"http://127.0.0.1:{self.port}{path}"

    def flip_etag(self, value):
        self.handler._etag = value

    def set_redirect(self, location):
        self.handler._redirect_to = location

    def change_content(self, content):
        self.handler._content = content


@pytest.fixture
def local_http_server(request):
    """Factory yielding a started LocalHttp whose teardown is guaranteed.

    Calling it returns a context manager that owns the server lifecycle:
    on exit (or at test teardown, via ``addfinalizer``) it calls
    ``shutdown()`` + ``server_close()`` and joins the serve thread with a
    timeout, so no test needs its own try/finally and the daemon thread
    can never outlive the test.
    """

    def _make(**behavior):
        guard = _LiveServer(LocalHttp(**behavior))
        request.addfinalizer(guard.stop)
        return guard

    return _make


class _LiveServer:
    """Guarantees LocalHttp teardown: ``stop()`` runs at most once (F14)."""

    def __init__(self, srv: LocalHttp) -> None:
        self.srv = srv
        self._stopped = False

    def stop(self) -> None:
        """Shutdown, close the socket, and join the serve thread (bounded)."""
        if self._stopped:
            return
        self._stopped = True
        self.srv.server.shutdown()
        self.srv.server.server_close()
        self.srv.thread.join(timeout=_SERVER_JOIN_TIMEOUT_SECONDS)
        assert not self.srv.thread.is_alive()

    def __enter__(self):
        return self.srv

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False


@pytest.fixture
def fast_policy():
    return TimeoutPolicy(
        connect_seconds=5, read_seconds=30, retry_attempts=3, backoff_seconds=0.01
    )


def _seed_partial(
    staging: Path,
    sidecar: Path,
    content: bytes,
    offset: int,
    *,
    length,
    etag,
    complete=False,
):
    """Write a pre-existing partial and a sidecar to simulate a prior run."""
    staging.parent.mkdir(parents=True, exist_ok=True)
    staging.write_bytes(content[:offset])
    sidecar.write_text(
        json.dumps(
            {
                "content_length": length,
                "etag": etag,
                "last_modified": LM,
                "complete": complete,
            }
        )
    )


def _manifest(staging: Path) -> dict:
    return json.loads(staging.with_name(staging.name + ".identity.json").read_text())


def test_new_download_success_and_identity_persisted(
    tmp_path, fast_policy, local_http_server
):
    with local_http_server() as srv:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert isinstance(result, VerifiedDownload)
        assert result.resumed is False
        assert result.error_class is None
        assert result.error_message is None
        assert result.bytes_downloaded == 1000
        assert result.final_path == staging
        assert result.expected_size == 1000
        # Exact byte count on disk.
        assert staging.is_file()
        assert staging.stat().st_size == 1000
        assert staging.read_bytes() == BODY_1000
        # Identity sidecar persisted with the HEAD identity.
        manifest = _manifest(staging)
        assert manifest["content_length"] == 1000
        assert manifest["etag"] == '"v1"'
        assert manifest["last_modified"] == LM
        assert manifest["complete"] is True
        # Remote identity surfaced in the outcome.
        assert result.remote_identity["content_length"] == 1000
        assert result.remote_identity["etag"] == '"v1"'


def test_range_resume_appends_and_verifies(tmp_path, fast_policy, local_http_server):
    with local_http_server() as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')
        progress = []
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            progress=lambda d, t: progress.append((d, t)),
            allow_local=True,
        )
        assert result.ok
        assert result.resumed is True
        assert result.bytes_downloaded == 1000
        assert result.final_path == staging
        # Appended to the 400-byte partial; final content is the full body.
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000
        # progress reported the total against the expected size.
        assert progress and progress[-1] == (1000, 1000)
        assert _manifest(staging)["complete"] is True


def test_server_ignores_range_restarts_from_zero(
    tmp_path, fast_policy, local_http_server
):
    with local_http_server(ignore_range=True) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        # Range was ignored (200), so it restarted from byte zero.
        assert result.resumed is False
        assert result.bytes_downloaded == 1000
        # Final content is the complete object, not a corrupted 400+append.
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000


def test_incorrect_content_range_start_restarts(
    tmp_path, fast_policy, local_http_server
):
    with local_http_server(wrong_range_start=True) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert result.resumed is False
        assert result.bytes_downloaded == 1000
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000


def test_wrong_content_range_total_restarts(tmp_path, fast_policy, local_http_server):
    with local_http_server(wrong_range_total=True) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert result.resumed is False
        assert result.bytes_downloaded == 1000
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000


def test_remote_size_changed_discards_partial(tmp_path, fast_policy, local_http_server):
    # The remote now serves a 1500-byte object; the sidecar recorded 1000.
    new_content = bytes((i % 256) for i in range(1500))
    with local_http_server(content=new_content, etag='"v1"') as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        old_partial = b"A" * 400  # 400 bytes of the *old* object
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(old_partial)
        sidecar.write_text(
            json.dumps(
                {
                    "content_length": 1000,
                    "etag": '"v1"',
                    "last_modified": LM,
                    "complete": False,
                }
            )
        )
        result = download_verified(
            srv.url(),
            staging,
            expected_size=None,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        # The 400-byte old partial was discarded; re-downloaded from zero.
        assert result.resumed is False
        assert result.bytes_downloaded == 1500
        assert staging.read_bytes() == new_content
        # The old 400-byte prefix was not retained from the discarded partial.
        assert staging.read_bytes()[:400] != old_partial
        assert staging.stat().st_size == 1500


def test_etag_flip_discards_partial(tmp_path, fast_policy, local_http_server):
    with local_http_server(etag='"v1"') as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')
        # The remote's identity has since flipped to a new ETag.
        srv.flip_etag('"v2"')
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        # ETag changed from the recorded value: partial discarded, restarted.
        assert result.resumed is False
        assert result.bytes_downloaded == 1000
        assert result.remote_identity["etag"] == '"v2"'
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000
        # Sidecar updated to the new identity.
        assert _manifest(staging)["etag"] == '"v2"'


def test_expected_size_mismatch_reports_sizemismatch(
    tmp_path, fast_policy, local_http_server
):
    # Server only has 800 bytes but the caller expects 1000.
    short = BODY_1000[:800]
    with local_http_server(content=short) as srv:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok is False
        assert result.error_class == "SizeMismatch"
        assert result.bytes_downloaded == 800
        assert result.final_path == staging
        # The partial is left in place at its actual size.
        assert staging.stat().st_size == 800
        assert staging.read_bytes() == short


def test_staged_partial_never_renamed(tmp_path, fast_policy, local_http_server):
    with local_http_server() as srv:
        staging = tmp_path / "clip.mkv.partial"
        final_name_candidate = staging.with_name(staging.name.replace(".partial", ""))
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        # The .partial file is the final path; no rename to a media path.
        assert result.final_path == staging
        assert staging.is_file()
        assert not final_name_candidate.exists()
        assert final_name_candidate.name == "clip.mkv"
        assert result.final_path.name.endswith(".partial")


def test_identity_manifest_default_and_custom_location(
    tmp_path, fast_policy, local_http_server
):
    with local_http_server() as srv:
        # Default location: <staging>.identity.json
        default_staging = tmp_path / "a.mp4.partial"
        download_verified(
            srv.url(),
            default_staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert (default_staging.with_name("a.mp4.partial.identity.json")).is_file()

        # Custom location via identity_manifest_path.
        custom = tmp_path / "custom-manifest.json"
        custom_staging = tmp_path / "b.mp4.partial"
        result = download_verified(
            srv.url(),
            custom_staging,
            expected_size=1000,
            timeout=fast_policy,
            identity_manifest_path=custom,
            allow_local=True,
        )
        assert result.ok
        assert custom.is_file()
        assert json.loads(custom.read_text())["content_length"] == 1000
        # The default-style sidecar was NOT written next to the custom staging.
        assert not custom_staging.with_name("b.mp4.partial.identity.json").is_file()


def test_mid_body_abort_retries_with_backoff_and_resumes(
    tmp_path, fast_policy, local_http_server
):
    # Seed a 400-byte partial; the first 206 resume aborts mid-body (after
    # 100 bytes), and the retry revalidates identity and resumes from the
    # new offset, completing the download.
    with local_http_server(abort_gets=1, abort_bytes=100) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')
        progress: list[tuple[int, int]] = []
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            progress=lambda d, t: progress.append((d, t)),
            allow_local=True,
        )
        assert result.ok
        # The final read appended to the (extended) partial: a resume.
        assert result.resumed is True
        assert result.bytes_downloaded == 1000
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000
        # F16: progress totals are monotonically non-decreasing. After the
        # mid-body abort, urllib3 raises IncompleteRead before the partial
        # chunk reaches the caller, so the aborted attempt emits no callback
        # and the staging file is still the seeded 400-byte partial. The
        # resumed attempt reports against that offset: every total is at
        # least the partial offset (no double-count of staged bytes) and the
        # final tuple is (1000, 1000).
        assert progress, "progress callback never fired"
        totals = [d for d, _ in progress]
        assert all(a <= b for a, b in zip(totals, totals[1:], strict=False))
        assert all(d >= 400 for d in totals)
        assert all(d <= 1000 for d in totals)
        assert progress[-1] == (1000, 1000)


def test_head_failure_reports_retries_exhausted_without_raising(
    tmp_path, fast_policy, local_http_server
):
    # Every HEAD/GET returns 500: retries are exhausted, no exception escapes.
    with local_http_server(status=500) as srv:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok is False
        # F6: exhaustion reports the sentinel class, carrying the last
        # attempt's concrete error for diagnostics.
        assert result.error_class == "RetriesExhausted"
        assert result.error_message is not None
        assert result.error_message.startswith("last error: ")
        assert result.final_path is None or result.final_path == staging
        assert not (staging.with_name("media.mp4.partial.identity.json")).is_file()


# --- Regression tests for verified-download edge cases ---------------------


def test_get_identity_mismatch_restarts_and_stays_correct(
    tmp_path, fast_policy, local_http_server
):
    # F1: HEAD reports the old identity (etag "v1", 1000 bytes); a 400-byte
    # partial is staged for it. The object is then replaced with a new 1000-byte
    # object (etag "v2") that keeps the same length. The 206 resume carries the
    # NEW object's ETag, conflicting with the HEAD identity: the download must
    # restart from zero and yield the complete new content, never a mix.
    new_content = bytes((255 - (i % 256)) for i in range(1000))
    with local_http_server(
        content=new_content,
        etag='"v2"',
        head_etag='"v1"',
        head_last_modified=LM,
    ) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(
            staging, sidecar, new_content, offset=400, length=1000, etag='"v1"'
        )
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        # The 206 carried the new ETag: partial discarded, restarted from zero.
        assert result.resumed is False
        assert result.bytes_downloaded == 1000
        # Full new content, not a 400-byte old + 600-byte new mix.
        assert staging.read_bytes() == new_content
        assert staging.stat().st_size == 1000
        # The adopted identity is the served one.
        assert result.remote_identity["etag"] == '"v2"'
        assert _manifest(staging)["complete"] is True


def test_resume_without_persisted_validator_restarts(
    tmp_path, fast_policy, local_http_server
):
    # F2: no ETag/Last-Modified was persisted (or is currently available), so
    # length alone must not be trusted. A 400-byte partial with an untrusted
    # identity must be discarded and re-downloaded from zero.
    with local_http_server(head_no_identity=True) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(b"OLD" * 133 + b"OLD"[:1])  # 400 junk bytes
        sidecar.write_text(
            json.dumps(
                {
                    "content_length": 1000,
                    "etag": None,
                    "last_modified": None,
                    "complete": False,
                }
            )
        )
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert result.resumed is False
        assert result.bytes_downloaded == 1000
        # The untrusted junk prefix was dropped; final content is the full body.
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000


def test_complete_manifest_with_truncated_partial_discards(
    tmp_path, fast_policy, local_http_server
):
    # F3a: a manifest claims complete=True but the staging file is truncated
    # (500 of 1000 bytes). Resume must be refused: the partial is discarded and
    # the full object re-downloaded, and the final content is correct.
    with local_http_server() as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(
            staging,
            sidecar,
            BODY_1000,
            offset=500,
            length=1000,
            etag='"v1"',
            complete=True,
        )
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert result.resumed is False
        assert result.bytes_downloaded == 1000
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000
        assert _manifest(staging)["complete"] is True


def test_size_mismatch_failure_leaves_manifest_incomplete(
    tmp_path, fast_policy, local_http_server
):
    # F3b: a SizeMismatch failure (remote 800 bytes, expected 1000) must not
    # leave a manifest claiming completion it never earned.
    short = BODY_1000[:800]
    with local_http_server(content=short) as srv:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok is False
        assert result.error_class == "SizeMismatch"
        assert staging.stat().st_size == 800
        # The manifest exists (written during the attempt) but does not
        # claim completion.
        manifest = _manifest(staging)
        assert manifest["complete"] is False


def test_non_numeric_content_length_fails_structurally(
    tmp_path, fast_policy, local_http_server
):
    # F5: a non-numeric Content-Length on the full-body GET must not escape as
    # a ValueError. The per-attempt guard (ValueError now caught) retries, and
    # exhaustion reports a structured failure, not a raised exception.
    with local_http_server(broken_get_content_length=True) as srv:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=None,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok is False
        # Structured, not raised: a non-None error class is reported.
        assert result.error_class is not None
        assert result.error_message is not None


def test_redirect_to_loopback_rejected(tmp_path, fast_policy, local_http_server):
    # F11: the initial (operator-chosen) URL may be loopback, but a redirect
    # target to 127.0.0.1 is a policy violation. The server 302s HEAD to a
    # loopback location; the hook raises URLPolicyError, which surfaces as a
    # structured failure after retries, never an escaped exception.
    with local_http_server() as srv:
        # Redirect every HEAD to a loopback URL (a second, arbitrary local port).
        srv.set_redirect(f"http://127.0.0.1:{srv.port}/elsewhere")
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok is False
        assert result.error_class is not None
        assert result.error_message is not None
        # The policy violation (loopback redirect target) is the root cause.
        assert "127.0.0.1" in (result.error_message or "") or "public" in (
            result.error_message or ""
        )


def test_path_outside_download_root_rejected(tmp_path, fast_policy):
    # F12: with download_root set, a staging path that resolves outside it must
    # raise ValueError before any mkdir/IO, and nothing may be written outside
    # the root.
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside" / "media.mp4.partial"
    outside_dir = tmp_path / "outside"
    with pytest.raises(ValueError):
        download_verified(
            "http://127.0.0.1:1/media.bin",
            outside,
            expected_size=1000,
            timeout=fast_policy,
            download_root=root,
        )
    # Nothing was written outside the root (dir was never created).
    assert not outside_dir.exists()
    # The root itself is empty: no manifest, no partial, no tmp sibling.
    assert list(root.iterdir()) == []


def test_confined_staging_directly_in_download_root_completes(
    tmp_path, fast_policy, local_http_server
):
    # P1: when the staging file sits DIRECTLY in download_root, the pinned
    # parent IS the root — _open_confined_parent_dirs hands the root's own
    # fd over as that parent's pin, so the helper must not close it in its
    # finally (the caller closes every returned fd). The pre-fix helper
    # closed root_fd unconditionally, so the confined leaf-opens
    # (os.open(dir_fd=<closed root fd>)) raised EBADF and it escaped
    # download_verified, violating the "errors never escape" contract.
    # This layout must complete ok with the staging file and the default
    # manifest inside the root.
    root = tmp_path / "root"
    root.mkdir()
    staging = root / "media.mp4.partial"
    with local_http_server() as srv:
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
            download_root=root,
        )
    assert result.ok
    assert result.resumed is False
    assert result.error_class is None
    assert result.error_message is None
    assert result.bytes_downloaded == 1000
    assert result.final_path == staging
    # The staging file and its default identity manifest are both inside
    # the root and complete.
    assert staging.is_file()
    assert staging.read_bytes() == BODY_1000
    manifest = _manifest(staging)
    assert manifest["complete"] is True
    assert manifest["content_length"] == 1000
    # Only the staging file and its manifest live in the root.
    assert {p.name for p in root.iterdir()} == {
        "media.mp4.partial",
        "media.mp4.partial.identity.json",
    }


# --- Gap-filling tests: identity-verified resume/restart/size verification ---


def test_range_416_reports_range_not_satisfiable(
    tmp_path, fast_policy, local_http_server
):
    # The 1200-byte partial is valid against the HEAD identity (the server
    # declares a 1500-byte object; the sidecar matches it), but the real
    # body is only the default 1000 bytes, so the range 1200- is
    # unsatisfiable and the server answers 416. The download must report a
    # structured RangeNotSatisfiable failure, never an escaped exception.
    # (A 1200-byte partial recorded against the real 1000-byte length would
    # be discarded client-side by _validate_resume before any GET.)
    with local_http_server(range_not_satisfiable=True, head_content_length=1500) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(BODY_1000 + b"X" * 200)
        sidecar.write_text(
            json.dumps(
                {
                    "content_length": 1500,
                    "etag": '"v1"',
                    "last_modified": LM,
                    "complete": False,
                }
            )
        )
        result = download_verified(
            srv.url(),
            staging,
            expected_size=None,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok is False
        assert result.error_class == "RangeNotSatisfiable"
        assert result.error_message is not None


def test_initial_url_policy_violations_fail_structured(
    tmp_path,
    fast_policy,
):
    # A bad initial URL (wrong scheme / userinfo / no hostname) fails
    # immediately as a structured URLPolicyError outcome; no server is
    # contacted, nothing raises. The no-hostname forms ('http:///media.bin',
    # 'http://') are rejected in the no-hostname branch, which PRECEDES
    # getaddrinfo entirely — so no CI-host network dependency.
    bad_urls = [
        "file:///etc/passwd",
        "ftp://127.0.0.1:8000/x",
        "http://user:pass@example.org/media.bin",
        "http:///media.bin",
        "http://",
    ]
    for url in bad_urls:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            url, staging, expected_size=None, timeout=fast_policy
        )
        assert result.ok is False, url
        assert result.error_class == "URLPolicyError", url
        assert result.error_message is not None, url
        assert result.final_path is None


def test_redirect_to_private_ip_rejected(tmp_path, fast_policy, local_http_server):
    # A redirect to a private (RFC1918) address is strictly rejected even
    # though the initial URL was an allowed loopback. The policy violation is
    # the root cause and must name the private address.
    with local_http_server() as srv:
        srv.set_redirect("http://192.168.1.1/x")
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok is False
        assert result.error_class is not None
        assert result.error_message is not None
        assert "192.168.1.1" in result.error_message


def test_remote_shrunk_to_partial_length_discards(
    tmp_path, fast_policy, local_http_server
):
    # The recorded remote length (1000) no longer matches: the remote shrank
    # to 600 bytes, exactly the partial's length. current_length <= partial
    # must discard the partial; resume is refused and the full 600-byte
    # object is downloaded from zero.
    short = BODY_1000[:600]
    with local_http_server(content=short) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, short, offset=600, length=1000, etag='"v1"')
        result = download_verified(
            srv.url(),
            staging,
            expected_size=None,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert result.resumed is False
        assert result.bytes_downloaded == 600
        assert staging.read_bytes() == short
        assert staging.stat().st_size == 600
        assert _manifest(staging)["complete"] is True


def test_recorded_validator_missing_from_current_head_discards(
    tmp_path, fast_policy, local_http_server
):
    # The sidecar records etag "v1", but the current HEAD carries no
    # identity at all. A missing validator is untrusted (not a conflict):
    # length alone never suffices, so the partial is discarded and the full
    # object is re-downloaded from zero.
    with local_http_server(head_no_identity=True) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert result.resumed is False
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000


def test_get_without_identity_headers_resumes(tmp_path, fast_policy, local_http_server):
    # The 206 body omits ETag/Last-Modified (servers may omit them on ranged
    # reads). A missing GET header must NOT count as an identity conflict:
    # the resume proceeds, appending to the matching partial to byte
    # completion.
    with local_http_server(get_no_identity=True) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert result.resumed is True
        assert result.bytes_downloaded == 1000
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000
        assert _manifest(staging)["complete"] is True


def test_staging_changed_mid_append_recovers(
    tmp_path, fast_policy, local_http_server, monkeypatch
):
    # The staging file is clobbered between the resume decision and the
    # append. The single-writer guard (_read_body) raises
    # _StagingFileChangedError; the attempt must discard the partial and
    # restart from zero in the same pass, completing correctly.
    with local_http_server() as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')

        original = downloader._read_body
        state = {"ab": 0}

        def _raising_read(
            response, staging_path, mode, base, chunk_size, progress, expected_size
        ):
            if mode == "ab" and state["ab"] == 0:
                state["ab"] += 1
                raise _StagingFileChangedError(
                    f"Staging file {staging_path} changed before append"
                )
            return original(
                response,
                staging_path,
                mode,
                base,
                chunk_size,
                progress,
                expected_size,
            )

        monkeypatch.setattr(downloader, "_read_body", _raising_read)
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        # First append was refused, then a clean restart from zero succeeded.
        assert state["ab"] == 1
        assert result.resumed is False
        assert result.bytes_downloaded == 1000
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000
        assert _manifest(staging)["complete"] is True


def test_caller_provided_session_honored(tmp_path, fast_policy, local_http_server):
    # A caller-supplied session is used for the download and, because it is
    # caller-owned, is NOT closed by download_verified and stays usable for
    # a follow-up request.
    with local_http_server() as srv:
        session = requests.Session()
        try:
            staging = tmp_path / "media.mp4.partial"
            result = download_verified(
                srv.url(),
                staging,
                expected_size=1000,
                timeout=fast_policy,
                session=session,
                allow_local=True,
            )
            assert result.ok
            assert result.resumed is False
            assert staging.read_bytes() == BODY_1000
            # Caller-owned: still open and usable after the download.
            follow_up = session.get(srv.url(), timeout=(5, 30))
            assert follow_up.status_code == 200
            assert follow_up.content == BODY_1000
        finally:
            session.close()


def test_head_non_numeric_content_length_falls_back(
    tmp_path, fast_policy, local_http_server
):
    # A non-numeric Content-Length on the HEAD must not escape as a
    # ValueError; _head_identity falls back to a None length. With
    # expected_size=None the final check is skipped (both lengths unknown)
    # and the download completes as a structured success.
    with local_http_server(head_content_length="not-a-number") as srv:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=None,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert result.resumed is False
        assert result.bytes_downloaded == 1000
        assert result.remote_identity is not None
        assert result.remote_identity["content_length"] is None
        assert staging.read_bytes() == BODY_1000
        assert _manifest(staging)["content_length"] is None


def test_corrupt_identity_sidecar_treated_as_missing(
    tmp_path, fast_policy, local_http_server
):
    # A sidecar that is not valid JSON (row 1) or not a JSON object (row 2)
    # is treated as missing: the partial is discarded and the full object is
    # re-downloaded from zero, completing successfully.
    corrupt_payloads = [
        "this is not valid json {",
        '"just a string"',
    ]
    for payload in corrupt_payloads:
        with local_http_server() as srv:
            staging = tmp_path / "media.mp4.partial"
            sidecar = staging.with_name(staging.name + ".identity.json")
            _seed_partial(
                staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"'
            )
            sidecar.write_text(payload)
            result = download_verified(
                srv.url(),
                staging,
                expected_size=1000,
                timeout=fast_policy,
                allow_local=True,
            )
            assert result.ok, payload
            assert result.resumed is False, payload
            assert result.bytes_downloaded == 1000, payload
            assert staging.read_bytes() == BODY_1000, payload
            # A fresh, valid sidecar was written by the successful download.
            assert _manifest(staging)["complete"] is True


# --- PR #9 follow-up regression tests (F-A..F-F) ---------------------------


def test_default_rejects_loopback_initial_url(tmp_path, fast_policy, local_http_server):
    # F-A: the default (allow_local=False) must reject a loopback INITIAL URL
    # with a structured URLPolicyError failure — the pre-connect check runs
    # before any request, so no retry is burned and nothing is written.
    with local_http_server() as srv:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
        )
        assert result.ok is False
        assert result.error_class == "URLPolicyError"
        assert result.error_message is not None
        # Nothing was created: the policy gate fails before any mkdir/IO.
        assert not staging.parent.joinpath(staging.name).exists()
        assert result.final_path is None
        assert result.bytes_downloaded == 0


def test_download_file_cli_path_rejects_loopback_structured(
    tmp_path, local_http_server, caplog
):
    # G1: the legacy CLI path (download_file with its own session) installs
    # the strict SSRF guards, so a loopback URL fails the (bool, int) contract
    # — no exception escapes and the policy rejection is logged.
    with local_http_server() as srv:
        local_path = tmp_path / "video.mp4"
        with caplog.at_level(logging.WARNING, logger="media_archive_sync.downloader"):
            result = download_file(srv.url(), local_path)
        assert result == (False, 0)
        assert not local_path.exists()
        assert not (local_path.with_suffix(local_path.suffix + ".partial")).exists()
        # The policy rejection is observable: download_file logs "Download
        # failed for <url>: <exc>" at WARNING, and the peer guard names the
        # offending peer in the message.
        failed = [r for r in caplog.records if "Download failed" in r.getMessage()]
        assert any(
            "127.0.0.1" in r.getMessage() for r in failed
        ), "expected the policy rejection to be logged"


def test_download_files_cli_path_rejects_loopback_structured(
    tmp_path, local_http_server
):
    # G1: the parallel worker path (download_files) enforces the same strict
    # policy; a loopback URL counts as a failure, and no exception escapes.
    with local_http_server() as srv:
        local_path = tmp_path / "video.mp4"
        result = download_files([(srv.url(), local_path)], workers=1)
        success, _skipped, failed, paths = result
        assert success == 0
        assert failed == 1
        assert paths == []
        assert not local_path.exists()


def test_peer_guard_rejects_loopback_at_connect(tmp_path, local_http_server):
    # F-B: unit-test the guarded connection directly. A strict guard (empty
    # allow-local set) connecting to the local test server (peer 127.0.0.1)
    # must raise URLPolicyError AT CONNECT TIME, with the socket closed —
    # this is the DNS-rebinding proof: the pre-connect check is bypassed by
    # connecting to a raw IP, and only the peer check catches it.
    with local_http_server() as srv:
        strict = _peer_checked_connection(
            urllib3.connection.HTTPConnection, frozenset()
        )
        conn = strict("127.0.0.1", srv.port)
        with pytest.raises(URLPolicyError):
            conn.connect()
        # The socket was closed on rejection (fileno -1 / no live fd).
        sock = conn.sock
        assert sock is None or sock.fileno() == -1


def test_peer_guard_adapter_unwraps_policy_error_end_to_end(
    local_http_server, monkeypatch
):
    # G2: drive a real request through the peer-checked adapter mounted on a
    # real requests.Session. The pre-connect _assert_public_url is bypassed by
    # pointing the session straight at the loopback server (as a rebinding
    # attacker's TCP peer would be after DNS flips). The peer guard must
    # reject the actual peer, and the adapter must re-raise the ORIGINAL
    # URLPolicyError (not a wrapped ConnectionError) so the structured failure
    # identity survives end-to-end. The socket must be closed on rejection.
    with local_http_server() as srv:
        session = requests.Session()
        _install_peer_check(session, frozenset())

        captured: dict = {}
        real_check_peer = downloader._check_peer

        def _spy(conn):
            captured["conn"] = conn
            captured["sock"] = getattr(conn, "sock", None)
            real_check_peer(conn)

        monkeypatch.setattr(downloader, "_check_peer", _spy)

        with pytest.raises(URLPolicyError) as exc_info:
            session.get(srv.url(), timeout=5)

        raised = exc_info.value
        assert isinstance(raised, URLPolicyError)
        # The unwrap preserves identity: it is URLPolicyError ITSELF, and
        # nothing urllib3 wrapped it into a plain ConnectionError.
        assert not isinstance(raised, requests.exceptions.ConnectionError)
        assert "127.0.0.1" in str(raised)
        # The socket held by the guarded connection was closed on rejection
        # (fileno -1 / no live fd).
        sock = captured.get("sock")
        assert sock is not None
        assert sock.fileno() == -1
        # And the guarded connection itself reports no live socket.
        conn = captured.get("conn")
        assert getattr(conn, "sock", None) is None


def test_peer_guard_allows_local_when_host_listed(tmp_path, local_http_server):
    # F-B: with the initial host in the allow-local set, the same guarded
    # connection to the loopback server succeeds.
    with local_http_server() as srv:
        allowed = _peer_checked_connection(
            urllib3.connection.HTTPConnection,
            frozenset({"127.0.0.1"}),
        )
        conn = allowed("127.0.0.1", srv.port)
        try:
            conn.connect()
            assert conn.sock is not None
            assert conn.sock.getpeername()[0] == "127.0.0.1"
        finally:
            with suppress(OSError):
                conn.close()


def test_final_manifest_write_failure_is_structured(
    tmp_path, fast_policy, local_http_server, monkeypatch
):
    # F-C: the final _record_identity(..., complete=True) write raises OSError.
    # The bytes are complete and verified in staging; the failure must be a
    # structured outcome (ok False, error OSError, final_path=staging,
    # ok_bytes=full size), never a raised exception.
    with local_http_server() as srv:
        staging = tmp_path / "media.mp4.partial"
        manifest = staging.with_name(staging.name + ".identity.json")

        def _boom(path, identity, *, complete):
            if complete:
                raise OSError("simulated final manifest write failure")
            _record_identity(path, identity, complete=complete)

        monkeypatch.setattr(downloader, "_record_identity", _boom)
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok is False
        assert result.error_class == "OSError"
        assert result.error_message is not None
        assert result.final_path == staging
        assert result.bytes_downloaded == 1000
        # The verified bytes remain in the staging file for the caller.
        assert staging.read_bytes() == BODY_1000
        # The complete mark never landed (only the in-attempt incomplete writes
        # via the original helper ran).
        if manifest.exists():
            assert _manifest(staging)["complete"] is False


def test_rotating_206_etag_terminates_structured(tmp_path, local_http_server):
    # F-D(a): a server rotating its ETag on every 206 must not force an
    # unbounded restart loop. The pre-fix inner `while True` would restart
    # forever on the 206 conflict-restart branch; now the per-pass restart
    # budget raises IdentityConflictError so each pass consumes one retry
    # attempt, and the bounded retry budget terminates the download FAST as
    # a structured RetriesExhausted failure.
    #
    # The restart budget is only reachable on the 206 conflict branch: a
    # conflicting fresh 200 is adopted and succeeds (see the sibling test).
    # We seed a partial so every GET is a 206, and we keep the 206 path
    # conflict-forcing: every 206 carries a fresh, HEAD-contradicting ETag.
    # always_206 keeps every GET a 206 (a 200 would be adopted and succeed),
    # so the bounded 206 conflict-restart branch is exercised every pass.
    fast = TimeoutPolicy(
        connect_seconds=5, read_seconds=30, retry_attempts=2, backoff_seconds=0.0
    )
    with local_http_server(rotating_206_etag=True, always_206=True) as srv:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')
        start = time.monotonic()
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast,
            allow_local=True,
        )
        elapsed = time.monotonic() - start
        assert result.ok is False
        assert result.error_class == "RetriesExhausted"
        assert result.error_message is not None
        # Error-propagation contract: the bounded conflict restarts surface
        # their cause — IdentityConflictError — in the RetriesExhausted
        # message, so the exhaustion names the conflict, not just "failed".
        assert "IdentityConflictError" in result.error_message
        # Must terminate quickly: bounded retries + restart budget, no loop.
        assert elapsed < 5.0


def test_conflicting_full_200_adopts_served_identity(
    tmp_path, fast_policy, local_http_server
):
    # F-D(b): fresh start (partial 0), HEAD reports etag v1, but the GET 200
    # carries etag v2. A conflicting full 200 with an empty partial must be
    # adopted and consumed as-is: success, correct content, and the outcome's
    # remote_identity carries the adopted etag v2.
    with local_http_server(etag='"v2"', head_etag='"v1"') as srv:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert result.bytes_downloaded == 1000
        assert result.resumed is False
        assert staging.read_bytes() == BODY_1000
        assert result.remote_identity is not None
        assert result.remote_identity["etag"] == '"v2"'
        assert _manifest(staging)["etag"] == '"v2"'


class _FakeResponse:
    """Minimal stand-in for a streamed requests.Response body."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def iter_content(self, chunk_size: int):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]

    @property
    def headers(self) -> dict:
        return {"Content-Length": str(len(self._body))}


def test_staging_leaf_symlink_rejected_at_open(tmp_path):
    # F-F: a fresh symlink created where the staging leaf sits must be refused
    # at open time by O_NOFOLLOW. The is_symlink pre-check is the primary
    # gate when download_root is set; O_NOFOLLOW in _read_body is the
    # open-time backstop. We point the staging path at a symlink whose target
    # is an existing real file; a direct open() with O_NOFOLLOW must fail
    # with ELOOP, proving the open site refuses the leaf symlink.
    real = tmp_path / "real_target.bin"
    real.write_bytes(b"0" * 8)
    leaf = tmp_path / "media.mp4.partial"
    leaf.symlink_to(real)
    assert leaf.is_symlink()
    with pytest.raises(OSError) as excinfo:
        _read_body(
            _FakeResponse(BODY_1000),
            leaf,
            "wb",
            base=0,
            chunk_size=8192,
            progress=None,
            expected_size=1000,
        )
    # ELOOP (looping symlink) is the OS refusal for O_NOFOLLOW open of a
    # symlink; the key property is an OSError raised at open time, not a
    # write through the link.
    assert excinfo.value.errno in (errno.ELOOP, errno.EISDIR)
    # The link target was NOT overwritten through the symlink.
    assert real.read_bytes() == b"0" * 8


def test_manifest_tmp_sibling_symlink_rejected(tmp_path):
    # P1: a symlink planted at the identity manifest's tmp-sibling path
    # (<manifest>.identity.json.tmp) must be refused at open time by
    # O_NOFOLLOW in _record_identity. The tmp-sibling write is the only site
    # that follows the tmp path, so a leaf swap there would let a local
    # attacker write through the link; the O_NOFOLLOW open must fail with
    # ELOOP instead, leaving the target untouched and the manifest
    # un-replaced.
    manifest = tmp_path / "media.mp4.partial.identity.json"
    manifest.write_text('{"complete": false}\n')
    real = tmp_path / "real_target.bin"
    real.write_bytes(b"PRECIOUS")
    # The exact tmp-sibling naming _record_identity derives:
    # path.with_suffix(path.suffix + ".tmp").
    tmp_sibling = manifest.with_suffix(manifest.suffix + ".tmp")
    assert tmp_sibling.name == "media.mp4.partial.identity.json.tmp"
    tmp_sibling.symlink_to(real)
    assert tmp_sibling.is_symlink()
    with pytest.raises(OSError) as excinfo:
        _record_identity(
            manifest,
            {"content_length": 1000, "etag": '"v1"'},
            complete=True,
        )
    # The OS refused the O_NOFOLLOW open of the symlink.
    assert excinfo.value.errno in (errno.ELOOP, errno.EISDIR)
    # The link target was NOT written through the symlink.
    assert real.read_bytes() == b"PRECIOUS"
    # The manifest itself was NOT replaced (the tmp write failed first).
    assert manifest.read_text() == '{"complete": false}\n'


def test_download_file_caller_session_rejects_loopback_and_stays_open(
    tmp_path, local_http_server, caplog
):
    # P1: like the own-session path (G1), a caller-supplied session is
    # opted into the strict SSRF guards, so a loopback URL fails the
    # (bool, int) contract with the policy rejection logged. The caller
    # session is NOT closed by download_file: a follow-up request on it is
    # still serviced by the (now-installed) peer guard — which rejects the
    # loopback peer with URLPolicyError, proving the session is alive and
    # the guards are mounted (a closed/abandoned session would fail a
    # different way).
    with local_http_server() as srv:
        local_path = tmp_path / "video.mp4"
        session = requests.Session()
        try:
            with caplog.at_level(
                logging.WARNING, logger="media_archive_sync.downloader"
            ):
                result = download_file(srv.url(), local_path, session=session)
            assert result == (False, 0)
            # No partial or final file was written.
            assert not local_path.exists()
            assert not (local_path.with_suffix(local_path.suffix + ".partial")).exists()
            # The policy rejection is observable: download_file logs
            # "Download failed for <url>: <exc>" at WARNING, and the peer
            # guard names the offending peer.
            failed = [r for r in caplog.records if "Download failed" in r.getMessage()]
            assert any(
                "127.0.0.1" in r.getMessage() for r in failed
            ), "expected the policy rejection to be logged"
            # Caller-owned: the session is still usable afterward. A
            # follow-up request is serviced by the mounted peer guard,
            # which rejects the loopback peer with the ORIGINAL
            # URLPolicyError (not a plain ConnectionError).
            follow_up_error: BaseException | None = None
            try:
                session.get(srv.url(), timeout=5)
            except BaseException as exc:
                follow_up_error = exc
            assert isinstance(follow_up_error, URLPolicyError), (
                f"expected the live session's peer guard to reject the "
                f"follow-up loopback request, got {follow_up_error!r}"
            )
        finally:
            session.close()


def test_check_peer_unparseable_peer_fails_closed():
    # P1: an unparseable getpeername() (a resolver/peer that returns
    # something ip_address cannot parse) must FAIL CLOSED for every
    # non-allowed host: URLPolicyError raised and the socket closed —
    # never a silent pass. Unit-test _check_peer directly with a guarded
    # connection (empty allowed set) and a stub sock.
    strict = _peer_checked_connection(urllib3.connection.HTTPConnection, frozenset())
    conn = strict("example.com", 80)

    class _FakeSock:
        """Minimal sock stand-in: records close(), returns a fixed peer."""

        def __init__(self, peer: str) -> None:
            self._peer = peer
            self.closed = False

        def getpeername(self) -> tuple[str, int]:
            return (self._peer, 41337)

        def close(self) -> None:
            self.closed = True

    # "256.256.256.256" makes ip_address raise ValueError.
    sock = _FakeSock("256.256.256.256")
    conn.sock = sock
    with pytest.raises(URLPolicyError):
        _check_peer(conn)
    # The socket was closed on the fail-closed rejection.
    assert sock.closed is True


def test_read_body_append_mode_symlink_rejected(tmp_path):
    # P2: the O_NOFOLLOW backstop must also fire in APPEND mode ("ab"),
    # not just "wb": a symlink swapped in at the staging leaf is refused
    # at open time with ELOOP, and the link target is not written through.
    # (Mirrors test_staging_leaf_symlink_rejected_at_open for the append
    # path; base=0 matches an empty target so the size guard passes and
    # the open is what must fail.)
    real = tmp_path / "real_target.bin"
    real.write_bytes(b"")
    leaf = tmp_path / "media.mp4.partial"
    leaf.symlink_to(real)
    assert leaf.is_symlink()
    with pytest.raises(OSError) as excinfo:
        _read_body(
            _FakeResponse(BODY_1000),
            leaf,
            "ab",
            base=0,
            chunk_size=8192,
            progress=None,
            expected_size=1000,
        )
    # ELOOP: the O_NOFOLLOW open of the symlink is refused at open time.
    assert excinfo.value.errno in (errno.ELOOP, errno.EISDIR)
    # The link target was NOT appended to through the symlink.
    assert real.read_bytes() == b""


def test_peer_adapter_send_reraises_plain_connection_error(monkeypatch):
    # P2: the adapter's ConnectionError unwrap must only re-raise
    # URLPolicyError when the cause/context chain actually carries one. A
    # plain transport ConnectionError (timeout, RST, ...) that carries NO
    # URLPolicyError must escape unchanged, so ordinary failures stay
    # ordinary and keep their identity.
    adapter = _PeerCheckedHTTPAdapter(frozenset())
    plain = requests.exceptions.ConnectionError("plain transport failure")
    calls = 0

    def _plain_send(self, request, **_kwargs):
        nonlocal calls
        calls += 1
        raise plain

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", _plain_send)
    with pytest.raises(requests.exceptions.ConnectionError) as excinfo:
        adapter.send(requests.PreparedRequest())
    # The SAME exception object escaped (identity, not just shape): the
    # unwrap did not wrap, re-type, or replace an ordinary failure.
    assert excinfo.value is plain
    assert calls == 1


def test_intermediate_symlink_after_precheck_pins_refused(
    tmp_path, fast_policy, local_http_server, monkeypatch
):
    # P0: a single O_NOFOLLOW open of the full parent path only guards the
    # FINAL component; a symlinked INTERMEDIATE directory under the root is
    # followed during traversal, so the pinned fd would capture the external
    # target and every confined write would escape the root. The race: the
    # upfront _assert_no_symlinked_dirs_under_root passes (sees a real dir),
    # then the intermediate is swapped for a symlink BEFORE the pin walk
    # opens it. The pin walk must open each component dir_fd-relative so the
    # intermediate symlink is refused with ELOOP instead of followed.
    #
    # Layout: the pinned parent is root/link/inner; the symlink is `link`,
    # an INTERMEDIATE component (not the final `inner`), so the pre-fix
    # final-component O_NOFOLLOW guard does NOT see it. Pre-fix, the open
    # follows link to the external realdir/inner, pins it, and the download
    # overwrites the external file (the escape). Post-fix, the per-component
    # walk refuses `link` with ELOOP before any write. The PRECIOUS-content
    # assertion is what discriminates the two.
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    real = outside / "realdir"
    inner_real = real / "inner"
    inner_real.mkdir(parents=True)
    (inner_real / "media.mp4.partial").write_bytes(b"PRECIOUS")
    # The swap source: root/link is a real dir mirroring the external
    # layout (pre-check passes); the attacker renames it and plants a
    # symlink to the external dir at the same name.
    link = root / "link"
    (link / "inner").mkdir(parents=True)
    staging = link / "inner" / "media.mp4.partial"

    def _swapped_precheck(*_args, **_kwargs):
        # Simulate the race window: the pre-check saw a real dir, then the
        # attacker swaps it for a symlink before the pin walk opens it.
        link.rename(link.with_name("link.real"))
        link.symlink_to(real)

    monkeypatch.setattr(
        downloader, "_assert_no_symlinked_dirs_under_root", _swapped_precheck
    )
    with local_http_server() as srv, pytest.raises(OSError) as excinfo:
        download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
            download_root=root,
        )
    # The symlinked intermediate was refused at open time instead of
    # followed. ENOTDIR is the Linux expression of O_NOFOLLOW|O_DIRECTORY on a
    # symlink-to-dir (the link is left unfollowed, then O_DIRECTORY finds a
    # non-directory); ELOOP is the leaf-style refusal. EISDIR is deliberately
    # NOT accepted here: it was the PRE-FIX escape symptom (the walk followed
    # the symlinked intermediate and the subsequent open hit a directory —
    # the behavior this test guards against), not a refusal; accepting it
    # would make the test pass against the very bug it exists to catch. The
    # PRECIOUS-content assertion below is the real discriminator.
    assert excinfo.value.errno in (errno.ENOTDIR, errno.ELOOP)
    # The external target was NOT written through the symlink: the pre-fix
    # code pinned the external directory and overwrote this file, so the
    # escape is refused, not silent.
    assert (inner_real / "media.mp4.partial").read_bytes() == b"PRECIOUS"
    # Nothing else landed outside the root: the symlinked component was
    # never pinned, so no leaf write could have resolved against it.
    assert not (inner_real / "media.mp4.partial.identity.json").exists()


# --- Confinement pin-acquisition contract tests (fd leak / contextvar) ----


def _fd_count() -> int:
    return len(os.listdir("/proc/self/fd"))


def test_confined_setup_failure_closes_pinned_fds_and_resets_contextvar(
    tmp_path, fast_policy, local_http_server, monkeypatch
):
    # The pin-acquisition block (pinned fds opened + _confined_dirs.set)
    # moved INSIDE the try/finally of download_verified so that a failure
    # during session setup — after the fds are pinned and the contextvar
    # is set — still releases everything. _install_peer_check raising
    # RuntimeError is not a requests.RequestException/OSError/ValueError,
    # so it is NOT converted to a structured outcome: it propagates out of
    # download_verified, but the finally must have run before it escaped
    # (no fd leak, contextvar reset to its default).
    root = tmp_path / "root"
    root.mkdir()
    staging = root / "media.mp4.partial"

    def _boom(*_args, **_kwargs):
        raise RuntimeError("simulated adapter-install failure")

    monkeypatch.setattr(downloader, "_install_peer_check", _boom)
    baseline = _fd_count()
    with (
        local_http_server() as srv,
        pytest.raises(RuntimeError, match="simulated adapter-install failure"),
    ):
        download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
            download_root=root,
        )
    # No pinned fd leaked: the finally closed every fd the pin walk opened.
    assert _fd_count() == baseline
    # The confinement contextvar was reset for the caller context: a later
    # (or nested) non-confined download must not see a stale parent map.
    assert downloader._confined_dirs.get() is None


def test_confined_parent_walk_failure_closes_all_walk_fds(tmp_path):
    # A walk failure in _open_confined_parent_dirs (a symlinked component
    # refused with ENOTDIR/ELOOP) must close EVERY fd the helper opened:
    # the in-progress walk fds AND the already-pinned parents' fds. Here
    # the first parent (root/ok) pins successfully, then the second
    # parent's walk reaches a symlink to a plain FILE and is refused —
    # without the except-branch rewind, the root/ok pin would leak.
    root = tmp_path / "root"
    (root / "ok").mkdir(parents=True)
    (root / "p2").mkdir()
    file_target = tmp_path / "outside_file.bin"
    file_target.write_bytes(b"not a directory")
    (root / "p2" / "mid").symlink_to(file_target)
    baseline = _fd_count()
    with pytest.raises(OSError) as excinfo:
        downloader._open_confined_parent_dirs(root, root / "ok", root / "p2" / "mid")
    # ENOTDIR: Linux's O_NOFOLLOW|O_DIRECTORY on a symlink (to anything);
    # ELOOP: the leaf-style refusal. Either proves the walk refused the
    # component instead of following it.
    assert excinfo.value.errno in (errno.ENOTDIR, errno.ELOOP)
    # The helper's own teardown closed everything it opened, including the
    # already-pinned root/ok fd — no fd leak on the failure path.
    assert _fd_count() == baseline


def test_confined_parent_walk_depth2_success_holds_only_final_fds(tmp_path):
    # Success-path fd accounting for a depth-2 walk: each intermediate
    # component fd is released as soon as the next component opens
    # (walk_fds.pop() + close), so only the final pin per parent survives.
    # A depth-2 walk of two parents must therefore hold EXACTLY two open
    # fds — the two pinned parents — with the root fd and any intermediate
    # already released by the time the helper returns.
    root = tmp_path / "root"
    (root / "d1").mkdir(parents=True)
    (root / "d2").mkdir()
    baseline = _fd_count()
    fds = downloader._open_confined_parent_dirs(root, root / "d1", root / "d2")
    assert len(fds) == 2
    # Only the two final pins are live; the root_fd was not handed over
    # (no parent == root) and no intermediate survived.
    assert _fd_count() == baseline + 2
    # Caller-owned pins close cleanly and release everything.
    for fd in fds.values():
        os.close(fd)
    assert _fd_count() == baseline


def test_confined_symlinked_download_root_refused(
    tmp_path, fast_policy, local_http_server
):
    # The per-component pin walk opens the ROOT itself with
    # O_NOFOLLOW|O_DIRECTORY, so a download_root that is itself a symlink
    # is refused at open time — a new refusal boundary the pre-walk code
    # (which opened the root's parent or the full parent paths) did not
    # have. NOTE: the structured-outcome conversion for pin-walk OSErrors
    # was deferred, so TODAY a raw OSError (ENOTDIR/ELOOP) escapes
    # download_verified; if a future change converts pin-walk failures to
    # structured outcomes, update this test's raises-clause to expect the
    # structured failure instead.
    realroot = tmp_path / "realroot"
    (realroot / "inner").mkdir(parents=True)
    link = tmp_path / "link"
    link.symlink_to(realroot)
    staging = link / "inner" / "media.mp4.partial"
    baseline = _fd_count()
    with local_http_server() as srv, pytest.raises(OSError) as excinfo:
        download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
            download_root=link,
        )
    assert excinfo.value.errno in (errno.ENOTDIR, errno.ELOOP)
    # The refusal happened before any write: the real target directory is
    # untouched (no .partial, no .identity.json) — nothing escaped through
    # the symlinked root.
    assert list((realroot / "inner").iterdir()) == []
    # The escaped failure still went through the finally: no fd leak.
    assert _fd_count() == baseline


# --- PR #9 canonical reconciliation fixes (F1..F7) --------------------------


def test_install_peer_check_preserves_pre_mounted_adapter_config():
    # F1: Session.mount() REPLACES the mounted adapter for a scheme, so
    # _install_peer_check must not silently discard a caller's transport
    # config. Two cases:
    #   (a) a legacy-path session (HTTPAdapter(max_retries=1) on https,
    #       as download_file / download_files install) keeps total==1 on
    #       the adapter that ends up serving the scheme; and
    #   (b) a caller session with a custom adapter (non-default
    #       max_retries / pool_connections / pool_maxsize) sees those
    #       settings carried into the peer-checked adapter.
    # Both must result in a _PeerCheckedHTTPAdapter (the guard is real),
    # with the preserved transport config — not the urllib3 Retry(0,
    # read=False) default that a bare _PeerCheckedHTTPAdapter() would carry.
    from urllib3.util.retry import Retry

    # (a) Legacy path: only https was pre-mounted with max_retries=1.
    legacy = requests.Session()
    legacy.mount("https://", HTTPAdapter(max_retries=1))
    _install_peer_check(legacy, frozenset())
    served = legacy.get_adapter("https://")
    assert isinstance(served, _PeerCheckedHTTPAdapter)
    # The deliberate max_retries=1 survived the peer-guard install: the
    # adapter serving the scheme still retries once (not the Retry(0,
    # read=False) downgrade a fresh _PeerCheckedHTTPAdapter() carries).
    assert isinstance(served.max_retries, Retry)
    assert served.max_retries.total == 1

    # (b) Caller session: custom adapter with non-default retry/pool config.
    caller = requests.Session()
    custom = HTTPAdapter(max_retries=2, pool_connections=5, pool_maxsize=9)
    caller.mount("http://", custom)
    _install_peer_check(caller, frozenset())
    served_http = caller.get_adapter("http://")
    assert isinstance(served_http, _PeerCheckedHTTPAdapter)
    # The peer guard is mounted (not the caller's plain adapter).
    assert served_http is not custom
    # Caller's transport config was carried into the peer-checked adapter.
    assert isinstance(served_http.max_retries, Retry)
    assert served_http.max_retries.total == 2
    assert served_http._pool_connections == 5
    assert served_http._pool_maxsize == 9


def test_install_peer_check_preserves_pool_block():
    # A caller's bounded-pool adapter (pool_block=True) must keep its
    # bounded behavior through the peer-guard install: requests stores the
    # constructor pool_block value on the adapter as _pool_block, and
    # _preserved_adapter_config carries it into the replacement.
    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_block=True, pool_maxsize=1))
    _install_peer_check(session, frozenset())
    served = session.get_adapter("https://")
    assert isinstance(served, _PeerCheckedHTTPAdapter)
    assert served._pool_block is True
    assert served._pool_maxsize == 1


def test_install_peer_check_reinstall_preserves_pool_block():
    # P1: re-installing the peer guard on a session whose mounted adapter is
    # ALREADY a _PeerCheckedHTTPAdapter from an earlier install must carry
    # the prior adapter's pool config forward (read back via
    # _preserved_adapter_config) — not downgrade a bounded pool to the
    # fresh-session defaults.
    session = requests.Session()
    _install_peer_check(session, frozenset())
    first = session.get_adapter("https://")
    # First install carries the session built-in adapter's config
    # (default pool_block=False); re-install with the bounded-pool
    # variant directly mounted so the re-install branch (previous
    # _PeerCheckedHTTPAdapter) is what gets read back.
    session.mount(
        "https://",
        _PeerCheckedHTTPAdapter(
            frozenset(), max_retries=2, pool_maxsize=2, pool_block=True
        ),
    )
    _install_peer_check(session, frozenset())
    served = session.get_adapter("https://")
    assert isinstance(served, _PeerCheckedHTTPAdapter)
    # A FRESH adapter replaced the mounted one (no in-place reuse) ...
    assert served is not first
    # ... and the bounded-pool config carried forward across the re-install.
    assert served._pool_block is True
    assert served._pool_maxsize == 2


def test_install_peer_check_non_http_adapter_defaults_pool_block_false():
    # P2: a mounted adapter that is NOT an HTTPAdapter (bare BaseAdapter
    # subclass — custom transport / test double) has no _pool_block
    # attribute, so the fallback branch of _preserved_adapter_config must
    # default pool_block to False with the standard pool sizes (10/10) —
    # not a getattr-default flip to True that would silently bound the
    # pool on a caller session with a custom transport.
    from requests.adapters import BaseAdapter

    class _BareAdapter(BaseAdapter):
        pass

    session = requests.Session()
    session.mount("http://", _BareAdapter())
    _install_peer_check(session, frozenset())
    served = session.get_adapter("http://")
    assert isinstance(served, _PeerCheckedHTTPAdapter)
    assert served._pool_block is False
    assert served._pool_connections == 10
    assert served._pool_maxsize == 10


def test_install_peer_check_preserves_pool_block_false_default():
    # P2: an HTTPAdapter mounted WITHOUT an explicit pool_block (the common
    # case) carries requests' default _pool_block=False. _preserved_adapter_config
    # reads it via getattr(adapter, "_pool_block", False) — if that default
    # flipped to True, every caller session with a plain HTTPAdapter would
    # get a silently bounded pool after the peer-guard install.
    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_maxsize=3))
    _install_peer_check(session, frozenset())
    served = session.get_adapter("https://")
    assert isinstance(served, _PeerCheckedHTTPAdapter)
    assert served._pool_block is False
    assert served._pool_maxsize == 3


def test_install_peer_check_reinstall_refreshes_direct_pool_classes():
    # D1: the allowed-local set is baked into the guarded connection classes
    # at pool-manager construction (init_poolmanager), so merely refreshing
    # a reused adapter's _mas_allowed_local in place is INERT for direct
    # pools: an already-constructed PoolManager would keep the FIRST
    # install's set. _install_peer_check therefore mounts a FRESH
    # _PeerCheckedHTTPAdapter on every install (mount replaces by prefix —
    # no stacking), baking the CURRENT allowed_local.
    # Install permissive, force pool-manager creation, re-install strict on
    # the SAME session, and assert the direct pool classes now carry the
    # strict (empty) set — not the stale permissive one.
    permissive = frozenset({"lab:8000"})
    session = requests.Session()
    _install_peer_check(session, permissive)
    first = session.get_adapter("http://")
    # Force pool-manager creation so the pool classes are baked.
    first.init_poolmanager(10, 10)
    assert (
        first.poolmanager.pool_classes_by_scheme[
            "http"
        ].ConnectionCls._mas_allowed_local
        == permissive
    )

    _install_peer_check(session, frozenset())
    fresh = session.get_adapter("http://")
    assert isinstance(fresh, _PeerCheckedHTTPAdapter)
    # A FRESH adapter replaced the mounted one (no in-place reuse).
    assert fresh is not first
    # The adapter attr is strict ...
    assert fresh._mas_allowed_local == frozenset()
    # ... and the DIRECT pool classes (existing pool manager, which HTTPAdapter
    # constructs eagerly, plus any future one from _guarded_pool_classes)
    # bake the STRICT set — the permissive set is gone.
    assert (
        fresh.poolmanager.pool_classes_by_scheme[
            "http"
        ].ConnectionCls._mas_allowed_local
        == frozenset()
    )
    assert (
        fresh.poolmanager.pool_classes_by_scheme[
            "https"
        ].ConnectionCls._mas_allowed_local
        == frozenset()
    )
    assert (
        fresh._guarded_pool_classes()["http"].ConnectionCls._mas_allowed_local
        == frozenset()
    )
    # Re-install is config-preserving, not a downgrade.
    assert fresh.max_retries == first.max_retries
    assert fresh._pool_connections == first._pool_connections
    assert fresh._pool_maxsize == first._pool_maxsize


def test_install_peer_check_tolerates_non_http_adapter():
    # D2: a caller session may mount a plain BaseAdapter subclass (custom
    # transport / test double) that carries no max_retries. _install_peer_check
    # must not raise reading config off it, and the installed peer-checked
    # adapter must get the standard fresh-session retry budget
    # (Retry(total=3)) — not a Retry(0, read=False) downgrade and not a
    # crash from a missing attribute.
    from requests.adapters import BaseAdapter
    from urllib3.util.retry import Retry

    class _BareAdapter(BaseAdapter):
        pass

    session = requests.Session()
    session.mount("http://", _BareAdapter())
    _install_peer_check(session, frozenset())  # must not raise
    served = session.get_adapter("http://")
    assert isinstance(served, _PeerCheckedHTTPAdapter)
    assert isinstance(served.max_retries, Retry)
    assert served.max_retries.total == 3
    # The other scheme still gets the requests built-in default preserved
    # (fresh session: Retry(0, read=False)).
    served_https = session.get_adapter("https://")
    assert isinstance(served_https, _PeerCheckedHTTPAdapter)
    assert isinstance(served_https.max_retries, Retry)
    assert served_https.max_retries.total == 0
    assert served_https.max_retries.read is False


def test_proxy_manager_for_uses_guarded_pool_classes(local_http_server):
    # F4: the branch claims "peer guard applied to proxy connection pools"
    # via _PeerCheckedHTTPAdapter.proxy_manager_for (pool_classes_by_scheme
    # replacement), but no test exercised it. Prove the guarded pool classes
    # are ACTUALLY used for proxy-originated connections, at the
    # pool-class level (the reliable form):
    #   (a) adapter.proxy_manager_for(proxy) returns a ProxyManager whose
    #       pool_classes_by_scheme are the guarded classes (NOT the urllib3
    #       defaults);
    #   (b) a connection through that proxy manager to a non-public peer is
    #       rejected by the peer guard (allow_local empty).
    # This test FAILS if proxy_manager_for stops overriding pool classes
    # (the guarded ConnectionCls would be the plain urllib3 default).
    with local_http_server() as srv:
        adapter = _PeerCheckedHTTPAdapter(frozenset())
        proxy_mgr = adapter.proxy_manager_for("http://127.0.0.1:8080")
        # (a) pool classes on the proxy manager are the guarded ones.
        pcls = proxy_mgr.pool_classes_by_scheme
        guarded_http = pcls["http"]
        guarded_https = pcls["https"]
        # They are subclasses of the standard pools (a guarded wrapper),
        # not the plain urllib3 defaults.
        assert issubclass(guarded_http, urllib3.connectionpool.HTTPConnectionPool)
        assert issubclass(guarded_https, urllib3.connectionpool.HTTPSConnectionPool)
        assert guarded_http is not urllib3.connectionpool.HTTPConnectionPool
        # The ConnectionCls the proxy pool uses is the peer-checked
        # connection (carrying the allowed-local set), not the plain one.
        assert issubclass(guarded_http.ConnectionCls, urllib3.connection.HTTPConnection)
        assert issubclass(
            guarded_https.ConnectionCls, urllib3.connection.HTTPSConnection
        )
        # The same guarded ConnectionCls is what a pool ACTUALLY pulled from
        # the proxy manager will use to connect (end-to-end pool wiring).
        pool = proxy_mgr.connection_from_host("example.com", 80)
        assert pool.ConnectionCls is guarded_http.ConnectionCls
        # (b) A connection through the guarded proxy pool class to a
        #     non-public (loopback) peer with an empty allow-local set is
        #     refused at connect time by the peer guard.
        conn_cls = guarded_http.ConnectionCls
        conn = conn_cls("127.0.0.1", srv.port)
        with pytest.raises(URLPolicyError):
            conn.connect()


def test_confined_root_pin_mixed_spelling_no_double_close(tmp_path):
    # F2: dedup in _open_confined_parent_dirs is raw-Path lexical equality
    # while rel is computed from abspath, so two distinct absolute spellings
    # of the SAME directory (normalized root vs root/inner/..) both get
    # rel.parts == () and (pre-fix) both stored root_fd itself; the caller
    # closing every pinned fd then double-closed it (EBADF escaping
    # download_verified). Post-fix, a parent equal to the root (in ANY raw
    # spelling) pins os.dup(root_fd) — one descriptor per key, distinct
    # spellings each get their own fd — and the helper always closes its own
    # root_fd, so closing every returned fd is safe and leaks nothing.
    # ALL-ABSOLUTE paths, NO chdir, NO relative Path.
    root = tmp_path / "root"
    root.mkdir()
    (root / "inner").mkdir()
    # A distinct ABSOLUTE raw spelling of root (unnormalized '..').
    root_alt = root / "inner" / ".."
    assert root != root_alt  # distinct raw Path spellings
    assert root.resolve() == root_alt.resolve()  # same real directory
    baseline = _fd_count()
    fds = downloader._open_confined_parent_dirs(root, root, root_alt)
    # Two distinct keys -> two distinct fd VALUES (each an os.dup of the
    # root fd), never the same descriptor twice.
    assert set(fds.keys()) == {root, root_alt}
    fd_root = fds[root]
    fd_alt = fds[root_alt]
    assert fd_root != fd_alt
    # Both fds point at the same (root) inode.
    assert os.fstat(fd_root).st_ino == os.fstat(fd_alt).st_ino
    # Closing every returned fd (in BOTH orders) must not double-close and
    # must leave zero leaked fds — the helper closed its own root_fd.
    os.close(fd_root)
    os.close(fd_alt)
    assert _fd_count() == baseline


def test_confined_root_pin_mixed_spelling_end_to_end(
    tmp_path, fast_policy, local_http_server
):
    # F2 (end-to-end): a REAL confined download whose download_root is the
    # normalized root and whose manifest parent is passed as a distinct
    # unnormalized '..' spelling of the SAME root must complete ok, with the
    # final file at the normalized location inside root. Pre-fix, the
    # finally's double-close of the shared root fd raised EBADF that escaped
    # download_verified; post-fix it completes cleanly with zero fd leak.
    with local_http_server() as srv:
        root = tmp_path / "root"
        root.mkdir()
        (root / "inner").mkdir()
        # Two DISTINCT absolute raw spellings of root, so that the staging
        # parent and the manifest parent are distinct Path objects spelling
        # the SAME directory — exactly what defeats the pre-fix raw-Path
        # dedup in _open_confined_parent_dirs (both normalize to
        # rel.parts == () and, pre-fix, both stored root_fd itself, so the
        # caller's close-everything double-closed it). pathlib keeps an
        # unnormalized '..' component, and two different intermediate names
        # give two different lexically-distinct spellings of the same dir.
        # ('a' need not exist: no component of the manifest chain is ever
        # stat'd — the confined leaf ops resolve against the pinned parent
        # fd — and both parents' symlink pre-check walks only real dirs.)
        staging = root / "inner" / ".." / "media.mp4.partial"
        manifest = root / "a" / ".." / "media.mp4.partial.identity.json"
        assert staging.parent != manifest.parent  # distinct raw spellings
        assert os.path.abspath(staging.parent) == os.path.abspath(manifest.parent)
        baseline = _fd_count()
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
            download_root=root,
            identity_manifest_path=manifest,
        )
        assert result.ok
        # The final file is at the NORMALIZED location inside root.
        final = root / "media.mp4.partial"
        assert final.is_file()
        assert final.read_bytes() == BODY_1000
        assert final.stat().st_size == 1000
        assert result.final_path == staging  # as passed (raw spelling)
        # Manifest persisted at the normalized manifest location.
        assert (root / "media.mp4.partial.identity.json").is_file()
        # No double-close / no fd leak: the mixed-spelling pin released all
        # of its descriptors.
        assert _fd_count() == baseline


def test_server_206_without_range_reports_not_resumed(
    tmp_path, fast_policy, local_http_server, monkeypatch
):
    # F5: a server that answers a GET with 206 + Content-Range bytes=0- even
    # though the client sent NO Range header and no partial exists (a fresh
    # download) must NOT be misreported as resumed. resumed is derived from
    # partial_size (a genuine Range resume), not the bare 206 status code.
    # The body is still written (ab to an empty/created file) and verifies.
    with local_http_server(always_206=True) as srv:
        staging = tmp_path / "media.mp4.partial"
        get_headers = []
        original_get = downloader.requests.Session.get

        def _spy(self, url, *a, **kw):
            get_headers.append(dict(kw.get("headers") or {}))
            return original_get(self, url, *a, **kw)

        monkeypatch.setattr(downloader.requests.Session, "get", _spy)
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        # Fresh download (no partial): the GET sent NO Range header, yet the
        # server answered 206 from zero. It must succeed with correct bytes
        # and NOT be reported as a resumption.
        assert result.ok
        assert result.resumed is False
        assert result.bytes_downloaded == 1000
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000
        # Proof no Range header was sent on the fresh GET (the 206 was the
        # server's choice, not a resume the client requested).
        assert get_headers, "the GET was never issued"
        assert all("Range" not in h for h in get_headers)


def test_probe_vanish_restarts_in_pass_without_retry(
    tmp_path, fast_policy, local_http_server, monkeypatch
):
    # F6: partial_size is computed as _confined_stat(...) if
    # _confined_is_file(...) else 0 — two syscalls. If the file vanishes
    # BETWEEN them, the pre-fix code let the bare FileNotFoundError/OSError
    # escape _verified_attempt and burn a full retry attempt with backoff.
    # Post-fix the probe's vanished-file race is translated into the same
    # clean in-pass restart the "vanished before append" branch uses: the
    # pass restarts from byte zero WITHOUT consuming a retry attempt (no
    # backoff sleep). We simulate the race by making the FIRST probe's
    # is_file lie True and the stat raise FileNotFoundError; subsequent
    # probes use the real helpers.
    with local_http_server() as srv:
        staging = tmp_path / "media.mp4.partial"
        probed = {"n": 0}
        real_is_file = downloader._confined_is_file
        real_stat = downloader._confined_stat

        def _fake_is_file(leaf):
            if probed["n"] == 0 and leaf == staging:
                # First probe: claim the file exists...
                return True
            return real_is_file(leaf)

        def _fake_stat(leaf):
            probed["n"] += 1
            if probed["n"] == 1 and leaf == staging:
                # ...then it has vanished before the stat (the race).
                raise FileNotFoundError(f"vanished: {leaf}")
            return real_stat(leaf)

        monkeypatch.setattr(downloader, "_confined_is_file", _fake_is_file)
        monkeypatch.setattr(downloader, "_confined_stat", _fake_stat)
        sleeps = []
        monkeypatch.setattr(downloader.time, "sleep", lambda s: sleeps.append(s))
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        # The vanished-probe race restarted in-pass (no retry attempt burned):
        # zero backoff sleeps across the whole download.
        assert result.ok
        assert sleeps == [], f"probe vanish burned a retry (sleeps={sleeps})"
        assert result.resumed is False
        assert staging.read_bytes() == BODY_1000


# --- Security-contract gap tests (SSRF guards / confinement / shape) ------


def test_check_peer_missing_sock_or_host_attr_fails_closed():
    # P1: _check_peer reads the peer via conn.sock.getpeername() and the
    # carve-out via conn.host. If EITHER attribute is missing the peer
    # CANNOT be inspected, so the guard must fail closed: raise
    # URLPolicyError (and close the socket when one exists). A guarded
    # connection that cannot be verified is never allowed to proceed.
    # Symmetric: a None sock with a set host must fail closed too (no crash
    # reading attributes off the None sock).
    guarded = _peer_checked_connection(urllib3.connection.HTTPConnection, frozenset())

    class _StubSock:
        def __init__(self) -> None:
            self.closed = False

        def getpeername(self) -> tuple[str, int]:
            return ("1.2.3.4", 41337)

        def close(self) -> None:
            self.closed = True

    # sock present, host missing -> raise + close the socket.
    conn = guarded("example.com", 80)
    conn.host = None
    stub = _StubSock()
    conn.sock = stub
    with pytest.raises(URLPolicyError) as exc_info:
        _check_peer(conn)
    assert "Unable to inspect connected peer" in str(exc_info.value)
    assert stub.closed is True

    # sock missing, host present (a public string) -> raise, no crash.
    conn2 = guarded("example.com", 80)
    conn2.host = "8.8.8.8"
    conn2.sock = None
    with pytest.raises(URLPolicyError) as exc_info2:
        _check_peer(conn2)
    assert "Unable to inspect connected peer" in str(exc_info2.value)


def test_allow_local_carve_out_scope(monkeypatch):
    # P2: allow_local=True scopes the carve-out EXACTLY to
    # allow_local and _is_local_address (private/loopback/link-local) —
    # NOT to "everything that isn't a hard disallow". The observable,
    # version-stable contract is:
    #   * the allow path works: a loopback (is_local) peer that is listed
    #     as the allowed host is ACCEPTED, whereas the same peer under the
    #     strict (empty-allow) policy is REJECTED;
    #   * multicast is NEITHER local nor (in CPython's special-registry)
    #     private, so it stays REJECTED under allow_local=True too —
    #     the carve-out is local-scoped, not a full open.
    # NOTE (deviation from the _assert_public_url docstring): the docstring
    # says "reserved/multicast/unspecified remain rejected", but the CODE
    # carves out by is_local, and CPython >=3.11 classifies 0.0.0.0/8 and
    # 240.0.0.0/4 as is_private (hence is_local) — so on this platform they
    # are ACCEPTED with allow_local=True, contradicting the docstring. We
    # pin the STABLE behavior (loopback allow + multicast reject + the
    # strict contrast) rather than the is_private-dependent reserved/
    # unspecified outcomes, which vary across CPython versions.
    allowed = _peer_checked_connection(
        urllib3.connection.HTTPConnection, frozenset({"127.0.0.1"})
    )
    strict = _peer_checked_connection(urllib3.connection.HTTPConnection, frozenset())

    class _FakeSock:
        def __init__(self, peer: str) -> None:
            self._peer = peer
            self.closed = False

        def getpeername(self) -> tuple[str, int]:
            return (self._peer, 41337)

        def close(self) -> None:
            self.closed = True

    def _make(cls, host: str, peer: str):
        c = cls(host, 80)
        c.sock = _FakeSock(peer)
        return c

    # Allow path: listed loopback host + loopback peer -> accepted.
    ok = _make(allowed, "127.0.0.1", "127.0.0.1")
    _check_peer(ok)  # no raise
    assert ok.sock.closed is False

    # Same peer under the strict policy (host not listed) -> rejected.
    rej = _make(strict, "127.0.0.1", "127.0.0.1")
    with pytest.raises(URLPolicyError):
        _check_peer(rej)
    assert rej.sock.closed is True

    # Multicast is not local: rejected even with the host listed.
    mc = _make(allowed, "127.0.0.1", "239.255.255.255")
    with pytest.raises(URLPolicyError):
        _check_peer(mc)
    assert mc.sock.closed is True

    # Mirror the matrix through _assert_public_url's resolution path (the
    # spec's "at minimum" requirement), pinning only the version-STABLE
    # outcomes (the is_private-dependent reserved/unspecified addresses are
    # deliberately NOT pinned, per the note above).
    def _fake_getaddrinfo(ip: str):
        def _resolve(_host, _port):
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 80))]

        return _resolve

    def _assert(ip: str, *, allow_local: bool) -> bool:
        monkeypatch.setattr(downloader.socket, "getaddrinfo", _fake_getaddrinfo(ip))
        try:
            downloader._assert_public_url(
                "http://example.com/x", allow_local=allow_local
            )
            return True
        except URLPolicyError:
            return False

    # Multicast: rejected under BOTH policies (neither local nor private).
    assert _assert("239.255.255.255", allow_local=True) is False
    assert _assert("239.255.255.255", allow_local=False) is False
    # Loopback: the allow path works under allow_local, rejected strict.
    assert _assert("127.0.0.1", allow_local=True) is True
    assert _assert("127.0.0.1", allow_local=False) is False
    # Private (RFC1918): allowed by the carve-out, rejected strict.
    assert _assert("192.168.1.1", allow_local=True) is True
    assert _assert("192.168.1.1", allow_local=False) is False
    # Public: allowed under BOTH policies.
    assert _assert("8.8.8.8", allow_local=True) is True
    assert _assert("8.8.8.8", allow_local=False) is True


def test_check_peer_unparseable_peer_allowed_for_listed_host():
    # P2: when getpeername returns an UNPARSEABLE peer but the connection's
    # host IS in the allowed-local set, _check_peer returns (allow) — the
    # deliberate design: an operator-allowed loopback host behind a
    # degenerate resolver must not be bricked. The paired negative (empty
    # allowed set) must raise and close the socket.
    class _FakeSock:
        def __init__(self, peer: str) -> None:
            self._peer = peer
            self.closed = False

        def getpeername(self) -> tuple[str, int]:
            return (self._peer, 9999)

        def close(self) -> None:
            self.closed = True

    # Positive: host listed in the allowed set -> unparseable peer allowed.
    allowed = _peer_checked_connection(
        urllib3.connection.HTTPConnection, frozenset({"127.0.0.1"})
    )
    conn = allowed("127.0.0.1", 80)
    stub = _FakeSock("not-an-ip")
    conn.sock = stub
    _check_peer(conn)  # must return normally
    assert stub.closed is False

    # Negative: same unparseable peer, host NOT listed -> fail closed.
    strict = _peer_checked_connection(urllib3.connection.HTTPConnection, frozenset())
    conn2 = strict("127.0.0.1", 80)
    stub2 = _FakeSock("not-an-ip")
    conn2.sock = stub2
    with pytest.raises(URLPolicyError):
        _check_peer(conn2)
    assert stub2.closed is True


def test_download_root_none_no_confinement(tmp_path, fast_policy, local_http_server):
    # download_root=None silently disables confinement (no pin walk, no
    # contextvar) — no warning by design. A real confined-less download must
    # complete ok with the file + manifest landed, and the confinement
    # contextvar must be reset to None afterward.
    with local_http_server() as srv:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok
        assert result.bytes_downloaded == 1000
        assert result.final_path == staging
        # File + manifest landed.
        assert staging.is_file()
        assert staging.read_bytes() == BODY_1000
        assert _manifest(staging)["complete"] is True
        # No confinement was active: the contextvar is back to its default.
        assert downloader._confined_dirs.get() is None


def test_install_url_policy_rebinds_allow_local_on_reuse():
    # P3: _install_url_policy is idempotent — reusing a session re-binds the
    # EXISTING hook to the new allow_local rather than stacking a second
    # hook. So a session first installed permissive (allow_local=True) and
    # later reused strict (allow_local=False) must end with exactly ONE
    # policy hook whose bound allow_local is False, and that hook must
    # reject a loopback redirect hop (strict despite the first install).
    session = requests.Session()
    downloader._install_url_policy(session, allow_local=True)
    downloader._install_url_policy(session, allow_local=False)

    policy_hooks = [
        h
        for h in session.hooks.get("response", [])
        if getattr(h, "func", None) is downloader._url_policy_hook
    ]
    assert len(policy_hooks) == 1, "the hook must be re-bound, not stacked"
    bound = policy_hooks[0]
    # allow_local is bound as a keyword on the stored partial.
    assert getattr(bound, "keywords", {}).get("allow_local") is False

    class _FakeRedirectResponse:
        # Minimal stand-in for the redirected response the hook inspects:
        # a loopback fetched URL with non-empty history (i.e. a redirect hop).
        url = "http://127.0.0.1:1/x"
        history = [object()]
        is_redirect = False
        headers = {}

    with pytest.raises(URLPolicyError):
        bound(_FakeRedirectResponse())


def test_unresolvable_url_reports_urlpolicyerror_structured(tmp_path, fast_policy):
    # P3: a hostname that does not resolve (RFC 6761 .invalid -> hermetic)
    # must fail at the pre-connect _assert_public_url as a structured
    # URLPolicyError outcome: nothing is written, no exception escapes.
    staging = tmp_path / "media.mp4.partial"
    result = download_verified(
        "http://media-archive-does-not-exist.invalid/media.bin",
        staging,
        expected_size=1000,
        timeout=fast_policy,
    )
    assert result.ok is False
    assert result.error_class == "URLPolicyError"
    assert result.error_message is not None
    assert result.final_path is None
    assert result.bytes_downloaded == 0
    # Nothing was created under tmp_path (the policy gate fails pre-IO).
    assert list(tmp_path.iterdir()) == []


def test_structured_failure_shape_pins_final_path_and_bytes(
    tmp_path, fast_policy, local_http_server
):
    # P3: pin the two failure shapes on the VerifiedDownload fields.
    #
    # (a) A body shorter than expected_size is a SizeMismatch whose
    #     final_path is the STAGING path (not None) and resumed is False:
    #     the bytes that were written are reported, at their real path.
    short = BODY_1000[:800]
    with local_http_server(content=short) as srv:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok is False
        assert result.error_class == "SizeMismatch"
        assert result.final_path == staging
        assert result.resumed is False
        assert result.bytes_downloaded == 800

    # (b) A genuine resume that then fails: seed a 400-byte partial whose
    #     identity matches a HEAD that LIES about the length (declares 1000,
    #     really serves 800). The resume decision sees the declared length
    #     (1000 > 400, matching validator) so it RESUMES (Range: bytes=400-),
    #     appending 400 more bytes -> 800 total. The final byte-count check
    #     then fails against the declared 1000. The failure must report a
    #     real resume (resumed True) with the partial bytes staged (0 <
    #     bytes < 1000).
    with local_http_server(content=BODY_1000[:800], head_content_length=1000) as srv:
        staging2 = tmp_path / "other.mp4.partial"
        sidecar2 = staging2.with_name(staging2.name + ".identity.json")
        _seed_partial(
            staging2, sidecar2, BODY_1000, offset=400, length=1000, etag='"v1"'
        )
        result2 = download_verified(
            srv.url(),
            staging2,
            expected_size=None,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result2.ok is False
        assert result2.error_class == "SizeMismatch"
        assert result2.resumed is True
        assert 0 < result2.bytes_downloaded < 1000


def test_multi_hop_redirect_chain_rejected(tmp_path, fast_policy, local_http_server):
    # P3: a redirect chain is re-validated PER HOP under the strict
    # policy. Two real loopback servers chained (srv1 -> srv2 -> a private
    # target) prove the hop re-validation: the chain is rejected at the
    # FIRST hop's Location (srv2's loopback target), which is checked
    # strictly.
    #
    # NOTE (adaptation to actual behavior): the spec anticipated the chain
    # failing on the SECOND hop's target (http://192.168.1.1/x), but the
    # per-hop re-validation stops the chain one hop earlier — hop-1's
    # Location (srv2, also loopback) is the strict-policy violation, so the
    # request never reaches srv2 and the 192.168.1.1 target is never
    # validated. We therefore pin the real contract: the failure is a
    # URLPolicyError (surfaced as RetriesExhausted after the bounded
    # retries) that names the loopback redirect target, and the unreachable
    # private target does NOT appear in the error. The load-bearing proof is
    # that a redirect hop's Location is validated strictly (allow_local does
    # not extend past the first hop), not just that the chain fails.
    with local_http_server() as srv, local_http_server() as second:
        second.set_redirect("http://192.168.1.1/x")
        srv.set_redirect(second.url())
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(),
            staging,
            expected_size=1000,
            timeout=fast_policy,
            allow_local=True,
        )
        assert result.ok is False
        assert result.error_class == "RetriesExhausted"
        msg = result.error_message or ""
        # The strict per-hop rejection surfaced the root cause ...
        assert "URLPolicyError" in msg
        # ... naming the loopback redirect target (a hop fetched at a
        # Location, checked strictly, not under allow_local) ...
        assert "127.0.0.1" in msg
        # ... and the unreachable private target was never validated.
        assert "192.168.1.1" not in msg

"""Tests for download_verified: identity-aware, resumable staged downloads.

Uses a local ThreadingHTTPServer (no external network) that can be configured
per test to exercise the full range of resume / restart / verify behavior.
"""

import http.server
import json
import socket
import threading
from contextlib import suppress
from pathlib import Path

import pytest
import requests

import media_archive_sync.downloader as downloader
from media_archive_sync import (
    TimeoutPolicy,
    VerifiedDownload,
    download_verified,
)
from media_archive_sync.downloader import _StagingFileChangedError

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
        if range_hdr and not cls._ignore_range:
            start = int(range_hdr.split("=", 1)[1].split("-")[0])
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
                self.send_header("ETag", cls._etag)
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=None, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), default_staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=None, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=None, timeout=fast_policy
        )
        assert result.ok is False
        assert result.error_class == "RangeNotSatisfiable"
        assert result.error_message is not None


def test_initial_url_policy_violations_fail_structured(
    tmp_path,
    fast_policy,
):
    # A bad initial URL (wrong scheme / userinfo) fails immediately as a
    # structured URLPolicyError outcome; no server is contacted, nothing
    # raises. allow_local=True permits loopback, so only scheme and userinfo
    # are exercised here.
    bad_urls = [
        "file:///etc/passwd",
        "ftp://127.0.0.1:8000/x",
        "http://user:pass@example.org/media.bin",
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=None, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=1000, timeout=fast_policy
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
            srv.url(), staging, expected_size=None, timeout=fast_policy
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
                srv.url(), staging, expected_size=1000, timeout=fast_policy
            )
            assert result.ok, payload
            assert result.resumed is False, payload
            assert result.bytes_downloaded == 1000, payload
            assert staging.read_bytes() == BODY_1000, payload
            # A fresh, valid sidecar was written by the successful download.
            assert _manifest(staging)["complete"] is True

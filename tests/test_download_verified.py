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

from media_archive_sync import (
    TimeoutPolicy,
    VerifiedDownload,
    download_verified,
)

# 1000-byte deterministic body.
BODY_1000 = bytes(range(256)) * 4
assert len(BODY_1000) == 1024
BODY_1000 = BODY_1000[:1000]
LM = "Wed, 07 Sep 2026 00:00:00 GMT"


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

    def do_HEAD(self):
        if type(self)._status >= 400:
            self._error()
            return
        content = type(self)._content
        self.send_response(200)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("ETag", type(self)._etag)
        self.send_header("Last-Modified", type(self)._last_modified)
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
        self.send_header("Content-Length", str(total))
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

    def change_content(self, content):
        self.handler._content = content

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


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


def test_new_download_success_and_identity_persisted(tmp_path, fast_policy):
    srv = LocalHttp()
    try:
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
    finally:
        srv.stop()


def test_range_resume_appends_and_verifies(tmp_path, fast_policy):
    srv = LocalHttp()
    try:
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
    finally:
        srv.stop()


def test_server_ignores_range_restarts_from_zero(tmp_path, fast_policy):
    srv = LocalHttp(ignore_range=True)
    try:
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
    finally:
        srv.stop()


def test_incorrect_content_range_start_restarts(tmp_path, fast_policy):
    srv = LocalHttp(wrong_range_start=True)
    try:
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
    finally:
        srv.stop()


def test_wrong_content_range_total_restarts(tmp_path, fast_policy):
    srv = LocalHttp(wrong_range_total=True)
    try:
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
    finally:
        srv.stop()


def test_remote_size_changed_discards_partial(tmp_path, fast_policy):
    # The remote now serves a 1500-byte object; the sidecar recorded 1000.
    new_content = bytes((i % 256) for i in range(1500))
    srv = LocalHttp(content=new_content, etag='"v1"')
    try:
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
    finally:
        srv.stop()


def test_etag_flip_discards_partial(tmp_path, fast_policy):
    srv = LocalHttp(etag='"v1"')
    try:
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
    finally:
        srv.stop()


def test_expected_size_mismatch_reports_sizemismatch(tmp_path, fast_policy):
    # Server only has 800 bytes but the caller expects 1000.
    short = BODY_1000[:800]
    srv = LocalHttp(content=short)
    try:
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
    finally:
        srv.stop()


def test_staged_partial_never_renamed(tmp_path, fast_policy):
    srv = LocalHttp()
    try:
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
    finally:
        srv.stop()


def test_identity_manifest_default_and_custom_location(tmp_path, fast_policy):
    srv = LocalHttp()
    try:
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
    finally:
        srv.stop()


def test_mid_body_abort_retries_with_backoff_and_resumes(tmp_path, fast_policy):
    # Seed a 400-byte partial; the first 206 resume aborts mid-body (after
    # 100 bytes), and the retry revalidates identity and resumes from the
    # new offset, completing the download.
    srv = LocalHttp(abort_gets=1, abort_bytes=100)
    try:
        staging = tmp_path / "media.mp4.partial"
        sidecar = staging.with_name(staging.name + ".identity.json")
        _seed_partial(staging, sidecar, BODY_1000, offset=400, length=1000, etag='"v1"')
        result = download_verified(
            srv.url(), staging, expected_size=1000, timeout=fast_policy
        )
        assert result.ok
        # The final read appended to the (extended) partial: a resume.
        assert result.resumed is True
        assert result.bytes_downloaded == 1000
        assert staging.read_bytes() == BODY_1000
        assert staging.stat().st_size == 1000
    finally:
        srv.stop()


def test_head_failure_reports_exhausted_without_raising(tmp_path, fast_policy):
    # Every HEAD/GET returns 500: retries are exhausted, no exception escapes.
    srv = LocalHttp(status=500)
    try:
        staging = tmp_path / "media.mp4.partial"
        result = download_verified(
            srv.url(), staging, expected_size=1000, timeout=fast_policy
        )
        assert result.ok is False
        assert result.error_class is not None
        assert result.error_message is not None
        assert result.final_path is None or result.final_path == staging
        assert not (staging.with_name("media.mp4.partial.identity.json")).is_file()
    finally:
        srv.stop()

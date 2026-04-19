"""Tests for extended crawler functions.

Covers crawl_remote, fetch_remote_page, save_media_meta_for_dir,
and parameterized variants of find_missing_to_append,
filter_cached_index_for_period, and is_file_too_old_for_download.
"""

import hashlib
import json
import time
from unittest.mock import Mock, patch

import pytest
import requests

from media_archive_sync.crawler import (
    crawl_remote,
    fetch_remote_page,
    filter_cached_index_for_period,
    find_missing_to_append,
    is_file_too_old_for_download,
    save_media_meta_for_dir,
)

# ============================================================================
# crawl_remote
# ============================================================================


class TestCrawlRemote:
    """Tests for crawl_remote convenience wrapper."""

    def test_delegates_to_crawl_archive(self):
        """crawl_remote calls crawl_archive with provided args."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        </body></html>
        """
        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            media_list, dir_counts = crawl_remote(remote_base="http://example.com/")
        assert len(media_list) == 1
        assert "video.mp4" in media_list[0][1]

    def test_default_max_depth_is_4(self):
        """crawl_remote uses max_depth=4 by default."""
        with patch("media_archive_sync.crawler.crawl_archive") as m:
            m.return_value = ([], {})
            crawl_remote(remote_base="http://example.com/")
        m.assert_called_once_with(
            start_dir=None,
            remote_base="http://example.com/",
            max_depth=4,
            video_extensions=None,
            progress_callback=None,
        )

    def test_passes_start_dir(self):
        """crawl_remote forwards start_dir to crawl_archive."""
        with patch("media_archive_sync.crawler.crawl_archive") as m:
            m.return_value = ([], {})
            crawl_remote(
                remote_base="http://example.com/",
                start_dir="http://example.com/jan/",
            )
        _, kwargs = m.call_args
        assert kwargs["start_dir"] == "http://example.com/jan/"

    def test_passes_custom_extensions(self):
        """crawl_remote forwards video_extensions to crawl_archive."""
        with patch("media_archive_sync.crawler.crawl_archive") as m:
            m.return_value = ([], {})
            crawl_remote(
                remote_base="http://example.com/",
                video_extensions={".mp4"},
            )
        _, kwargs = m.call_args
        assert kwargs["video_extensions"] == {".mp4"}

    def test_passes_progress_callback(self):
        """crawl_remote forwards progress_callback to crawl_archive."""

        def cb(url, depth):
            return None

        with patch("media_archive_sync.crawler.crawl_archive") as m:
            m.return_value = ([], {})
            crawl_remote(
                remote_base="http://example.com/",
                progress_callback=cb,
            )
        _, kwargs = m.call_args
        assert kwargs["progress_callback"] is cb


# ============================================================================
# fetch_remote_page
# ============================================================================


class TestFetchRemotePage:
    """Tests for fetch_remote_page (no extension filter)."""

    def test_returns_all_files_including_non_video(self):
        """fetch_remote_page returns all files, not just video extensions."""
        html = """
        <html><body>
        <a href="../">../</a>
        <a href="./">./</a>
        <a href="video.mp4">video.mp4</a>
        <a href="document.pdf">document.pdf</a>
        <a href="readme.txt">readme.txt</a>
        <a href="subdir/">subdir/</a>
        </body></html>
        """
        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_remote_page("http://example.com/dir/")
        names = [name for _, name in result]
        assert "video.mp4" in names
        assert "document.pdf" in names
        assert "readme.txt" in names
        assert "subdir/" not in names

    def test_skips_parent_and_self_links(self):
        """../ and ./ hrefs are excluded."""
        html = """
        <html><body>
        <a href="../">../</a>
        <a href="./">./</a>
        <a href="file.mp4">file.mp4</a>
        </body></html>
        """
        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_remote_page("http://example.com/dir/")
        for url, _ in result:
            assert "../" not in url
            assert "./" not in url

    def test_empty_html_returns_empty(self):
        """Empty HTML response yields empty list."""
        with patch("media_archive_sync.crawler.fetch_html", return_value=""):
            result = fetch_remote_page("http://example.com/dir/")
        assert result == []

    def test_url_decoded_filenames(self):
        """URL-encoded filenames are decoded."""
        html = """
        <html><body>
        <a href="video%20with%20spaces.mp4">video%20with%20spaces.mp4</a>
        </body></html>
        """
        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_remote_page("http://example.com/dir/")
        assert result[0][1] == "video with spaces.mp4"

    def test_blocks_different_domain(self):
        """URLs from different domains are filtered out."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        <a href="http://evil.com/malware.mp4">malware.mp4</a>
        </body></html>
        """
        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_remote_page("http://example.com/dir/")
        assert len(result) == 1
        assert "example.com" in result[0][0]

    def test_exception_returns_empty(self):
        """requests.RequestException is caught and returns empty list."""
        with patch(
            "media_archive_sync.crawler.fetch_html",
            side_effect=requests.RequestException("boom"),
        ):
            result = fetch_remote_page("http://example.com/dir/")
        assert result == []

    def test_programming_errors_not_swallowed(self):
        """Programming errors (TypeError, etc.) propagate, not swallowed."""
        with (
            patch(
                "media_archive_sync.crawler.fetch_html",
                side_effect=TypeError("bad type"),
            ),
            pytest.raises(TypeError),
        ):
            fetch_remote_page("http://example.com/dir/")


# ============================================================================
# save_media_meta_for_dir
# ============================================================================


class TestSaveMediaMetaForDir:
    """Tests for save_media_meta_for_dir (alias for save_metadata)."""

    def test_creates_meta_file(self, tmp_path):
        """Creates a JSON file with ETag, Last-Modified, and html_hash."""
        meta_file = tmp_path / "meta.json"
        dir_url = "http://example.com/dir/"

        mock_head = Mock()
        mock_head.status_code = 200
        mock_head.headers = {
            "ETag": '"abc"',
            "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
        }

        mock_get = Mock()
        mock_get.status_code = 200
        mock_get.content = b"<html>test</html>"

        with (
            patch("requests.head", return_value=mock_head),
            patch("requests.get", return_value=mock_get),
        ):
            save_media_meta_for_dir(dir_url, meta_file)

        data = json.loads(meta_file.read_text())
        assert data[dir_url]["etag"] == '"abc"'
        assert data[dir_url]["last_modified"] == "Mon, 01 Jan 2024 00:00:00 GMT"
        assert (
            data[dir_url]["html_hash"]
            == hashlib.sha256(b"<html>test</html>").hexdigest()
        )

    def test_preserves_existing_entries(self, tmp_path):
        """Updating meta preserves entries from other URLs."""
        meta_file = tmp_path / "meta.json"
        existing = {
            "http://other.url/": {
                "etag": "old",
                "last_modified": None,
                "html_hash": None,
            }
        }
        meta_file.write_text(json.dumps(existing))

        dir_url = "http://example.com/dir/"
        mock_head = Mock()
        mock_head.status_code = 200
        mock_head.headers = {"ETag": '"new"'}

        mock_get = Mock()
        mock_get.status_code = 200
        mock_get.content = b"html"

        with (
            patch("requests.head", return_value=mock_head),
            patch("requests.get", return_value=mock_get),
        ):
            save_media_meta_for_dir(dir_url, meta_file)

        data = json.loads(meta_file.read_text())
        assert "http://other.url/" in data
        assert dir_url in data


# ============================================================================
# find_missing_to_append — match_by parameter
# ============================================================================


class TestFindMissingMatchByInvalid:
    """Tests for invalid match_by values."""

    def test_invalid_match_by_raises_value_error(self):
        """Invalid match_by raises ValueError."""
        with pytest.raises(ValueError, match="Unknown match_by"):
            find_missing_to_append([], [], match_by="invalid")


class TestFindMissingMatchByTuple:
    """Default match_by='tuple' matches by full (url, name) tuple."""

    def test_different_url_same_name_is_new(self):
        """Same name at different URL is considered new."""
        cached = [("http://old.com/1.mp4", "1.mp4")]
        month = [("http://new.com/1.mp4", "1.mp4")]
        result = find_missing_to_append(cached, month, match_by="tuple")
        assert len(result) == 1

    def test_identical_tuple_excluded(self):
        """Identical tuples are excluded."""
        item = ("http://a.com/1.mp4", "1.mp4")
        result = find_missing_to_append([item], [item], match_by="tuple")
        assert result == []

    def test_none_inputs_return_empty(self):
        """None inputs return empty list."""
        assert find_missing_to_append(None, None, match_by="tuple") == []


class TestFindMissingMatchByName:
    """match_by='name' matches by decoded_name only (legacy local behaviour)."""

    def test_same_name_different_url_is_existing(self):
        """Same name at different URL is considered existing."""
        cached = [("http://old.com/1.mp4", "1.mp4")]
        month = [("http://new.com/1.mp4", "1.mp4")]
        result = find_missing_to_append(cached, month, match_by="name")
        assert result == []

    def test_different_name_is_new(self):
        """Different name is considered new."""
        cached = [("http://a.com/1.mp4", "1.mp4")]
        month = [("http://a.com/2.mp4", "2.mp4")]
        result = find_missing_to_append(cached, month, match_by="name")
        assert len(result) == 1
        assert result[0][1] == "2.mp4"

    def test_preserves_order(self):
        """Order of month_items is preserved."""
        cached = [("url", "2.mp4")]
        month = [
            ("url1", "1.mp4"),
            ("url2", "2.mp4"),
            ("url3", "3.mp4"),
        ]
        result = find_missing_to_append(cached, month, match_by="name")
        assert len(result) == 2
        assert result[0][1] == "1.mp4"
        assert result[1][1] == "3.mp4"

    def test_none_inputs_return_empty(self):
        """None inputs return empty list."""
        assert find_missing_to_append(None, None, match_by="name") == []


# ============================================================================
# filter_cached_index_for_period — normalize_keys parameter
# ============================================================================


class TestFilterCachedIndexNormalizeKeys:
    """Tests for normalize_keys parameter."""

    def test_normalize_keys_true_handles_missing_trailing_slash(self):
        """normalize_keys=True finds keys without trailing slashes."""
        ml = [
            ("http://a.com/jan/1.mp4", "1.mp4"),
            ("http://a.com/feb/2.mp4", "2.mp4"),
        ]
        dc = {"http://a.com/jan": 1, "http://a.com/feb/": 1}
        result_ml, result_dc, prepared = filter_cached_index_for_period(
            ml, dc, "http://a.com/jan/", normalize_keys=True
        )
        assert len(result_ml) == 1
        assert prepared is True

    def test_normalize_keys_false_requires_exact_match(self):
        """normalize_keys=False requires exact key match (no normalization)."""
        ml = [
            ("http://a.com/jan/1.mp4", "1.mp4"),
            ("http://a.com/feb/2.mp4", "2.mp4"),
        ]
        dc = {"http://a.com/jan": 1, "http://a.com/feb/": 1}
        result_ml, result_dc, prepared = filter_cached_index_for_period(
            ml, dc, "http://a.com/jan/", normalize_keys=False
        )
        # "http://a.com/jan/" not in dc (dc has "http://a.com/jan" without slash)
        assert prepared is False
        assert len(result_ml) == 2

    def test_normalize_keys_false_exact_match_works(self):
        """normalize_keys=False works when keys match exactly."""
        ml = [
            ("http://a.com/jan/1.mp4", "1.mp4"),
            ("http://a.com/feb/2.mp4", "2.mp4"),
        ]
        dc = {"http://a.com/jan/": 1, "http://a.com/feb/": 1}
        result_ml, result_dc, prepared = filter_cached_index_for_period(
            ml, dc, "http://a.com/jan/", normalize_keys=False
        )
        assert len(result_ml) == 1
        assert prepared is True

    def test_normalize_keys_false_checks_count_gt_zero(self):
        """normalize_keys=False requires dir_counts value > 0."""
        ml = [("http://a.com/jan/1.mp4", "1.mp4")]
        dc = {"http://a.com/jan/": 0}
        result_ml, result_dc, prepared = filter_cached_index_for_period(
            ml, dc, "http://a.com/jan/", normalize_keys=False
        )
        assert prepared is False

    def test_normalize_keys_true_zero_count_returns_not_prepared(self):
        """normalize_keys=True with zero count returns prepared=False."""
        ml = [("http://a.com/jan/1.mp4", "1.mp4")]
        dc = {"http://a.com/jan/": 0}
        _, _, prepared = filter_cached_index_for_period(
            ml, dc, "http://a.com/jan/", normalize_keys=True
        )
        assert prepared is False

    def test_no_periodic_dir_returns_unfiltered(self):
        """No periodic_dir returns original data regardless of normalize_keys."""
        ml = [("http://a.com/1.mp4", "1.mp4")]
        dc = {"http://a.com/": 1}
        for nk in (True, False):
            result_ml, result_dc, prepared = filter_cached_index_for_period(
                ml, dc, None, normalize_keys=nk
            )
            assert result_ml == ml
            assert prepared is False


# ============================================================================
# is_file_too_old_for_download — fail_closed parameter
# ============================================================================


class TestIsFileTooOldFailClosed:
    """Tests for fail_closed parameter."""

    def test_fail_closed_true_with_defaults_returns_true(self):
        """fail_closed=True with default params blocks download."""
        result = is_file_too_old_for_download(
            "http://example.com/video.mp4",
            "video.mp4",
            fail_closed=True,
        )
        assert result is True

    def test_fail_closed_false_with_defaults_allows(self):
        """fail_closed=False with default params allows download."""
        result = is_file_too_old_for_download(
            "http://example.com/video.mp4",
            "video.mp4",
            fail_closed=False,
        )
        assert result is False

    def test_fail_closed_true_with_explicit_max_age_uses_max_age(self):
        """fail_closed=True with explicit max_age_days uses that value."""
        recent_epoch = int(time.time()) - 3600
        url = f"http://example.com/{recent_epoch}/video.mp4"
        result = is_file_too_old_for_download(
            url,
            "video.mp4",
            max_age_days=30,
            fail_closed=True,
        )
        assert result is False

    def test_fail_closed_with_explicit_max_age_zero_allows(self):
        """max_age_days=0 with fail_closed=True allows any download."""
        result = is_file_too_old_for_download(
            "http://example.com/video.mp4",
            "video.mp4",
            max_age_days=0,
            fail_closed=True,
        )
        assert result is False

    def test_default_max_age_none_no_limit(self):
        """Default max_age_days=None means no age limit."""
        result = is_file_too_old_for_download(
            "http://example.com/video.mp4",
            "video.mp4",
        )
        assert result is False

    def test_fail_closed_true_with_allow_old_allows(self):
        """fail_closed=True with allow_old_downloads=True allows download."""
        result = is_file_too_old_for_download(
            "http://example.com/video.mp4",
            "video.mp4",
            allow_old_downloads=True,
            fail_closed=True,
        )
        assert result is False

    def test_recent_file_not_too_old(self):
        """A recent file is not too old regardless of fail_closed."""
        recent_epoch = int(time.time()) - 3600
        url = f"http://example.com/{recent_epoch}/video.mp4"
        for fc in (True, False):
            result = is_file_too_old_for_download(
                url,
                "video.mp4",
                max_age_days=30,
                fail_closed=fc,
            )
            assert result is False

    def test_old_file_is_too_old(self):
        """An old file exceeds max_age_days regardless of fail_closed."""
        old_epoch = int(time.time()) - 86400 * 2
        url = f"http://example.com/{old_epoch}/video.mp4"
        for fc in (True, False):
            result = is_file_too_old_for_download(
                url,
                "video.mp4",
                max_age_days=1,
                fail_closed=fc,
            )
            assert result is True

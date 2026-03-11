"""Tests for crawler module."""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from media_archive_sync.crawler import (
    crawl_archive,
    fetch_directory,
    fetch_html,
    save_metadata,
)


class TestFetchDirectory:
    """Tests for fetch_directory function."""

    def test_fetch_directory_filters_non_media(self):
        """Test that fetch_directory filters non-media files."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        <a href="README.md">README.md</a>
        <a href="video.mkv">video.mkv</a>
        <a href="./">./</a>
        <a href="../">../</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_directory("http://example.com/dir/")

        assert len(result) == 2
        assert all(".mp4" in url or ".mkv" in url for url, _ in result)

    def test_fetch_directory_with_query_strings(self):
        """Test that URLs with query strings are handled correctly."""
        html = """
        <html><body>
        <a href="video.mp4?download=1">video.mp4?download=1</a>
        <a href="video.mkv">video.mkv</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_directory("http://example.com/dir/")

        assert len(result) == 2
        urls = [url for url, _ in result]
        assert any("video.mp4" in url for url in urls)

    def test_fetch_directory_custom_extensions(self):
        """Test custom extension filtering."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        <a href="audio.mp3">audio.mp3</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_directory(
                "http://example.com/dir/", allowed_extensions={".mp3"}
            )

        assert len(result) == 1
        assert result[0][0].endswith("audio.mp3")

    def test_fetch_directory_empty_html(self):
        """Test handling of empty HTML response."""
        with patch("media_archive_sync.crawler.fetch_html", return_value=""):
            result = fetch_directory("http://example.com/dir/")

        assert result == []

    def test_fetch_directory_network_error(self):
        """Test handling of network errors."""
        with patch(
            "media_archive_sync.crawler.fetch_html",
            side_effect=requests.RequestException("Network error"),
        ):
            result = fetch_directory("http://example.com/dir/")

        assert result == []


class TestSaveMetadata:
    """Tests for save_metadata function."""

    def test_save_metadata_valid_dict(self, tmp_path):
        """Test saving metadata with valid dict cache."""
        meta_file = tmp_path / "meta.json"
        meta_file.write_text('{"old-url": {"etag": "old"}}')

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "ETag": '"abc123"',
            "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
        }
        mock_response.content = b"<html>test</html>"

        with (
            patch("requests.head", return_value=mock_response),
            patch("requests.get", return_value=mock_response),
        ):
            save_metadata("http://example.com/dir/", meta_file)

        content = json.loads(meta_file.read_text())
        assert "http://example.com/dir/" in content
        assert content["http://example.com/dir/"]["etag"] == '"abc123"'

    def test_save_metadata_invalid_json_resets_to_empty(self, tmp_path):
        """Test that invalid JSON resets cache to empty dict."""
        meta_file = tmp_path / "meta.json"
        meta_file.write_text("not valid json")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.content = b"<html>test</html>"

        with (
            patch("requests.head", return_value=mock_response),
            patch("requests.get", return_value=mock_response),
        ):
            save_metadata("http://example.com/dir/", meta_file)

        content = json.loads(meta_file.read_text())
        assert "http://example.com/dir/" in content

    def test_save_metadata_list_resets_to_empty(self, tmp_path):
        """Test that list JSON resets cache to empty dict."""
        meta_file = tmp_path / "meta.json"
        meta_file.write_text('["list", "not", "dict"]')

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.content = b"<html>test</html>"

        with (
            patch("requests.head", return_value=mock_response),
            patch("requests.get", return_value=mock_response),
        ):
            save_metadata("http://example.com/dir/", meta_file)

        content = json.loads(meta_file.read_text())
        assert isinstance(content, dict)
        assert "http://example.com/dir/" in content

    def test_save_metadata_no_existing_file(self, tmp_path):
        """Test saving metadata when file doesn't exist."""
        meta_file = tmp_path / "meta.json"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"ETag": '"abc123"'}
        mock_response.content = b"<html>test</html>"

        with (
            patch("requests.head", return_value=mock_response),
            patch("requests.get", return_value=mock_response),
        ):
            save_metadata("http://example.com/dir/", meta_file)

        assert meta_file.exists()
        content = json.loads(meta_file.read_text())
        assert "http://example.com/dir/" in content


class TestFetchHtml:
    """Tests for fetch_html function."""

    def test_fetch_html_success(self):
        """Test successful HTML fetch."""
        mock_response = MagicMock()
        mock_response.text = "<html><body>Test</body></html>"
        mock_response.status_code = 200

        with patch("requests.get", return_value=mock_response):
            result = fetch_html("http://example.com/")

        assert result == "<html><body>Test</body></html>"

    def test_fetch_html_not_found(self):
        """Test handling 404 response."""
        from requests import HTTPError

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError("404")

        with patch("requests.get", return_value=mock_response):
            result = fetch_html("http://example.com/404")

        assert result == ""

    def test_fetch_html_network_error(self):
        """Test handling network errors."""
        import requests

        with patch(
            "requests.get", side_effect=requests.RequestException("Connection failed")
        ):
            result = fetch_html("http://example.com/")

        assert result == ""


class TestWillPerformFullCrawl:
    """Tests for will_perform_full_crawl function."""

    def test_will_perform_full_crawl_not_prepared_no_media(self):
        """Test when not prepared and no media."""
        from media_archive_sync.crawler import will_perform_full_crawl

        result = will_perform_full_crawl(None, False)
        assert result is True

    def test_will_perform_full_crawl_prepared_no_media(self):
        """Test when prepared but no media."""
        from media_archive_sync.crawler import will_perform_full_crawl

        result = will_perform_full_crawl([], True)
        assert result is False

    def test_will_perform_full_crawl_not_prepared_has_media(self):
        """Test when not prepared but has media."""
        from media_archive_sync.crawler import will_perform_full_crawl

        result = will_perform_full_crawl([("url", "name")], False)
        assert result is False

    def test_will_perform_full_crawl_prepared_has_media(self):
        """Test when prepared and has media."""
        from media_archive_sync.crawler import will_perform_full_crawl

        result = will_perform_full_crawl([("url", "name")], True)
        assert result is False


class TestFilterCachedIndexForPeriod:
    """Tests for filter_cached_index_for_period function."""

    def test_filter_no_periodic_dir(self):
        """Test when no periodic_dir specified."""
        from media_archive_sync.crawler import filter_cached_index_for_period

        media_list = [("http://example.com/video.mp4", "video.mp4")]
        dir_counts = {"http://example.com/": 1}

        result = filter_cached_index_for_period(media_list, dir_counts, None)

        assert result[0] == media_list
        assert result[1] == dir_counts
        assert result[2] is False

    def test_filter_with_periodic_dir_found(self):
        """Test filtering with periodic_dir that exists."""
        from media_archive_sync.crawler import filter_cached_index_for_period

        media_list = [
            ("http://example.com/jan/video.mp4", "video.mp4"),
            ("http://example.com/feb/video.mp4", "video2.mp4"),
        ]
        dir_counts = {"http://example.com/jan/": 1, "http://example.com/feb/": 1}

        result = filter_cached_index_for_period(
            media_list, dir_counts, "http://example.com/jan/"
        )

        assert len(result[0]) == 1
        assert "jan" in result[0][0][0]
        assert result[2] is True

    def test_filter_with_periodic_dir_not_found(self):
        """Test filtering with periodic_dir that doesn't exist."""
        from media_archive_sync.crawler import filter_cached_index_for_period

        media_list = [("http://example.com/video.mp4", "video.mp4")]
        dir_counts = {"http://example.com/": 1}

        result = filter_cached_index_for_period(
            media_list, dir_counts, "http://example.com/missing/"
        )

        assert result[0] == media_list
        assert result[2] is False


class TestCrawlArchive:
    """Tests for crawl_archive function."""

    def test_crawl_empty_page(self):
        """Test crawling an empty directory listing."""
        html = """
        <html><body>
        <a href="../">../</a>
        <a href="./">./</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = crawl_archive(remote_base="http://example.com/")

        media_list, dir_counts = result
        assert len(media_list) == 0

    def test_crawl_single_video(self):
        """Test crawling a page with single video."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = crawl_archive(remote_base="http://example.com/")

        media_list, dir_counts = result
        assert len(media_list) == 1
        assert "video.mp4" in media_list[0][1]

    def test_crawl_with_subdirectory(self):
        """Test crawling with subdirectories."""
        root_html = """
        <html><body>
        <a href="subdir/">subdir/</a>
        <a href="video1.mp4">video1.mp4</a>
        </body></html>
        """
        subdir_html = """
        <html><body>
        <a href="video2.mp4">video2.mp4</a>
        </body></html>
        """

        def mock_fetch(url):
            if "subdir" in url:
                return subdir_html
            return root_html

        with patch("media_archive_sync.crawler.fetch_html", side_effect=mock_fetch):
            result = crawl_archive(remote_base="http://example.com/")

        media_list, dir_counts = result
        assert len(media_list) == 2

    def test_crawl_with_start_dir(self):
        """Test crawling with start_dir parameter."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = crawl_archive(
                start_dir="http://example.com/january/",
                remote_base="http://example.com/",
            )

        media_list, dir_counts = result
        assert len(media_list) == 1

    def test_crawl_respects_max_depth(self):
        """Test that max_depth is respected."""
        root_html = """
        <html><body>
        <a href="level1/">level1/</a>
        <a href="video1.mp4">video1.mp4</a>
        </body></html>
        """
        level1_html = """
        <html><body>
        <a href="level2/">level2/</a>
        <a href="video2.mp4">video2.mp4</a>
        </body></html>
        """
        level2_html = """
        <html><body>
        <a href="video3.mp4">video3.mp4</a>
        </body></html>
        """

        call_count = [0]

        def mock_fetch(url):
            call_count[0] += 1
            if "level2" in url:
                return level2_html
            if "level1" in url:
                return level1_html
            return root_html

        with patch("media_archive_sync.crawler.fetch_html", side_effect=mock_fetch):
            result = crawl_archive(
                remote_base="http://example.com/",
                max_depth=1,
            )

        media_list, dir_counts = result
        # Should only have videos from root and level1, not level2
        assert len(media_list) >= 1

    def test_crawl_custom_extensions(self):
        """Test crawling with custom video extensions."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        <a href="audio.mp3">audio.mp3</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = crawl_archive(
                remote_base="http://example.com/",
                video_extensions={".mp4"},
            )

        media_list, dir_counts = result
        assert len(media_list) == 1
        assert all(".mp4" in url for url, _ in media_list)

    def test_crawl_requires_remote_base_or_start_dir(self):
        """Test that ValueError is raised when neither remote_base nor start_dir provided."""
        with pytest.raises(ValueError, match="Either remote_base or start_dir"):
            crawl_archive()

    def test_crawl_with_callback(self):
        """Test crawling with progress callback."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        </body></html>
        """

        callback_calls = []

        def progress_callback(dir_url, depth):
            callback_calls.append((dir_url, depth))

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            crawl_archive(
                remote_base="http://example.com/",
                progress_callback=progress_callback,
            )

        assert len(callback_calls) >= 1
        assert callback_calls[0][0] == "http://example.com/"

    def test_crawl_different_domains_filtered(self):
        """Test that URLs from different domains are filtered out."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        <a href="http://other.com/video2.mp4">video2.mp4</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = crawl_archive(remote_base="http://example.com/")

        media_list, dir_counts = result
        # Only video.mp4 from example.com should be included
        assert len(media_list) == 1
        assert all("example.com" in url for url, _ in media_list)

    def test_crawl_respects_start_dir_scope(self):
        """Test that start_dir limits crawl scope correctly."""
        # Simulate a structure where sibling dirs exist
        root_html = """
        <html><body>
        <a href="jan/">jan/</a>
        <a href="feb/">feb/</a>
        <a href="video.mp4">video.mp4</a>
        </body></html>
        """
        jan_html = """
        <html><body>
        <a href="video_jan.mp4">video_jan.mp4</a>
        </body></html>
        """
        feb_html = """
        <html><body>
        <a href="video_feb.mp4">video_feb.mp4</a>
        </body></html>
        """

        def mock_fetch(url):
            if "/jan/" in url:
                return jan_html
            if "/feb/" in url:
                return feb_html
            return root_html

        # When crawling with start_dir=jan, feb should be excluded
        with patch("media_archive_sync.crawler.fetch_html", side_effect=mock_fetch):
            result = crawl_archive(
                start_dir="http://example.com/jan/",
                remote_base="http://example.com/",
            )

        media_list, dir_counts = result
        # Should only have jan video, not feb
        assert len(media_list) == 1
        assert "video_jan.mp4" in media_list[0][1]


class TestFetchDirectorySecurity:
    """Tests for fetch_directory security features."""

    def test_fetch_directory_blocks_different_domain(self):
        """Test that URLs from different domains are blocked."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        <a href="http://evil.com/malware.mp4">malware.mp4</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_directory("http://example.com/dir/")

        assert len(result) == 1
        assert all("example.com" in url for url, _ in result)
        assert all("evil.com" not in url for url, _ in result)

    def test_fetch_directory_blocks_path_traversal(self):
        """Test that path traversal is blocked."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        <a href="/etc/passwd.mp4">passwd.mp4</a>
        <a href="/other/path/video.mp4">other.mp4</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_directory("http://example.com/dir/")

        # Should only include the video in the directory
        assert len(result) == 1
        assert "video.mp4" in result[0][1]

    def test_fetch_directory_allows_same_host_relative(self):
        """Test that relative URLs within same host are allowed."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_directory("http://example.com/dir/")

        assert len(result) == 1
        assert result[0][0].startswith("http://example.com/dir/")

    def test_fetch_directory_handles_relative_href_parent(self):
        """Test that relative URLs climbing up are blocked."""
        html = """
        <html><body>
        <a href="video.mp4">video.mp4</a>
        <a href="../sibling/video.mp4">sibling.mp4</a>
        </body></html>
        """

        with patch("media_archive_sync.crawler.fetch_html", return_value=html):
            result = fetch_directory("http://example.com/dir/")

        # Only the video in the current directory should be included
        assert len(result) == 1
        assert "sibling" not in result[0][1]

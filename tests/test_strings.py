"""Tests for string utilities."""

from media_archive_sync.strings import (
    normalise_string,
    sanitize_title_for_filename,
    urldecode,
)


class TestUrldecode:
    """Tests for urldecode function."""

    def test_simple_url_encoding(self):
        """Test decoding simple URL-encoded strings."""
        assert urldecode("hello%20world") == "hello world"
        assert urldecode("foo%3Abar") == "foo:bar"

    def test_non_encoded(self):
        """Test non-encoded strings pass through."""
        assert urldecode("hello") == "hello"
        assert urldecode("hello world") == "hello world"

    def test_mixed(self):
        """Test mixed encoded and non-encoded."""
        assert urldecode("video%20title.mp4") == "video title.mp4"


class TestNormaliseString:
    """Tests for normalise_string function."""

    def test_lowercase(self):
        """Test conversion to lowercase."""
        assert normalise_string("HELLO") == "hello"
        assert normalise_string("Hello World") == "hello world"

    def test_strip_punctuation(self):
        """Test punctuation is replaced with spaces."""
        assert normalise_string("hello.world") == "hello world"
        assert normalise_string("foo-bar") == "foo bar"

    def test_strip_extension(self):
        """Test file extensions are stripped."""
        assert normalise_string("video.mp4") == "video"
        assert normalise_string("movie.mkv") == "movie"

    def test_collapse_whitespace(self):
        """Test multiple spaces collapsed."""
        assert normalise_string("hello   world") == "hello world"

    def test_empty_input(self):
        """Test empty/None input returns empty string."""
        assert normalise_string("") == ""
        assert normalise_string(None) == ""


class TestSanitizeTitleForFilename:
    """Tests for sanitize_title_for_filename function."""

    def test_basic_sanitization(self):
        """Test basic filename sanitization."""
        result = sanitize_title_for_filename("My Video Title")
        assert result == "My.Video.Title"

    def test_custom_replacements(self):
        """Test custom replacement dictionary."""
        replacements = {"&": "and", "?": ""}
        result = sanitize_title_for_filename(
            "Foo & Bar? Baz", replacements=replacements
        )
        assert "and" in result
        assert "?" not in result

    def test_strip_tokens(self):
        """Test token stripping - tokens must start with '!' to be stripped."""
        # Tokens with '!' prefix should be stripped
        result = sanitize_title_for_filename(
            "Video !gg !tts", strip_tokens={"gg", "tts"}
        )
        assert "!gg" not in result
        assert "!tts" not in result
        # Tokens without '!' prefix should NOT be stripped
        result = sanitize_title_for_filename("Video gg tts", strip_tokens={"gg", "tts"})
        assert "gg" in result
        assert "tts" in result

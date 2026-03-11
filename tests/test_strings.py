"""Tests for string utilities."""

import pytest

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

    def test_strip_ad_tag(self):
        """Test that #ad is stripped."""
        result = sanitize_title_for_filename("Video Title #ad", strip_tokens={"ad"})
        assert "#ad" not in result

    def test_strip_bang_suffix(self):
        """Test stripping bang suffixes like word!tag."""
        result = sanitize_title_for_filename("Video D!gg", strip_tokens={"gg"})
        assert "D!gg" not in result
        assert "D" in result

    def test_keep_bang_at_start(self):
        """Test that bang tokens at start with uppercase are kept."""
        result = sanitize_title_for_filename(
            "!KEEP Video",
            strip_tokens={"gg"},  # !KEEP not in tokens
        )
        # Uppercase bang at start should be kept
        assert "KEEP" in result

    def test_remove_lowercase_bang_at_start(self):
        """Test that lowercase bang tokens at start are removed."""
        result = sanitize_title_for_filename("!keep Video", strip_tokens={"keep"})
        # Lowercase bang token should be removed
        assert "keep" not in result.lower()
        assert "Video" in result

    def test_empty_title(self):
        """Test empty title returns no.title."""
        result = sanitize_title_for_filename("")
        assert result == "no.title"

    def test_only_special_chars(self):
        """Test title with only special chars returns no.title."""
        result = sanitize_title_for_filename("!@#$%")
        assert result == "no.title"

    def test_very_long_title(self):
        """Test very long titles are truncated."""
        long_title = "a" * 200
        result = sanitize_title_for_filename(long_title)
        assert len(result) <= 120

    def test_leading_trailing_dots(self):
        """Test leading and trailing dots are stripped."""
        result = sanitize_title_for_filename("...Hello World...")
        assert not result.startswith(".")
        assert not result.endswith(".")


class TestUrldecodeExtended:
    """Extended tests for urldecode function."""

    def test_urldecode_type_error(self):
        """Test that non-string input raises TypeError."""
        with pytest.raises(TypeError):
            urldecode(12345)

    def test_urldecode_empty(self):
        """Test decoding empty string."""
        assert urldecode("") == ""

    def test_urldecode_complex(self):
        """Test decoding complex URL."""
        url = "https%3A%2F%2Fexample.com%2Fpath%3Fquery%3Dvalue"
        expected = "https://example.com/path?query=value"
        assert urldecode(url) == expected

    def test_urldecode_unicode(self):
        """Test decoding unicode characters."""
        assert urldecode("%C3%A9") == "é"


class TestNormaliseStringExtended:
    """Extended tests for normalise_string function."""

    def test_normalise_with_underscore(self):
        """Test underscore is treated as separator."""
        assert normalise_string("hello_world") == "hello world"

    def test_normalise_multiple_extensions(self):
        """Test with multiple dots in filename."""
        assert normalise_string("movie.2024.mp4") == "movie 2024"

    def test_normalise_unknown_extension(self):
        """Test unknown extension is kept."""
        assert normalise_string("file.xyz") == "file xyz"

    def test_normalise_unicode(self):
        """Test unicode characters."""
        assert normalise_string("Héllo Wörld") == "héllo wörld"

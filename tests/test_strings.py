"""Tests for string utilities."""

import pytest

from media_archive_sync.strings import (
    _strip_bang_tokens,
    normalise_string,
    sanitize_dir_name,
    sanitize_title_for_filename,
    server_basename_variants,
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

    def test_dot_in_name_with_explicit_extensions(self):
        """Test dot in name is replaced with space when using explicit set."""
        assert (
            normalise_string("hello.world", video_extensions={"mp4"}) == "hello world"
        )

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
        """Test unknown extension is kept by built-in set (default None)."""
        assert normalise_string("file.xyz") == "file xyz"

    def test_normalise_unknown_extension_explicit_set(self):
        """Test unknown extension is kept when using explicit set."""
        assert normalise_string("file.xyz", video_extensions={"mp4"}) == "file xyz"

    def test_normalise_unicode(self):
        """Test unicode characters."""
        assert normalise_string("Héllo Wörld") == "héllo wörld"


class TestNormaliseStringVideoExtensions:
    """Tests for normalise_string video_extensions parameter."""

    def test_default_none_uses_builtin_set(self):
        """When video_extensions is None, built-in set strips known media extensions."""
        assert normalise_string("video.mp4") == "video"

    def test_default_none_regex_strips_short_ext(self):
        """Built-in set keeps unknown extensions."""
        assert normalise_string("file.xyz") == "file xyz"

    def test_default_none_regex_keeps_long_ext(self):
        """Built-in set keeps unknown extensions."""
        assert normalise_string("file.abcdefg") == "file abcdefg"

    def test_default_strips_known_media_extensions(self):
        """Built-in set strips known media extensions."""
        assert normalise_string("video.mp4") == "video"
        assert normalise_string("video.flv") == "video"

    def test_default_keeps_unknown_extensions(self):
        """Built-in set keeps extensions not in the set."""
        assert normalise_string("hello.world") == "hello world"
        assert normalise_string("Dr.House") == "dr house"

    def test_explicit_set_strips_known(self):
        """Explicit set strips only extensions in the set."""
        exts = {"mp4", "mkv"}
        assert normalise_string("video.mp4", video_extensions=exts) == "video"
        assert normalise_string("video.mkv", video_extensions=exts) == "video"

    def test_explicit_set_keeps_unknown(self):
        """Explicit set keeps extensions not in the set."""
        exts = {"mp4", "mkv"}
        assert normalise_string("file.xyz", video_extensions=exts) == "file xyz"

    def test_explicit_set_empty_strips_nothing(self):
        """Empty set strips no extensions."""
        assert normalise_string("video.mp4", video_extensions=set()) == "video mp4"

    def test_backward_compat_default_behaviour(self):
        """Default (None) uses built-in set which keeps unknown extensions."""
        assert normalise_string("video.mp4") == "video"
        assert normalise_string("video.xyz") == "video xyz"
        assert normalise_string("video.abcdefg") == "video abcdefg"


class TestStripBangTokensStrictVsLenient:
    """Tests for _strip_bang_tokens strict_tokens parameter."""

    def test_strict_default_strips_known_only(self):
        """strict_tokens=True (default) strips only tokens in the set."""
        assert _strip_bang_tokens("hello !gg world", {"gg"}) == "hello world"
        assert _strip_bang_tokens("hello !xyz world", {"gg"}) == "hello !xyz world"

    def test_lenient_strips_any_lowercase(self):
        """strict_tokens=False strips any lowercase suffix after '!'."""
        assert (
            _strip_bang_tokens("hello !gg world", {"gg"}, strict_tokens=False)
            == "hello world"
        )
        assert (
            _strip_bang_tokens("hello !xyz world", {"gg"}, strict_tokens=False)
            == "hello world"
        )

    def test_strict_inline_known_only(self):
        """strict_tokens=True strips inline known suffixes only."""
        assert _strip_bang_tokens("word!gg test", {"gg"}) == "word test"
        assert _strip_bang_tokens("word!xyz test", {"gg"}) == "word!xyz test"

    def test_lenient_inline_any_lowercase(self):
        """strict_tokens=False strips inline any lowercase suffix."""
        assert (
            _strip_bang_tokens("word!gg test", {"gg"}, strict_tokens=False)
            == "word test"
        )
        assert (
            _strip_bang_tokens("word!xyz test", {"gg"}, strict_tokens=False)
            == "word test"
        )

    def test_both_keep_uppercase_inline(self):
        """Neither mode strips uppercase inline suffixes."""
        assert _strip_bang_tokens("word!XYZ test", {"gg"}) == "word!XYZ test"
        assert (
            _strip_bang_tokens("word!XYZ test", {"gg"}, strict_tokens=False)
            == "word!XYZ test"
        )

    def test_lenient_standalone_lowercase_bang(self):
        """strict_tokens=False strips standalone lowercase !token even if not in set."""
        assert (
            _strip_bang_tokens("hello !xyz world", set(), strict_tokens=False)
            == "hello world"
        )

    def test_strict_standalone_unknown_kept(self):
        """strict_tokens=True keeps standalone !xyz when not in set."""
        assert _strip_bang_tokens("hello !xyz world", set()) == "hello !xyz world"

    def test_hash_ad_stripped_both_modes(self):
        """Both modes strip '#ad'."""
        assert _strip_bang_tokens("hello #ad world", {"gg"}) == "hello world"
        assert (
            _strip_bang_tokens("hello #ad world", {"gg"}, strict_tokens=False)
            == "hello world"
        )

    def test_default_is_strict(self):
        """Default strict_tokens is True."""
        result_default = _strip_bang_tokens("hello !xyz world", {"gg"})
        result_explicit = _strip_bang_tokens(
            "hello !xyz world", {"gg"}, strict_tokens=True
        )
        assert result_default == result_explicit

    def test_lenient_strips_numeric_bang_tokens(self):
        """strict_tokens=False strips numeric bang tokens."""
        assert _strip_bang_tokens("test !123", set(), strict_tokens=False) == "test"


class TestSanitizeDirName:
    """Tests for sanitize_dir_name function."""

    def test_none_returns_untitled(self):
        assert sanitize_dir_name(None) == "Untitled"

    def test_empty_returns_untitled(self):
        assert sanitize_dir_name("") == "Untitled"

    def test_whitespace_only_returns_untitled(self):
        assert sanitize_dir_name("   ") == "Untitled"

    def test_slash_replaced_with_dash_space(self):
        assert sanitize_dir_name("folder/file") == "folder - file"

    def test_illegal_chars_removed(self):
        assert sanitize_dir_name("test:file*name?") == "testfilename"

    def test_backslash_removed(self):
        assert sanitize_dir_name("folder\\file") == "folderfile"

    def test_leading_trailing_dots_stripped(self):
        assert sanitize_dir_name("  .folder.  ") == "folder"

    def test_windows_reserved_name_prefixed(self):
        assert sanitize_dir_name("CON.txt") == "_CON.txt"

    def test_windows_reserved_case_insensitive(self):
        assert sanitize_dir_name("con.txt") == "_con.txt"

    def test_non_string_coerced(self):
        assert sanitize_dir_name(0) == "0"

    def test_control_chars_removed(self):
        assert sanitize_dir_name("hello\x00world") == "helloworld"

    def test_multibyte_truncation(self):
        multibyte = "é" * 300
        result = sanitize_dir_name(multibyte)
        assert len(result.encode("utf-8")) <= 255

    def test_post_sanitization_empty_returns_untitled(self):
        assert sanitize_dir_name("\\") == "Untitled"

    def test_surrogate_fallback(self):
        assert sanitize_dir_name("\udcff") == "Untitled"

    def test_normal_name_preserved(self):
        assert sanitize_dir_name("My Collection") == "My Collection"


class TestServerBasenameVariants:
    """Tests for server_basename_variants function."""

    def test_empty_returns_empty_list(self):
        assert server_basename_variants("") == []

    def test_base_without_extension(self):
        result = server_basename_variants("file.txt")
        assert "file" in result

    def test_strips_numeric_day_prefix(self):
        result = server_basename_variants("22_video.mp4")
        assert "video" in result

    def test_strips_trailing_small_numeric(self):
        result = server_basename_variants("video_123.mp4")
        assert "video" in result

    def test_strips_epoch_suffix(self):
        result = server_basename_variants("video_1716423900.mp4")
        assert "video" in result

    def test_strips_epoch_with_subsecond(self):
        result = server_basename_variants("video_1716423900_000.mp4")
        assert "video" in result

    def test_combined_day_prefix_and_epoch(self):
        result = server_basename_variants("22_video_1716423900.mp4")
        assert "video" in result

    def test_dedupes_results(self):
        result = server_basename_variants("simple.mp4")
        assert result.count("simple") == 1

    def test_bang_token_removal(self):
        result = server_basename_variants("video_gg.mp4", bang_tokens={"gg"})
        assert "video" in result
        assert any("gg" not in v for v in result)

    def test_no_bang_tokens_default(self):
        """Without bang_tokens, no token-based stripping occurs."""
        result = server_basename_variants("video_gg.mp4")
        assert "video_gg" in result

    def test_no_empty_variants(self):
        result = server_basename_variants("22_.mp4")
        assert all(v for v in result)

    def test_preserves_order_deduped(self):
        result = server_basename_variants("test.mp4")
        assert result[0] == "test"

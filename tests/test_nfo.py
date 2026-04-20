"""Tests for NFO module."""

from xml.etree import ElementTree as ET

from media_archive_sync.nfo import (
    build_movie_nfo,
    generate_nfo,
    parse_release_date,
    write_nfo_for_path,
)


class TestParseReleaseDate:
    """Tests for parse_release_date function."""

    def test_parse_unix_timestamp(self):
        """Test parsing Unix timestamp (seconds)."""
        result = parse_release_date("1609459200")  # 2021-01-01
        assert result == "2021-01-01"

    def test_parse_millisecond_timestamp(self):
        """Test parsing millisecond timestamp."""
        result = parse_release_date("1609459200000")  # 2021-01-01 in ms
        assert result == "2021-01-01"

    def test_parse_modern_millisecond_timestamp(self):
        """Test parsing modern millisecond timestamps (upper bound 2e12)."""
        # A 2024 timestamp in milliseconds should work now
        result = parse_release_date("1704067200000")  # 2024-01-01 in ms
        assert result == "2024-01-01"

    def test_parse_iso_date(self):
        """Test parsing ISO format date."""
        result = parse_release_date("2021-01-01")
        assert result == "2021-01-01"

    def test_parse_iso_with_z(self):
        """Test parsing ISO date with Z timezone."""
        result = parse_release_date("2021-01-01T00:00:00Z")
        assert result == "2021-01-01"

    def test_parse_invalid_returns_none(self):
        """Test that invalid dates return None."""
        result = parse_release_date("invalid")
        assert result is None

    def test_parse_none_returns_none(self):
        """Test that None input returns None."""
        result = parse_release_date(None)
        assert result is None


class TestBuildMovieNfo:
    """Tests for build_movie_nfo function."""

    def test_basic_nfo_structure(self):
        """Test basic NFO structure is created."""
        xml = build_movie_nfo(title="Test Movie")
        root = ET.fromstring(xml)

        assert root.tag == "movie"
        title = root.find("title")
        assert title is not None
        assert title.text == "Test Movie"

    def test_collections_as_list(self):
        """Test collections as list are handled."""
        xml = build_movie_nfo(
            title="Test Movie", collections=["Collection 1", "Collection 2"]
        )
        root = ET.fromstring(xml)

        collections = root.find("collections")
        assert collections is not None
        sets = collections.findall("set")
        assert len(sets) == 2

    def test_collections_as_tuple(self):
        """Test collections as tuple are handled."""
        xml = build_movie_nfo(title="Test Movie", collections=("Coll1", "Coll2"))
        root = ET.fromstring(xml)

        collections = root.find("collections")
        assert collections is not None

    def test_collections_as_set(self):
        """Test collections as set are handled."""
        xml = build_movie_nfo(title="Test Movie", collections={"Set Collection"})
        root = ET.fromstring(xml)

        collections = root.find("collections")
        assert collections is not None
        sets = collections.findall("set")
        assert len(sets) == 1
        assert sets[0].text == "Set Collection"

    def test_collections_as_string(self):
        """Test collections as string are handled."""
        xml = build_movie_nfo(title="Test Movie", collections="Single Collection")
        root = ET.fromstring(xml)

        collections = root.find("collections")
        assert collections is not None
        sets = collections.findall("set")
        assert len(sets) == 1

    def test_actors_as_list(self):
        """Test actors as list creates actor elements."""
        xml = build_movie_nfo(title="Test", actors=["Actor 1", "Actor 2"])
        root = ET.fromstring(xml)

        actors = root.findall("actor")
        assert len(actors) == 2

    def test_actors_as_set(self):
        """Test actors as set are handled."""
        xml = build_movie_nfo(title="Test", actors={"Actor 1"})
        root = ET.fromstring(xml)

        actors = root.findall("actor")
        assert len(actors) == 1

    def test_genres_as_list(self):
        """Test genres as list creates genre elements."""
        xml = build_movie_nfo(title="Test", genres=["Action", "Drama"])
        root = ET.fromstring(xml)

        genres = root.findall("genre")
        assert len(genres) == 2

    def test_genres_as_set(self):
        """Test genres as set are handled."""
        xml = build_movie_nfo(title="Test", genres={"Comedy"})
        root = ET.fromstring(xml)

        genres = root.findall("genre")
        assert len(genres) == 1


class TestWriteNfoForPath:
    """Tests for write_nfo_for_path function."""

    def test_write_new_nfo(self, tmp_path):
        """Test writing new NFO file."""
        video_path = tmp_path / "video.mp4"
        video_path.touch()

        nfo_data = build_movie_nfo(title="Test Movie")
        result = write_nfo_for_path(video_path, nfo_data)

        assert result is True
        nfo_path = tmp_path / "video.nfo"
        assert nfo_path.exists()

    def test_skip_identical_content(self, tmp_path):
        """Test skipping when content is identical."""
        video_path = tmp_path / "video.mp4"
        video_path.touch()

        nfo_data = build_movie_nfo(title="Test Movie")
        nfo_path = tmp_path / "video.nfo"
        nfo_path.write_text(nfo_data, encoding="utf-8")

        result = write_nfo_for_path(video_path, nfo_data)

        assert result is False

    def test_overwrite_existing(self, tmp_path):
        """Test overwriting existing NFO with overwrite=True."""
        video_path = tmp_path / "video.mp4"
        video_path.touch()

        nfo_path = tmp_path / "video.nfo"
        nfo_path.write_text("<old>nfo</old>", encoding="utf-8")

        nfo_data = build_movie_nfo(title="New Title")
        result = write_nfo_for_path(video_path, nfo_data, overwrite=True)

        assert result is True
        content = nfo_path.read_text(encoding="utf-8")
        assert "New Title" in content

    def test_creates_directory(self, tmp_path):
        """Test that missing directories are created."""
        video_path = tmp_path / "subdir" / "video.mp4"
        video_path.parent.mkdir(parents=True)
        video_path.touch()

        nfo_data = build_movie_nfo(title="Test")
        result = write_nfo_for_path(video_path, nfo_data)

        assert result is True


class TestBuildMovieNfoCollectionsFiltering:
    """Tests for collections filtering and sorting."""

    def test_collections_empty_after_filtering_no_wrapper(self):
        """Test that empty collections don't create wrapper element."""
        xml = build_movie_nfo(
            title="Test Movie", collections=["", "   ", None, "Valid"]
        )
        root = ET.fromstring(xml)

        collections = root.find("collections")
        # Wrapper should still exist because we have one valid entry
        assert collections is not None
        sets = collections.findall("set")
        assert len(sets) == 1
        assert sets[0].text == "Valid"

    def test_collections_all_empty_no_wrapper(self):
        """Test that all-empty collections don't create wrapper."""
        xml = build_movie_nfo(title="Test Movie", collections=["", "   ", None])
        root = ET.fromstring(xml)

        collections = root.find("collections")
        assert collections is None

    def test_collections_set_sorted_deterministically(self):
        """Test that set collections are sorted for deterministic output."""
        xml = build_movie_nfo(
            title="Test Movie", collections={"Zebra", "Alpha", "Mike"}
        )
        root = ET.fromstring(xml)

        collections = root.find("collections")
        assert collections is not None
        sets = collections.findall("set")
        texts = [s.text for s in sets]
        # Should be sorted alphabetically
        assert texts == ["Alpha", "Mike", "Zebra"]

    def test_collections_list_not_sorted(self):
        """Test that list collections maintain original order."""
        xml = build_movie_nfo(
            title="Test Movie", collections=["Zebra", "Alpha", "Mike"]
        )
        root = ET.fromstring(xml)

        collections = root.find("collections")
        assert collections is not None
        sets = collections.findall("set")
        texts = [s.text for s in sets]
        # Should maintain original order
        assert texts == ["Zebra", "Alpha", "Mike"]

    def test_collections_single_string(self):
        """Test that single string collections work."""
        xml = build_movie_nfo(title="Test Movie", collections="Single Collection")
        root = ET.fromstring(xml)

        collections = root.find("collections")
        assert collections is not None
        sets = collections.findall("set")
        assert len(sets) == 1
        assert sets[0].text == "Single Collection"

    def test_collections_single_string_empty_no_wrapper(self):
        """Test that empty string collection doesn't create wrapper."""
        xml = build_movie_nfo(title="Test Movie", collections="   ")
        root = ET.fromstring(xml)

        collections = root.find("collections")
        assert collections is None


class TestParseReleaseDateValidateEpoch:
    """Tests for parse_release_date validate_epoch parameter."""

    def test_validate_epoch_true_rejects_small_values(self):
        """Small epoch values are rejected when validate_epoch=True."""
        result = parse_release_date("100", validate_epoch=True)
        assert result is None

    def test_validate_epoch_false_accepts_small_values(self):
        """Small epoch values are accepted when validate_epoch=False."""
        result = parse_release_date("100", validate_epoch=False)
        assert result == "1970-01-01"

    def test_validate_epoch_true_rejects_zero(self):
        """Zero is rejected when validate_epoch=True."""
        result = parse_release_date("0", validate_epoch=True)
        assert result is None

    def test_validate_epoch_false_accepts_zero(self):
        """Zero is accepted when validate_epoch=False."""
        result = parse_release_date("0", validate_epoch=False)
        assert result == "1970-01-01"

    def test_validate_epoch_default_is_true(self):
        """Default behavior validates epoch (backward compatibility)."""
        result = parse_release_date("100")
        assert result is None

    def test_validate_epoch_true_accepts_valid_range(self):
        """Valid epoch range still works with validate_epoch=True."""
        result = parse_release_date("1609459200", validate_epoch=True)
        assert result == "2021-01-01"

    def test_validate_epoch_false_accepts_valid_range(self):
        """Valid epoch range works with validate_epoch=False."""
        result = parse_release_date("1609459200", validate_epoch=False)
        assert result == "2021-01-01"

    def test_validate_epoch_true_rejects_very_large(self):
        """Very large values beyond 2e12 are rejected with validate_epoch=True."""
        result = parse_release_date("3e12", validate_epoch=True)
        assert result is None

    def test_validate_epoch_false_accepts_very_large(self):
        """Very large values are attempted with validate_epoch=False."""
        result = parse_release_date("3e12", validate_epoch=False)
        assert result is not None

    def test_iso_parsing_unaffected_by_validate_epoch(self):
        """ISO format parsing works regardless of validate_epoch."""
        for flag in (True, False):
            result = parse_release_date("2021-01-01", validate_epoch=flag)
            assert result == "2021-01-01"

    def test_negative_epoch_with_validate_false(self):
        """Negative epoch values are accepted with validate_epoch=False."""
        result = parse_release_date("-1", validate_epoch=False)
        assert result is not None
        assert result.startswith("1969")


class TestBuildMovieNfoKickOptions:
    """Tests for build_movie_nfo kick_suffix and kick_tag parameters."""

    def test_kick_suffix_appends_to_title(self):
        """kick_suffix=True appends (KICK) to title."""
        xml = build_movie_nfo(title="My Stream", kick_suffix=True)
        root = ET.fromstring(xml)
        title = root.find("title")
        assert title is not None
        assert title.text == "My Stream (KICK)"

    def test_kick_suffix_false_no_append(self):
        """kick_suffix=False does not append to title."""
        xml = build_movie_nfo(title="My Stream", kick_suffix=False)
        root = ET.fromstring(xml)
        title = root.find("title")
        assert title is not None
        assert title.text == "My Stream"

    def test_kick_suffix_default_false(self):
        """Default kick_suffix is False (backward compatibility)."""
        xml = build_movie_nfo(title="My Stream")
        root = ET.fromstring(xml)
        title = root.find("title")
        assert title is not None
        assert title.text == "My Stream"

    def test_kick_suffix_affects_sorttitle_when_no_original(self):
        """sorttitle uses the suffixed title when no original_title."""
        xml = build_movie_nfo(title="My Stream", kick_suffix=True)
        root = ET.fromstring(xml)
        sorttitle = root.find("sorttitle")
        assert sorttitle is not None
        assert sorttitle.text == "My Stream (KICK)"

    def test_kick_suffix_does_not_affect_sorttitle_with_original(self):
        """sorttitle uses original_title when provided, not suffixed title."""
        xml = build_movie_nfo(
            title="My Stream", original_title="Original", kick_suffix=True
        )
        root = ET.fromstring(xml)
        sorttitle = root.find("sorttitle")
        assert sorttitle is not None
        assert sorttitle.text == "Original"

    def test_kick_tag_adds_genre(self):
        """kick_tag=True adds 'Kick Vod' genre."""
        xml = build_movie_nfo(title="My Stream", kick_tag=True)
        root = ET.fromstring(xml)
        genres = [g.text for g in root.findall("genre")]
        assert "Kick Vod" in genres

    def test_kick_tag_false_no_genre(self):
        """kick_tag=False does not add Kick Vod genre."""
        xml = build_movie_nfo(title="My Stream", kick_tag=False)
        root = ET.fromstring(xml)
        genres = [g.text for g in root.findall("genre")]
        assert "Kick Vod" not in genres

    def test_kick_tag_default_false(self):
        """Default kick_tag is False (backward compatibility)."""
        xml = build_movie_nfo(title="My Stream")
        root = ET.fromstring(xml)
        genres = [g.text for g in root.findall("genre")]
        assert "Kick Vod" not in genres

    def test_kick_tag_with_existing_genres(self):
        """kick_tag=True adds Kick Vod alongside existing genres."""
        xml = build_movie_nfo(
            title="My Stream", genres=["Action", "Drama"], kick_tag=True
        )
        root = ET.fromstring(xml)
        genres = [g.text for g in root.findall("genre")]
        assert "Action" in genres
        assert "Drama" in genres
        assert "Kick Vod" in genres

    def test_kick_suffix_and_tag_combined(self):
        """Both kick_suffix and kick_tag can be used together."""
        xml = build_movie_nfo(title="My Stream", kick_suffix=True, kick_tag=True)
        root = ET.fromstring(xml)
        title = root.find("title")
        assert title.text == "My Stream (KICK)"
        genres = [g.text for g in root.findall("genre")]
        assert "Kick Vod" in genres

    def test_no_duplicate_kick_vod_genre(self):
        """kick_tag=True should not add duplicate 'Kick Vod' genre."""
        xml = build_movie_nfo(title="Test", genres=["Kick Vod"], kick_tag=True)
        root = ET.fromstring(xml)
        genres = [g.text for g in root.findall("genre")]
        kick_vod_count = genres.count("Kick Vod")
        assert kick_vod_count == 1

    def test_validate_epoch_forwarded_in_build_movie_nfo(self):
        """validate_epoch is forwarded to parse_release_date in build_movie_nfo."""
        xml = build_movie_nfo(title="Test", releasedate="100", validate_epoch=False)
        root = ET.fromstring(xml)
        rd = root.find("releasedate")
        assert rd is not None
        assert rd.text == "1970-01-01"


class TestGenerateNfo:
    """Tests for generate_nfo function."""

    def test_basic_generate(self):
        """generate_nfo produces valid NFO from dict."""
        xml = generate_nfo({"title": "Test Movie"})
        root = ET.fromstring(xml)
        assert root.tag == "movie"
        title = root.find("title")
        assert title is not None
        assert title.text == "Test Movie"

    def test_generate_with_year(self):
        """generate_nfo passes year from dict."""
        xml = generate_nfo({"title": "Test", "year": 2024})
        root = ET.fromstring(xml)
        year = root.find("year")
        assert year is not None
        assert year.text == "2024"

    def test_generate_with_releasedate(self):
        """generate_nfo parses releasedate from dict."""
        xml = generate_nfo({"title": "Test", "releasedate": "1609459200"})
        root = ET.fromstring(xml)
        rd = root.find("releasedate")
        assert rd is not None
        assert rd.text == "2021-01-01"

    def test_generate_with_validate_epoch_false(self):
        """generate_nfo passes validate_epoch to parse_release_date."""
        xml = generate_nfo(
            {"title": "Test", "releasedate": "100"}, validate_epoch=False
        )
        root = ET.fromstring(xml)
        rd = root.find("releasedate")
        assert rd is not None
        assert rd.text == "1970-01-01"

    def test_generate_with_validate_epoch_true_rejects_small(self):
        """generate_nfo with validate_epoch=True rejects small epoch values."""
        xml = generate_nfo({"title": "Test", "releasedate": "100"}, validate_epoch=True)
        root = ET.fromstring(xml)
        rd = root.find("releasedate")
        assert rd is None

    def test_generate_with_kick_suffix(self):
        """generate_nfo passes kick_suffix to build_movie_nfo."""
        xml = generate_nfo({"title": "My Stream"}, kick_suffix=True)
        root = ET.fromstring(xml)
        title = root.find("title")
        assert title.text == "My Stream (KICK)"

    def test_generate_with_kick_tag(self):
        """generate_nfo passes kick_tag to build_movie_nfo."""
        xml = generate_nfo({"title": "My Stream"}, kick_tag=True)
        root = ET.fromstring(xml)
        genres = [g.text for g in root.findall("genre")]
        assert "Kick Vod" in genres

    def test_generate_uses_tags_as_genres_fallback(self):
        """generate_nfo uses 'tags' key as fallback for genres."""
        xml = generate_nfo({"title": "Test", "tags": ["Action", "Comedy"]})
        root = ET.fromstring(xml)
        genres = [g.text for g in root.findall("genre")]
        assert "Action" in genres
        assert "Comedy" in genres

    def test_generate_genres_over_tags(self):
        """generate_nfo prefers 'genres' key over 'tags'."""
        xml = generate_nfo({"title": "Test", "genres": ["Drama"], "tags": ["Action"]})
        root = ET.fromstring(xml)
        genres = [g.text for g in root.findall("genre")]
        assert "Drama" in genres

    def test_generate_with_originaltitle(self):
        """generate_nfo maps originaltitle from dict."""
        xml = generate_nfo({"title": "Test", "originaltitle": "Original"})
        root = ET.fromstring(xml)
        orig = root.find("originaltitle")
        assert orig is not None
        assert orig.text == "Original"

    def test_generate_with_collections(self):
        """generate_nfo passes collections from dict."""
        xml = generate_nfo({"title": "Test", "collections": ["Coll1"]})
        root = ET.fromstring(xml)
        collections = root.find("collections")
        assert collections is not None

    def test_generate_with_actors(self):
        """generate_nfo passes actors from dict."""
        xml = generate_nfo({"title": "Test", "actors": ["Actor1"]})
        root = ET.fromstring(xml)
        actors = root.findall("actor")
        assert len(actors) == 1

    def test_generate_empty_title_defaults_to_empty_string(self):
        """generate_nfo produces valid XML even when title missing."""
        xml = generate_nfo({})
        root = ET.fromstring(xml)
        assert root.tag == "movie"

    def test_generate_with_plot(self):
        """generate_nfo passes plot from dict."""
        xml = generate_nfo({"title": "Test", "plot": "A great movie"})
        root = ET.fromstring(xml)
        plot = root.find("plot")
        assert plot is not None
        assert plot.text == "A great movie"

    def test_generate_with_director(self):
        """generate_nfo passes director from dict."""
        xml = generate_nfo({"title": "Test", "director": "Someone"})
        root = ET.fromstring(xml)
        director = root.find("director")
        assert director is not None
        assert director.text == "Someone"

    def test_generate_with_uniqueid(self):
        """generate_nfo passes uniqueid from dict."""
        xml = generate_nfo({"title": "Test", "uniqueid": {"imdb": "tt12345"}})
        root = ET.fromstring(xml)
        uids = root.findall("uniqueid")
        assert len(uids) == 1
        assert uids[0].text == "tt12345"

    def test_empty_genres_does_not_fall_through_to_tags(self):
        """Empty genres list should not fall through to tags."""
        xml = generate_nfo({"title": "Test", "genres": [], "tags": ["Action"]})
        root = ET.fromstring(xml)
        genres = [g.text for g in root.findall("genre")]
        assert "Action" not in genres
        assert len(genres) == 0

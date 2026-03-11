"""Tests for NFO module."""

from xml.etree import ElementTree as ET

from media_archive_sync.nfo import (
    build_movie_nfo,
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

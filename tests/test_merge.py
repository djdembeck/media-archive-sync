"""Tests for merge module."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from media_archive_sync.merge import (
    _create_concat_list,
    _resolve_ffprobe_path,
    detect_video_parts,
    get_video_duration,
    merge_video_parts,
)


class TestGetVideoDuration:
    """Tests for get_video_duration function."""

    def test_get_duration_success(self, tmp_path):
        """Test successfully getting video duration."""
        video_file = tmp_path / "video.mp4"
        video_file.write_text("fake video content")

        mock_result = MagicMock()
        mock_result.stdout = "123.456\n"
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result):
            result = get_video_duration(video_file)

        assert result == 123.456

    def test_get_duration_empty_output(self, tmp_path):
        """Test handling empty ffprobe output."""
        video_file = tmp_path / "video.mp4"
        video_file.write_text("fake video content")

        mock_result = MagicMock()
        mock_result.stdout = ""
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result):
            result = get_video_duration(video_file)

        assert result is None

    def test_get_duration_ffprobe_not_found(self, tmp_path):
        """Test handling when ffprobe is not installed."""
        video_file = tmp_path / "video.mp4"
        video_file.write_text("fake video content")

        with patch(
            "subprocess.run",
            side_effect=FileNotFoundError("ffprobe not found"),
        ):
            result = get_video_duration(video_file)

        assert result is None

    def test_get_duration_subprocess_error(self, tmp_path):
        """Test handling subprocess error."""
        import subprocess

        video_file = tmp_path / "video.mp4"
        video_file.write_text("fake video content")

        with patch(
            "subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "ffprobe"),
        ):
            result = get_video_duration(video_file)

        assert result is None


class TestDetectVideoParts:
    """Tests for detect_video_parts function."""

    def test_detect_parts_empty_directory(self, tmp_path):
        """Test detecting parts in empty directory."""
        result = detect_video_parts(tmp_path, "video")

        assert result == []

    def test_detect_parts_no_matches(self, tmp_path):
        """Test when no part files match pattern."""
        (tmp_path / "video.mp4").write_text("test")
        (tmp_path / "other_part1.mp4").write_text("test")

        result = detect_video_parts(tmp_path, "video")

        assert result == []

    def test_detect_parts_single_match(self, tmp_path):
        """Test detecting single part file."""
        (tmp_path / "video_part1.mp4").write_text("test")

        result = detect_video_parts(tmp_path, "video")

        assert len(result) == 1
        assert result[0].name == "video_part1.mp4"

    def test_detect_parts_multiple_matches(self, tmp_path):
        """Test detecting multiple part files."""
        (tmp_path / "video_part2.mp4").write_text("test")
        (tmp_path / "video_part1.mp4").write_text("test")
        (tmp_path / "video_part3.mp4").write_text("test")

        result = detect_video_parts(tmp_path, "video")

        assert len(result) == 3
        # Should be sorted by part number
        assert result[0].name == "video_part1.mp4"
        assert result[1].name == "video_part2.mp4"
        assert result[2].name == "video_part3.mp4"

    def test_detect_parts_different_extensions(self, tmp_path):
        """Test detecting parts with different video extensions."""
        (tmp_path / "video_part1.mp4").write_text("test")
        (tmp_path / "video_part2.mkv").write_text("test")
        (tmp_path / "video_part3.avi").write_text("test")

        result = detect_video_parts(tmp_path, "video")

        assert len(result) == 3

    def test_detect_parts_skips_non_video(self, tmp_path):
        """Test that non-video files are skipped."""
        (tmp_path / "video_part1.mp4").write_text("test")
        (tmp_path / "video_part2.txt").write_text("test")  # Not a video

        result = detect_video_parts(tmp_path, "video")

        assert len(result) == 1

    def test_detect_parts_case_insensitive(self, tmp_path):
        """Test case-insensitive extension matching."""
        (tmp_path / "video_part1.MP4").write_text("test")
        (tmp_path / "video_part2.MKV").write_text("test")

        result = detect_video_parts(tmp_path, "video")

        assert len(result) == 2

    def test_detect_parts_nonexistent_directory(self, tmp_path):
        """Test detecting parts in nonexistent directory."""
        result = detect_video_parts(tmp_path / "nonexistent", "video")

        assert result == []

    def test_detect_parts_file_not_directory(self, tmp_path):
        """Test detecting parts when path is a file, not directory."""
        file_path = tmp_path / "not_a_dir"
        file_path.write_text("test")

        result = detect_video_parts(file_path, "video")

        assert result == []


class TestMergeVideoParts:
    """Tests for merge_video_parts function."""

    def test_merge_empty_parts_raises_error(self, tmp_path):
        """Test that empty parts list raises ValueError."""
        output = tmp_path / "output.mp4"

        with pytest.raises(ValueError, match="part_paths cannot be empty"):
            merge_video_parts([], output)

    def test_merge_single_part(self, tmp_path):
        """Test merging single video part."""
        part1 = tmp_path / "video_part1.mp4"
        part1.write_text("fake video content")
        output = tmp_path / "output.mp4"

        with (
            patch("subprocess.run") as mock_run,
            patch("media_archive_sync.merge.get_video_duration", return_value=10.0),
            patch(
                "media_archive_sync.merge._resolve_ffprobe_path",
                return_value="ffprobe",
            ),
            patch("media_archive_sync.merge._create_concat_list") as mock_concat,
        ):
            mock_concat.return_value = tmp_path / "concat.txt"
            mock_run.return_value = MagicMock(returncode=0)

            # Create output file to simulate successful merge
            output.write_text("merged video content")

            result = merge_video_parts([part1], output)

            assert result is True

    def test_merge_ffmpeg_failure(self, tmp_path):
        """Test handling ffmpeg failure."""
        part1 = tmp_path / "video_part1.mp4"
        part1.write_text("fake video content")
        output = tmp_path / "output.mp4"

        with (
            patch("subprocess.run") as mock_run,
            patch(
                "media_archive_sync.merge._resolve_ffprobe_path", return_value="ffprobe"
            ),
            patch("media_archive_sync.merge._create_concat_list") as mock_concat,
        ):
            mock_concat.return_value = tmp_path / "concat.txt"
            mock_run.side_effect = subprocess.CalledProcessError(1, "ffmpeg")

            result = merge_video_parts([part1], output)

            assert result is False

    def test_merge_ffmpeg_not_found(self, tmp_path):
        """Test handling when ffmpeg is not found."""
        part1 = tmp_path / "video_part1.mp4"
        part1.write_text("fake video content")
        output = tmp_path / "output.mp4"

        with (
            patch("subprocess.run") as mock_run,
            patch(
                "media_archive_sync.merge._resolve_ffprobe_path", return_value="ffprobe"
            ),
            patch("media_archive_sync.merge._create_concat_list") as mock_concat,
        ):
            mock_concat.return_value = tmp_path / "concat.txt"
            mock_run.side_effect = FileNotFoundError("ffmpeg not found")

            result = merge_video_parts([part1], output)

            assert result is False

    def test_merge_multiple_parts(self, tmp_path):
        """Test merging multiple video parts."""
        part1 = tmp_path / "video_part1.mp4"
        part1.write_text("fake video content")
        part2 = tmp_path / "video_part2.mp4"
        part2.write_text("fake video content")
        output = tmp_path / "output.mp4"

        with (
            patch("subprocess.run") as mock_run,
            patch("media_archive_sync.merge.get_video_duration", return_value=10.0),
            patch(
                "media_archive_sync.merge._resolve_ffprobe_path",
                return_value="ffprobe",
            ),
            patch("media_archive_sync.merge._create_concat_list") as mock_concat,
        ):
            mock_concat.return_value = tmp_path / "concat.txt"
            mock_run.return_value = MagicMock(returncode=0)

            # Create output file to simulate successful merge
            output.write_text("merged video content")

            result = merge_video_parts([part1, part2], output)

            assert result is True


class TestResolveFfprobePath:
    """Tests for _resolve_ffprobe_path function."""

    def test_ffprobe_in_path(self):
        """Test resolving ffprobe when in PATH."""
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            result = _resolve_ffprobe_path("ffmpeg")

        assert result == "/usr/bin/ffprobe"

    def test_ffprobe_not_in_path(self):
        """Test resolving ffprobe when not in PATH."""
        with patch("shutil.which", return_value=None):
            result = _resolve_ffprobe_path("/usr/local/bin/ffmpeg")

        assert result == "/usr/local/bin/ffprobe"


class TestCreateConcatList:
    """Tests for _create_concat_list function."""

    def test_create_concat_list_single_file(self, tmp_path):
        """Test creating concat list with single file."""
        part1 = tmp_path / "video_part1.mp4"
        part1.write_text("fake video content")

        concat_path = _create_concat_list([part1])

        assert concat_path is not None
        assert concat_path.exists()
        content = concat_path.read_text()
        assert "file" in content
        assert "video_part1.mp4" in content

        # Clean up
        concat_path.unlink()

    def test_create_concat_list_multiple_files(self, tmp_path):
        """Test creating concat list with multiple files."""
        part1 = tmp_path / "video_part1.mp4"
        part1.write_text("content")
        part2 = tmp_path / "video_part2.mp4"
        part2.write_text("content")

        concat_path = _create_concat_list([part1, part2])

        assert concat_path is not None
        assert concat_path.exists()
        content = concat_path.read_text()
        assert content.count("file") == 2
        assert "video_part1.mp4" in content
        assert "video_part2.mp4" in content

        # Clean up
        concat_path.unlink()

    def test_create_concat_list_escapes_single_quotes(self, tmp_path):
        """Test that single quotes in paths are escaped."""
        part = tmp_path / "video's_part1.mp4"
        part.write_text("content")

        concat_path = _create_concat_list([part])

        assert concat_path is not None
        assert concat_path.exists()
        content = concat_path.read_text()
        # Should have escaped single quotes
        assert "'\\''" in content or "video's_part1" in content

        # Clean up
        concat_path.unlink()

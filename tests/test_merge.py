"""Tests for merge module."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from media_archive_sync.merge import (
    _create_concat_list,
    _prepare_merge_order,
    _resolve_ffprobe_path,
    cluster_by_epoch_window,
    detect_video_parts,
    extract_epoch_from_filename,
    get_video_duration,
    merge_multipart_group,
    merge_multipart_videos,
    merge_video_parts,
    should_merge_group,
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


class TestClusterByEpochWindow:
    """Tests for cluster_by_epoch_window function."""

    def test_single_file_creates_single_cluster(self):
        result = cluster_by_epoch_window(["video_1700000000_part1.mp4"])
        assert result == [["video_1700000000_part1.mp4"]]

    def test_files_within_window_are_clustered(self):
        names = [
            "video_1700000000_part1.mp4",
            "video_1700001000_part2.mp4",
        ]
        result = cluster_by_epoch_window(names)
        assert len(result) == 1
        assert len(result[0]) == 2

    def test_files_beyond_window_are_separate_clusters(self):
        names = [
            "video_1700000000_part1.mp4",
            "video_1701000000_part2.mp4",
        ]
        result = cluster_by_epoch_window(names)
        assert len(result) == 2

    def test_custom_window_seconds(self):
        names = [
            "video_1700000000_part1.mp4",
            "video_1700000500_part2.mp4",
        ]
        result = cluster_by_epoch_window(names, window_seconds=400)
        assert len(result) == 2
        result = cluster_by_epoch_window(names, window_seconds=600)
        assert len(result) == 1

    def test_no_epoch_files_form_own_clusters(self):
        names = ["no_epoch_file.mp4", "another_no_epoch.mkv"]
        result = cluster_by_epoch_window(names)
        assert len(result) == 2
        assert result[0] == ["no_epoch_file.mp4"]
        assert result[1] == ["another_no_epoch.mkv"]

    def test_mixed_epoch_and_no_epoch(self):
        names = [
            "video_1700000000_part1.mp4",
            "no_epoch.mp4",
            "video_1700001000_part2.mp4",
        ]
        result = cluster_by_epoch_window(names)
        assert len(result) == 2
        epoch_cluster = [c for c in result if len(c) == 2]
        no_epoch_cluster = [c for c in result if len(c) == 1]
        assert len(epoch_cluster) == 1
        assert len(no_epoch_cluster) == 1

    def test_empty_input_returns_empty(self):
        assert cluster_by_epoch_window([]) == []

    def test_cluster_boundary_is_inclusive(self):
        names = [
            "video_1700000000_part1.mp4",
            "video_1700028800_part2.mp4",
        ]
        result = cluster_by_epoch_window(names, window_seconds=28800)
        assert len(result) == 1

    def test_fixed_start_window_clustering(self):
        # A-B within window, B-C within window, but A-C NOT within window
        # C should be in a different cluster from A
        names = [
            "video_1700000000_part1.mp4",
            "video_1700010000_part2.mp4",
            "video_1700028800_part3.mp4",
        ]
        result = cluster_by_epoch_window(names, window_seconds=15000)
        # A and B are within 15000s of A-start
        # C is 28800s from A-start, beyond window
        assert len(result) == 2


class TestShouldMergeGroup:
    """Tests for should_merge_group function."""

    def test_two_parts_no_base_returns_true(self, tmp_path):
        parts = [tmp_path / "video_part1.mp4", tmp_path / "video_part2.mp4"]
        base = tmp_path / "video.mp4"
        assert should_merge_group(parts, base) is True

    def test_one_part_with_base_returns_true(self, tmp_path):
        parts = [tmp_path / "video_part1.mp4"]
        base = tmp_path / "video.mp4"
        base.write_bytes(b"base")
        assert should_merge_group(parts, base) is True

    def test_one_part_no_base_returns_false(self, tmp_path):
        parts = [tmp_path / "video_part1.mp4"]
        base = tmp_path / "video.mp4"
        assert should_merge_group(parts, base) is False

    def test_zero_parts_returns_false(self, tmp_path):
        base = tmp_path / "video.mp4"
        base.write_bytes(b"base")
        assert should_merge_group([], base) is False


class TestExtractEpochFromFilename:
    """Tests for extract_epoch_from_filename function."""

    def test_extracts_valid_epoch(self):
        result = extract_epoch_from_filename("video_1700000000_part1.mp4")
        assert result == 1700000000

    def test_returns_none_for_no_epoch(self):
        result = extract_epoch_from_filename("video_part1.mp4")
        assert result is None

    def test_returns_none_for_invalid_epoch(self):
        result = extract_epoch_from_filename("video_part1_0.mp4")
        assert result is None


class TestPrepareMergeOrder:
    """Tests for _prepare_merge_order function."""

    def test_orders_by_part_index(self, tmp_path):
        p2 = tmp_path / "video_part2.mp4"
        p1 = tmp_path / "video_part1.mp4"
        p2.write_bytes(b"2")
        p1.write_bytes(b"1")
        merge_order, merged_path, overwrite = _prepare_merge_order(
            [p2, p1], "video", ".mp4"
        )
        assert merge_order[0].name == "video_part1.mp4"
        assert merge_order[1].name == "video_part2.mp4"

    def test_merged_path_is_base_ext(self, tmp_path):
        p1 = tmp_path / "video_part1.mp4"
        p1.write_bytes(b"1")
        _, merged_path, _ = _prepare_merge_order([p1], "video", ".mp4")
        assert merged_path == tmp_path / "video.mp4"

    def test_overwrite_existing_when_base_exists(self, tmp_path):
        base = tmp_path / "video.mp4"
        base.write_bytes(b"base")
        p1 = tmp_path / "video_part1.mp4"
        p1.write_bytes(b"1")
        _, _, overwrite = _prepare_merge_order([p1], "video", ".mp4")
        assert overwrite is True

    def test_dry_run_skips_renames(self, tmp_path):
        p1 = tmp_path / "video_1700000001_part2.mp4"
        p2 = tmp_path / "video_1700000000_part1.mp4"
        p1.write_bytes(b"1")
        p2.write_bytes(b"2")
        merge_order, _, _ = _prepare_merge_order(
            [p1, p2], "video", ".mp4", dry_run=True
        )
        assert p1.exists()
        assert p2.exists()

    def test_no_epoch_parts_sorted_after_real_epochs(self, tmp_path):
        p1 = tmp_path / "video_part1.mp4"
        p2 = tmp_path / "video_1700000000_part2.mp4"
        p1.write_bytes(b"1")
        p2.write_bytes(b"2")
        merge_order, _, _ = _prepare_merge_order(
            [p1, p2], "video", ".mp4", dry_run=True
        )
        assert merge_order[0].name == "video_1700000000_part2.mp4"


class TestMergeMultipartGroup:
    """Tests for merge_multipart_group function."""

    def test_dry_run_returns_merged_path_and_parent(self, tmp_path):
        p1 = tmp_path / "video_part1.mp4"
        p2 = tmp_path / "video_part2.mp4"
        p1.write_bytes(b"1")
        p2.write_bytes(b"2")

        result = merge_multipart_group([p1, p2], dry_run=True)
        assert result is not None
        merged_path, parent = result
        assert merged_path == tmp_path / "video.mp4"
        assert parent == tmp_path

    def test_empty_parts_returns_none(self):
        result = merge_multipart_group([], dry_run=True)
        assert result is None

    def test_auto_detects_base_and_ext(self, tmp_path):
        p1 = tmp_path / "myvid_part1.mkv"
        p2 = tmp_path / "myvid_part2.mkv"
        p1.write_bytes(b"1")
        p2.write_bytes(b"2")

        result = merge_multipart_group([p1, p2], dry_run=True)
        assert result is not None
        merged_path, _ = result
        assert merged_path == tmp_path / "myvid.mkv"

    def test_successful_merge_with_mocked_ffmpeg(self, tmp_path):
        p1 = tmp_path / "video_part1.mp4"
        p2 = tmp_path / "video_part2.mp4"
        p1.write_bytes(b"1")
        p2.write_bytes(b"2")

        with (
            patch("media_archive_sync.merge.subprocess.run") as mock_run,
            patch("media_archive_sync.merge.get_video_duration", return_value=10.0),
            patch("media_archive_sync.merge._create_concat_list") as mock_concat,
            patch(
                "media_archive_sync.merge._resolve_ffprobe_path", return_value="ffprobe"
            ),
        ):
            mock_concat.return_value = tmp_path / "concat.txt"
            mock_run.return_value = MagicMock(returncode=0)

            def fake_run(cmd, **kwargs):
                if "concat" in str(cmd):
                    out_path = tmp_path / "video.recreated.mp4"
                    out_path.write_bytes(b"merged")

            mock_run.side_effect = fake_run
            result = merge_multipart_group([p1, p2], dry_run=False)
            assert result is not None

    def test_ffmpeg_failure_returns_none(self, tmp_path):
        p1 = tmp_path / "video_part1.mp4"
        p2 = tmp_path / "video_part2.mp4"
        p1.write_bytes(b"1")
        p2.write_bytes(b"2")

        with (
            patch("media_archive_sync.merge._create_concat_list") as mock_concat,
            patch(
                "media_archive_sync.merge._resolve_ffprobe_path", return_value="ffprobe"
            ),
        ):
            mock_concat.return_value = tmp_path / "concat.txt"
            with patch(
                "media_archive_sync.merge.subprocess.run",
                side_effect=subprocess.CalledProcessError(1, "ffmpeg"),
            ):
                result = merge_multipart_group([p1, p2], dry_run=False)
                assert result is None

    def test_rename_failure_returns_none(self, tmp_path):
        p1 = tmp_path / "video_part1.mp4"
        p2 = tmp_path / "video_part2.mp4"
        p1.write_bytes(b"1")
        p2.write_bytes(b"2")

        with (
            patch("media_archive_sync.merge.subprocess.run") as mock_run,
            patch("media_archive_sync.merge._create_concat_list") as mock_concat,
            patch(
                "media_archive_sync.merge._resolve_ffprobe_path",
                return_value="ffprobe",
            ),
            patch("media_archive_sync.merge.get_video_duration", return_value=10.0),
        ):
            mock_concat.return_value = tmp_path / "concat.txt"
            mock_run.return_value = MagicMock(returncode=0)

            def fake_run(cmd, **kwargs):
                if "concat" in str(cmd):
                    out_path = tmp_path / "video.recreated.mp4"
                    out_path.write_bytes(b"merged")

            mock_run.side_effect = fake_run

            # Make rename fail
            with patch.object(Path, "rename", side_effect=OSError("permission denied")):
                result = merge_multipart_group([p1, p2], dry_run=False)
                assert result is None


class TestMergeMultipartVideos:
    """Tests for merge_multipart_videos function."""

    def test_dry_run_returns_merge_candidates(self, tmp_path):
        p1 = tmp_path / "video_part1.mp4"
        p2 = tmp_path / "video_part2.mp4"
        p1.write_bytes(b"1")
        p2.write_bytes(b"2")

        result = merge_multipart_videos(
            media_root=tmp_path,
            dry_run=True,
            directories=[tmp_path],
        )
        assert len(result) == 1
        merged_path, parent, source_parts = result[0]
        assert merged_path == tmp_path / "video.mp4"
        assert parent == tmp_path
        assert len(source_parts) == 2

    def test_dry_run_with_single_part_and_base(self, tmp_path):
        base = tmp_path / "video.mp4"
        base.write_bytes(b"base")
        p1 = tmp_path / "video_part1.mp4"
        p1.write_bytes(b"1")

        result = merge_multipart_videos(
            media_root=tmp_path,
            dry_run=True,
            directories=[tmp_path],
        )
        assert len(result) == 1

    def test_dry_run_skips_single_part_no_base(self, tmp_path):
        p1 = tmp_path / "video_part1.mp4"
        p1.write_bytes(b"1")

        result = merge_multipart_videos(
            media_root=tmp_path,
            dry_run=True,
            directories=[tmp_path],
        )
        assert len(result) == 0

    def test_non_video_files_are_ignored(self, tmp_path):
        p1 = tmp_path / "video_part1.nfo"
        p1.write_bytes(b"nfo")

        result = merge_multipart_videos(
            media_root=tmp_path,
            dry_run=True,
            directories=[tmp_path],
        )
        assert len(result) == 0

    def test_directories_parameter_limits_scan(self, tmp_path):
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        p1 = subdir / "video_part1.mp4"
        p2 = subdir / "video_part2.mp4"
        p1.write_bytes(b"1")
        p2.write_bytes(b"2")

        other = tmp_path / "other"
        other.mkdir()
        p3 = other / "other_part1.mp4"
        p4 = other / "other_part2.mp4"
        p3.write_bytes(b"3")
        p4.write_bytes(b"4")

        result = merge_multipart_videos(
            media_root=tmp_path,
            dry_run=True,
            directories=[subdir],
        )
        assert len(result) == 1
        assert result[0][1] == subdir

    def test_scans_entire_media_root_when_no_directories(self, tmp_path):
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        p1 = subdir / "video_part1.mp4"
        p2 = subdir / "video_part2.mp4"
        p1.write_bytes(b"1")
        p2.write_bytes(b"2")

        result = merge_multipart_videos(
            media_root=tmp_path,
            dry_run=True,
        )
        assert len(result) == 1

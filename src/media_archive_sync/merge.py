"""Video merge utilities for media archive synchronization.

This module provides functionality for merging multipart video files
using ffmpeg concat demuxer.
"""

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from .logging import get_logger

logger = get_logger(__name__)

# Common video extensions
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v", ".mpg", ".mpeg"}


def get_video_duration(video_path: Path, ffprobe_path: str = "ffprobe") -> float | None:
    """Get video file duration using ffprobe.

    Args:
        video_path: Path to the video file.
        ffprobe_path: Path or command name for ffprobe executable.

    Returns:
        Duration in seconds, or None if probing failed.
    """
    try:
        result = subprocess.run(
            [
                ffprobe_path,
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        out = result.stdout.strip()
        return float(out) if out else None
    except (
        subprocess.CalledProcessError,
        ValueError,
        FileNotFoundError,
        OSError,
    ) as exc:
        logger.debug("Failed to get duration for %s: %s", video_path, exc)
        return None


def detect_video_parts(directory: Path, base_name: str) -> list[Path]:
    """Find multipart video files in a directory.

    Scans the directory for files matching the pattern {base_name}_part{N}.{ext}
    and returns them sorted by part number.

    Args:
        directory: Directory to search for part files.
        base_name: Base filename (without extension) to match.

    Returns:
        List of Paths to part files, sorted by part index.
    """
    parts: list[tuple[int, Path]] = []

    if not directory.exists() or not directory.is_dir():
        logger.debug("Directory does not exist or is not a directory: %s", directory)
        return []

    for file_path in directory.iterdir():
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() not in VIDEO_EXTENSIONS:
            continue

        # Match pattern: base_name_partN.ext
        pattern = (
            re.escape(base_name) + r"_part(\d+)" + re.escape(file_path.suffix) + r"$"
        )
        match = re.match(pattern, file_path.name, re.IGNORECASE)
        if match:
            part_index = int(match.group(1))
            parts.append((part_index, file_path))

    # Sort by part index and return just the paths
    parts.sort(key=lambda x: x[0])
    return [p for _, p in parts]


def merge_video_parts(
    part_paths: list[Path],
    output_path: Path,
    ffmpeg_path: str = "ffmpeg",
    ffprobe_path: str | None = None,
) -> bool:
    """Merge multiple video files using ffmpeg concat.

    Uses ffmpeg's concat demuxer to concatenate video files in order.
    Files are concatenated with stream copy (no re-encoding) for speed.

    Args:
        part_paths: List of video file paths to merge, in desired order.
        output_path: Path for the merged output file.
        ffmpeg_path: Path or command name for ffmpeg executable.
        ffprobe_path: Path or command name for ffprobe executable.
            If None, will attempt to resolve from ffmpeg_path.

    Returns:
        True if merge succeeded, False otherwise.

    Raises:
        subprocess.CalledProcessError: If ffmpeg command fails.
        ValueError: If part_paths is empty.
    """
    if not part_paths:
        raise ValueError("part_paths cannot be empty")

    # Resolve ffprobe path if not provided
    if ffprobe_path is None:
        ffprobe_path = _resolve_ffprobe_path(ffmpeg_path)

    # Create temporary concat list file
    concat_list = _create_concat_list(part_paths)
    if concat_list is None:
        logger.error("Failed to create concat list file")
        return False

    try:
        # Build ffmpeg command
        cmd = [
            ffmpeg_path,
            "-y",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_list),
            "-c",
            "copy",
            str(output_path),
        ]

        logger.info(
            "Merging %d parts into %s",
            len(part_paths),
            output_path.name,
        )

        # Execute ffmpeg
        subprocess.run(cmd, check=True, capture_output=True)

        # Verify output was created
        if not output_path.exists():
            logger.error("ffmpeg completed but output file not found: %s", output_path)
            return False

        # Verify duration matches expected (sum of parts)
        expected_duration = 0.0
        for part in part_paths:
            dur = get_video_duration(part, ffprobe_path)
            if dur:
                expected_duration += dur

        actual_duration = get_video_duration(output_path, ffprobe_path)
        if (
            expected_duration > 0
            and actual_duration
            and abs(actual_duration - expected_duration) > 1.0
        ):
            logger.warning(
                "Merged file duration (%.2fs) differs from expected (%.2fs)",
                actual_duration,
                expected_duration,
            )

        logger.info("Successfully merged to: %s", output_path)
        return True

    except (subprocess.CalledProcessError, FileNotFoundError, OSError) as exc:
        logger.error("ffmpeg merge failed: %s", exc)
        return False
    finally:
        # Clean up concat list file
        try:
            if concat_list.exists():
                concat_list.unlink()
        except Exception as exc:
            logger.debug("Failed to remove concat list: %s", exc)


def _resolve_ffprobe_path(ffmpeg_cmd: str = "ffmpeg") -> str:
    """Resolve ffprobe executable path.

    First tries to find ffprobe using shutil.which(). If not found,
    falls back to replacing 'ffmpeg' with 'ffprobe' in the provided
    ffmpeg command path.

    Args:
        ffmpeg_cmd: Path or command name for ffmpeg executable.

    Returns:
        Path to ffprobe executable.
    """
    ffprobe_path = shutil.which("ffprobe")
    if ffprobe_path:
        return ffprobe_path
    return ffmpeg_cmd.replace("ffmpeg", "ffprobe")


def _create_concat_list(part_paths: list[Path]) -> Path | None:
    """Create ffmpeg concat demuxer list file.

    Args:
        part_paths: List of video file paths to include.

    Returns:
        Path to the created list file, or None if creation failed.
    """
    try:
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            for part_path in part_paths:
                # Use absolute path and escape single quotes
                abs_path = str(part_path.resolve())
                # ffmpeg concat format requires escaping single quotes
                escaped_path = abs_path.replace("'", "'\\''")
                f.write(f"file '{escaped_path}'\n")
            return Path(f.name)
    except Exception as exc:
        logger.error("Failed to create concat list: %s", exc)
        return None


def should_merge_parts(parts: list[Path], base_file: Path) -> bool:
    """Determine if a multipart group should be merged.

    A group should be merged when:
    - There are 2+ part files, OR
    - There is at least 1 part file AND a base file exists

    Args:
        parts: List of part file paths.
        base_file: Path to the base file (without part suffix).

    Returns:
        True if the group should be merged, False otherwise.
    """
    has_multiple_parts = len(parts) >= 2
    has_base_with_part = len(parts) >= 1 and base_file.exists()
    return has_multiple_parts or has_base_with_part


def extract_epoch_from_filename(name: str) -> int:
    """Extract epoch timestamp from filename.

    Searches for a 9-13 digit number in the filename which represents
    a Unix epoch timestamp.

    Args:
        name: The filename to search for epoch timestamp.

    Returns:
        The extracted epoch as an integer, or 0 if not found.
    """
    try:
        match = re.search(r"(\d{9,13})", name)
        if match:
            return int(match.group(1))
    except (ValueError, AttributeError):
        pass
    return 0


def order_parts_by_epoch(parts: list[Path]) -> list[Path]:
    """Order part files by embedded epoch timestamp.

    Extracts epoch timestamps from filenames and sorts parts chronologically.
    Parts without epoch timestamps are placed at the end in their
    original order.

    Args:
        parts: List of part file paths.

    Returns:
        List of part paths sorted by epoch (oldest first).
    """
    with_epoch: list[tuple[int, int, Path]] = []
    without_epoch: list[tuple[int, Path]] = []

    for idx, part in enumerate(parts):
        epoch = extract_epoch_from_filename(part.name)
        if epoch > 0:
            with_epoch.append((epoch, idx, part))
        else:
            without_epoch.append((idx, part))

    # Sort by epoch, then by original index for stability
    with_epoch.sort(key=lambda x: (x[0], x[1]))

    # Combine: epoch-sorted parts first, then parts without epoch in original order
    result = [p for _, _, p in with_epoch]
    result.extend([p for _, p in without_epoch])

    return result

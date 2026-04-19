"""Video merge utilities for media archive synchronization.

This module provides functionality for merging multipart video files
using ffmpeg concat demuxer, including epoch-based clustering for
grouping related parts.
"""

import contextlib
import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable
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


def extract_epoch_from_filename(name: str) -> int | None:
    """Extract epoch timestamp from filename.

    Searches for a 9-13 digit number in the filename which represents
    a Unix epoch timestamp.

    Args:
        name: The filename to search for epoch timestamp.

    Returns:
        The extracted epoch as an integer, or None if not found.
    """
    try:
        match = re.search(r"(\d{9,13})", name)
        if match:
            return int(match.group(1))
    except (ValueError, AttributeError):
        pass
    return None


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
        if epoch is not None:
            with_epoch.append((epoch, idx, part))
        else:
            without_epoch.append((idx, part))

    # Sort by epoch, then by original index for stability
    with_epoch.sort(key=lambda x: (x[0], x[1]))

    # Combine: epoch-sorted parts first, then parts without epoch in original order
    result = [p for _, _, p in with_epoch]
    result.extend([p for _, p in without_epoch])

    return result


def cluster_by_epoch_window(
    names: list[str], window_seconds: int = 28800
) -> list[list[str]]:
    """Group filenames into clusters based on epoch timestamp proximity.

    Args:
        names: List of filenames to cluster.
        window_seconds: Maximum time difference (in seconds) for files
            to be considered in the same cluster. Defaults to 8 hours.

    Returns:
        List of clusters, where each cluster is a list of filenames.
    """
    items: list[tuple[float, str]] = []
    for n in names:
        extracted_epoch = extract_epoch_from_filename(n)
        if extracted_epoch is not None:
            items.append((float(extracted_epoch), n))
        else:
            items.append((float("inf"), n))

    items.sort(key=lambda x: x[0])
    groups: list[list[str]] = []
    cur_group: list[tuple[float, str]] = []
    cur_start: float | None = None

    for epoch, name in items:
        if epoch == float("inf"):
            if cur_group:
                groups.append([n for _, n in cur_group])
                cur_group = []
                cur_start = None
            groups.append([name])
            continue

        if not cur_group:
            cur_group = [(epoch, name)]
            cur_start = epoch
            continue

        assert cur_start is not None
        if epoch - cur_start <= window_seconds:
            cur_group.append((epoch, name))
        else:
            groups.append([n for _, n in cur_group])
            cur_group = [(epoch, name)]
            cur_start = epoch

    if cur_group:
        groups.append([n for _, n in cur_group])

    return groups


def should_merge_group(parts: list[Path], base_file: Path) -> bool:
    """Determine if a multipart group should be merged.

    A group should be merged when:
    - There are 2+ part files, OR
    - There is at least 1 part file AND a base file exists

    This is functionally equivalent to should_merge_parts but provided
    under the name used by the epoch-clustering merge workflow.

    Args:
        parts: List of part file paths.
        base_file: Path to the base file (without part suffix).

    Returns:
        True if the group should be merged, False otherwise.
    """
    return should_merge_parts(parts, base_file)


def _prepare_merge_order(
    parts: list[Path],
    base: str,
    ext: str,
    dry_run: bool = False,
) -> tuple[list[Path], Path, bool]:
    """Prepare merge order by epoch/part index.

    Orders parts by embedded epoch timestamp or part index, handles
    temporary renaming if epoch reordering is needed, and checks for
    existing base file to include in merge order.

    Args:
        parts: List of part file paths.
        base: Base filename without extension.
        ext: File extension including the dot.
        dry_run: If True, skip actual file renames.

    Returns:
        Tuple of (merge_order, merged_path, overwrite_existing).
    """
    parent = parts[0].parent

    def _part_index(p: Path) -> int:
        mm = re.search(r"_part(\d+)", p.name)
        return int(mm.group(1)) if mm else 0

    parts_by_index = sorted(parts, key=_part_index)

    epochs = {p: extract_epoch_from_filename(p.name) for p in parts_by_index}
    if any(v is not None and v > 0 for v in epochs.values()):

        def _epoch_sort_key(p: Path) -> tuple[int, int, int]:
            epoch_val = epochs.get(p) or 0
            has_real_epoch = 0 if epoch_val > 0 else 1
            return (has_real_epoch, epoch_val, _part_index(p))

        parts_by_epoch = sorted(parts_by_index, key=_epoch_sort_key)
        if [p.name for p in parts_by_epoch] != [p.name for p in parts_by_index]:
            logger.warning(
                "Reordering parts in %s by embedded epoch to ensure correct merge order",
                parent,
            )
            if dry_run:
                parts_by_index = list(parts_by_epoch)
            else:
                temp_map: dict[Path, Path] = {}
                temp_to_original: dict[Path, Path] = {}
                try:
                    for i, p in enumerate(parts_by_epoch, start=1):
                        new_name = f"{base}_part{i}{ext}"
                        new_path = parent / new_name
                        if p == new_path:
                            continue
                        tmp = p.with_suffix(p.suffix + ".merge_tmp")
                        p.rename(tmp)
                        temp_map[tmp] = new_path
                        temp_to_original[tmp] = p
                    for tmp, final in temp_map.items():
                        if tmp == final:
                            continue
                        if final.exists():
                            final.unlink()
                        tmp.rename(final)
                    parts_by_index = [
                        parent / f"{base}_part{i}{ext}"
                        for i in range(1, len(parts_by_epoch) + 1)
                    ]
                except OSError:
                    from contextlib import suppress

                    for tmp, orig in temp_to_original.items():
                        if tmp.exists():
                            with suppress(Exception):
                                tmp.rename(orig)
                    for final in temp_map.values():
                        if final.exists():
                            with suppress(Exception):
                                final.unlink()
                    raise

    merge_order = parts_by_index
    try:
        base_file = parent / f"{base}{ext}"
        if base_file.exists() and merge_order and merge_order[0] != base_file:
            merge_order = [p for p in merge_order if p != base_file]
            merge_order.insert(0, base_file)
    except (OSError, ValueError) as exc:
        logger.debug("Failed to reorder merge_order for base file: %s", exc)

    merged_path = parent / f"{base}{ext}"
    overwrite_existing = merged_path.exists()
    if overwrite_existing:
        try:
            if merged_path not in merge_order:
                merge_order = [merged_path] + [
                    p for p in merge_order if p != merged_path
                ]
        except (OSError, ValueError) as exc:
            logger.debug(
                "Failed to reorder merge_order for existing merged file: %s", exc
            )

    return merge_order, merged_path, overwrite_existing


def merge_multipart_group(
    parts: list[Path],
    base: str | None = None,
    ext: str | None = None,
    ffmpeg_path: str = "ffmpeg",
    dry_run: bool = False,
    backup_parts: bool = False,
    backup_suffix: str = ".bak",
) -> tuple[Path, Path] | None:
    """Merge a specific group of multipart video files.

    Takes a pre-determined list of part files and merges them into
    a single video file. Handles ordering by epoch or part index,
    and manages backup/removal of part files.

    Args:
        parts: List of Path objects representing the part files to merge.
        base: Optional base name for the merged file. Auto-detected if None.
        ext: Optional file extension. Auto-detected if None.
        ffmpeg_path: Path or command name for ffmpeg executable.
        dry_run: If True, only log what would be done without executing.
        backup_parts: If True, backup part files instead of deleting.
        backup_suffix: Suffix to append when backing up part files.
        progress_cb: Unused parameter, kept for backward compatibility.

    Returns:
        Tuple of (merged_file_path, parent_directory) on success,
        or None if the operation failed or was skipped.
    """
    try:
        if not parts:
            return None
        parts = list(parts)
        parent = parts[0].parent
        if base is None or ext is None:
            m = re.match(
                r"^(?P<base>.+)_part(?P<idx>\d+)(?P<ext>\.[^.]+)$", parts[0].name
            )
            if m:
                base = base or m.group("base")
                ext = ext or m.group("ext")
            else:
                base = base or parts[0].stem
                ext = ext or parts[0].suffix

        merge_order, merged_path, overwrite_existing = _prepare_merge_order(
            parts, base, ext, dry_run
        )

        if dry_run:
            logger.info(
                "DRY-RUN: would merge %d parts into %s", len(merge_order), merged_path
            )
            return (merged_path, parent)

        ffprobe_path = _resolve_ffprobe_path(ffmpeg_path)
        listfile = _create_concat_list(merge_order)
        if listfile is None:
            logger.error("Failed to create concat list for merge")
            return None

        out_path = merged_path.with_suffix(".recreated" + merged_path.suffix)

        cmd = [
            ffmpeg_path,
            "-y",
            "-hide_banner",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(listfile),
            "-c",
            "copy",
            str(out_path),
        ]
        logger.info("Merging %d parts into %s", len(merge_order), merged_path)

        try:
            subprocess.run(cmd, check=True, capture_output=True)

            if not out_path.exists():
                logger.error("ffmpeg completed but output file not found: %s", out_path)
                return None

            expected_duration = 0.0
            for part in merge_order:
                dur = get_video_duration(part, ffprobe_path)
                if dur:
                    expected_duration += dur

            actual_duration = get_video_duration(out_path, ffprobe_path)
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

            for p in merge_order:
                try:
                    if p.exists():
                        if overwrite_existing and p.resolve() == merged_path.resolve():
                            continue
                        if backup_parts:
                            try:
                                bak = p.with_suffix(p.suffix + backup_suffix)
                                p.rename(bak)
                                logger.info("Backed up part %s to %s", p, bak)
                            except Exception:
                                logger.debug(
                                    "Failed to back up part %s", p, exc_info=True
                                )
                        else:
                            p.unlink()
                except Exception:
                    logger.debug("Failed to remove part %s after merge", p)

            rename_ok = False
            try:
                if overwrite_existing:
                    bak = merged_path.with_suffix(merged_path.suffix + ".bak2")
                    try:
                        merged_path.rename(bak)
                        logger.info("Backed up original merged file to %s", bak)
                    except Exception:
                        logger.debug(
                            "Failed to back up original merged file %s", merged_path
                        )
                    out_path.rename(merged_path)
                else:
                    if merged_path.exists():
                        try:
                            merged_path.unlink()
                        except Exception:
                            logger.debug(
                                "Failed to remove existing file before rename: %s",
                                merged_path,
                            )
                    out_path.rename(merged_path)
                rename_ok = True
            except OSError as exc:
                logger.error(
                    "Failed to move recreated output into place for %s: %s",
                    merged_path,
                    exc,
                )

            if not rename_ok:
                return None

            logger.info("Successfully merged to: %s", merged_path)
            return (merged_path, parent)

        except (subprocess.CalledProcessError, FileNotFoundError, OSError) as exc:
            logger.error("ffmpeg merge failed: %s", exc)
            with contextlib.suppress(OSError):
                if out_path.exists():
                    out_path.unlink()
            return None
        finally:
            with contextlib.suppress(OSError):
                if listfile.exists():
                    listfile.unlink()

    except (OSError, ValueError, TypeError, RuntimeError):
        logger.debug("merge_multipart_group failed", exc_info=True)
        return None


def merge_multipart_videos(
    media_root: Path,
    ffmpeg_path: str = "ffmpeg",
    dry_run: bool = True,
    backup_parts: bool = False,
    backup_suffix: str = ".bak",
    progress_cb: Callable[[float, float], None] | None = None,
    directories: list[Path] | None = None,
) -> list[tuple[Path, Path, list[Path]]]:
    """Scan for and merge multipart video files.

    Automatically detects multipart video files (matching pattern *_partN.*)
    in the specified directories, orders them by embedded epoch or part index,
    and merges them using ffmpeg concat.

    Args:
        media_root: Root directory for media files.
        ffmpeg_path: Path or command name for ffmpeg executable.
        dry_run: If True, only log what would be done without executing.
        backup_parts: If True, backup part files instead of deleting.
        backup_suffix: Suffix to append when backing up part files.
        progress_cb: Optional callback for merge progress updates.
        directories: Optional list of specific directories to scan. If provided,
            only these directories will be scanned for multipart files instead
            of the entire media_root.

    Returns:
        List of tuples containing (merged_path, parent_dir, source_parts) for
        each successful merge operation.
    """
    groups: dict[tuple[Path, str, str], list[Path]] = {}

    if directories is not None:
        files_to_scan: list[Path] = []
        for directory in directories:
            if directory.exists() and directory.is_dir():
                files_to_scan.extend(directory.rglob("*_part*.*"))
    else:
        files_to_scan = list(media_root.rglob("*_part*.*"))

    for p in files_to_scan:
        if not p.is_file():
            continue
        if p.suffix.lower() not in VIDEO_EXTENSIONS:
            continue
        m = re.match(r"^(?P<base>.+)_part(?P<idx>\d+)(?P<ext>\.[^.]+)$", p.name)
        if not m:
            continue
        base = m.group("base")
        ext = m.group("ext")
        groups.setdefault((p.parent, base, ext), []).append(p)

    merged_results: list[tuple[Path, Path, list[Path]]] = []

    for (parent, base, ext), parts in groups.items():
        base_file = parent / f"{base}{ext}"
        if not should_merge_group(parts, base_file):
            continue

        if dry_run:
            result = merge_multipart_group(
                parts,
                base=base,
                ext=ext,
                dry_run=True,
                backup_parts=backup_parts,
                backup_suffix=backup_suffix,
            )
            if result is not None:
                merged_path_result, parent_dir = result
                source_parts = [p for p in parts if p != merged_path_result]
                merged_results.append((merged_path_result, parent_dir, source_parts))
            continue

        result = merge_multipart_group(
            parts,
            base=base,
            ext=ext,
            ffmpeg_path=ffmpeg_path,
            dry_run=False,
            backup_parts=backup_parts,
            backup_suffix=backup_suffix,
        )

        if result is not None:
            merged_path_result, parent_dir = result
            source_parts = [p for p in parts if p != merged_path_result]
            merged_results.append((merged_path_result, parent_dir, source_parts))

    return merged_results

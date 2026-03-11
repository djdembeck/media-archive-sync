#!/usr/bin/env python3
"""CLI for media-archive-sync."""

import argparse
import urllib.parse
from pathlib import Path

from . import ArchiveConfig, crawl_archive, download_with_config


def main():
    parser = argparse.ArgumentParser(
        description="Download media from web archives"
    )
    parser.add_argument(
        "--remote",
        required=True,
        help="Remote archive URL"
    )
    parser.add_argument(
        "--local",
        default="./media",
        help="Local download path (default: ./media)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Number of download workers (default: 3)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only, don't download"
    )
    parser.add_argument(
        "--organize",
        action="store_true",
        help="Organize files by month after download"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress non-essential output"
    )

    args = parser.parse_args()

    config = ArchiveConfig(
        remote_base=args.remote,
        local_root=Path(args.local),
        workers=args.workers,
        quiet=args.quiet,
    )

    # Crawl
    media_list, dir_counts = crawl_archive(
        remote_base=config.remote_base,
        max_depth=config.max_depth,
        video_extensions=config.video_extensions,
    )
    print(f"Found {len(media_list)} files")

    def _compute_target_path(url: str, local_root: Path) -> Path:
        parsed = urllib.parse.urlparse(url)
        path = parsed.path if parsed.path.startswith("/") else "/" + parsed.path
        rel_path = Path(path).relative_to("/")
        return local_root / rel_path

    if not args.dry_run:
        download_with_config(
            media_list=[(url, _compute_target_path(url, config.local_root)) for url, name in media_list],
            config=config,
        )

    # Organize
    if args.organize:
        from .organizer import organize_files_by_month
        organized = organize_files_by_month(
            local_root=config.local_root,
            month_format=config.month_folder_format,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            import shutil
            for month_folder, file_list in organized.items():
                target_dir = config.local_root / month_folder
                target_dir.mkdir(parents=True, exist_ok=True)
                for filepath in file_list:
                    target_path = target_dir / filepath.name
                    if target_path != filepath:
                        shutil.move(str(filepath), str(target_path))


if __name__ == "__main__":
    main()

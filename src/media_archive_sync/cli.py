#!/usr/bin/env python3
"""CLI for media-archive-sync."""

import argparse
import sys
from pathlib import Path

from . import ArchiveConfig, crawl_archive, download_files


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
    media_list, dir_counts = crawl_archive(config=config)
    print(f"Found {len(media_list)} files")

    # Download
    if not args.dry_run:
        download_files(config=config)

    # Organize
    if args.organize:
        from .organizer import organize_files_by_month
        organize_files_by_month(
            config.local_root,
            config.month_folder_format
        )


if __name__ == "__main__":
    main()

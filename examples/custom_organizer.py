#!/usr/bin/env python3
"""Advanced example: Custom organization with NFO generation."""

from pathlib import Path
from media_archive_sync import (
    ArchiveConfig, 
    crawl_archive, 
    DownloadManager,
)

config = ArchiveConfig(
    remote_base="https://archive.example.com/vods/",
    local_root=Path("./media"),
    month_folder_format="%Y-%m",  # e.g., "2026-01"
    write_nfo=True,
    strip_tokens={"gg", "tts", "ad"},
    workers=5,
)

# Custom download with progress
downloader = DownloadManager(config)
media_list, _ = crawl_archive(config=config)
downloader.download_batch(media_list)

# Organize and create NFOs
from media_archive_sync.organizer import organize_files_by_month
organize_files_by_month(
    config.local_root,
    config.month_folder_format,
)
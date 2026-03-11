#!/usr/bin/env python3
"""Basic example: Download all files from an archive."""

from pathlib import Path

from media_archive_sync import ArchiveConfig, crawl_archive, download_files

# Configure
config = ArchiveConfig(
    remote_base="https://archive.example.com/vods/",
    local_root=Path("./downloads"),
    workers=5,
)

# Crawl
media_list, dir_counts = crawl_archive(config=config)
print(f"Found {len(media_list)} files")

# Download
download_files(config=config)

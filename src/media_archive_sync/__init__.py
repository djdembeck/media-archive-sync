"""Media Archive Sync - Download and organize media from web archives."""

__version__ = "0.1.0"

from .config import ArchiveConfig
from .crawler import crawl_archive, fetch_directory, fetch_html
from .strings import urldecode, normalise_string, sanitize_title_for_filename
from .logging import get_logger
from .organizer import (
    extract_epoch_from_name,
    extract_date_from_epoch,
    load_local_files,
    load_local_index,
    organize_files_by_month,
    get_target_path,
)
from .downloader import (
    download_file,
    download_files,
    download_with_config,
    DownloadManager,
)
from .merge import (
    merge_video_parts,
    detect_video_parts,
    get_video_duration,
)

__all__ = [
    "__version__",
    "ArchiveConfig",
    "crawl_archive",
    "fetch_directory",
    "fetch_html",
    "urldecode",
    "normalise_string",
    "sanitize_title_for_filename",
    "get_logger",
    "extract_epoch_from_name",
    "extract_date_from_epoch",
    "load_local_files",
    "load_local_index",
    "organize_files_by_month",
    "get_target_path",
    "download_file",
    "download_files",
    "download_with_config",
    "DownloadManager",
    "merge_video_parts",
    "detect_video_parts",
    "get_video_duration",
]

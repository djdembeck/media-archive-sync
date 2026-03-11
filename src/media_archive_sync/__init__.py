"""Media Archive Sync - Download and organize media from web archives."""

__version__ = "0.1.0"

from .config import ArchiveConfig
from .crawler import crawl_archive, fetch_directory, fetch_html
from .downloader import (
    DownloadManager,
    download_file,
    download_files,
    download_with_config,
)
from .logging import get_logger
from .merge import (
    detect_video_parts,
    get_video_duration,
    merge_video_parts,
)
from .organizer import (
    extract_date_from_epoch,
    extract_epoch_from_name,
    get_target_path,
    load_local_files,
    load_local_index,
    organize_files_by_month,
)
from .strings import normalise_string, sanitize_title_for_filename, urldecode

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

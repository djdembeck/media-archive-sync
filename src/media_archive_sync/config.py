"""Configuration dataclass for media archive sync."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Set


@dataclass
class ArchiveConfig:
    """Configuration for media archive sync."""

    # Remote source
    remote_base: str = ""  # e.g., "https://archive.example.com/vods/"

    # Local storage
    local_root: Path = field(default_factory=lambda: Path("./media"))
    max_depth: int = 4

    # File types
    video_extensions: Set[str] = field(default_factory=lambda: {".mp4", ".mkv", ".avi", ".mov", ".webm"})

    # Organization
    month_folder_format: str = "%b_%Y"  # e.g., "jan_2026"
    use_month_folders: bool = True

    # Naming
    sanitize_replacements: dict = field(default_factory=dict)
    strip_tokens: Set[str] = field(default_factory=set)  # e.g., {"gg", "tts"}

    # Downloads
    workers: int = 3
    skip_existing: bool = True
    partial_extension: str = ".partial"

    # Cache
    cache_dir: Optional[Path] = None
    cache_backend: str = "sqlite"  # "sqlite" or "json"

    # NFO
    write_nfo: bool = True
    nfo_overwrite: bool = False

    # Display
    quiet: bool = False
    force_progress: bool = False

    # HTTP settings
    request_timeout: int = 15
    max_retries: int = 3
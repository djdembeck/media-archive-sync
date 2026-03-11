# Media Archive Sync

[![CI](https://github.com/djdembeck/media-archive-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/djdembeck/media-archive-sync/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/media-archive-sync.svg)](https://badge.fury.io/py/media-archive-sync/)
[![Python versions](https://img.shields.io/pypi/pyversions/media-archive-sync.svg)](https://pypi.org/project/media-archive-sync/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Download and organize media files from web archives (Apache-style directory listings).

## Features

- 🌐 **Web Archive Crawling** - Crawl Apache-style directory listings
- 📥 **Parallel Downloads** - Download multiple files simultaneously with resume support
- 📁 **Smart Organization** - Organize by date, custom naming conventions
- 📝 **Metadata Support** - Generate NFO sidecar files
- 🎬 **Video Merging** - Concatenate multipart video files
- 💾 **Resume Support** - Cache progress and resume interrupted downloads
- 🔧 **Configurable** - Everything is configurable, no hardcoded values

## Installation

### From PyPI (recommended)

```bash
pip install media-archive-sync
```

### From source

```bash
pip install git+https://github.com/djdembeck/media-archive-sync.git
```

### Development install

```bash
git clone https://github.com/djdembeck/media-archive-sync.git
cd media-archive-sync
pip install -e ".[dev]"

# Enable git hooks (runs lint/tests automatically)
git config --local core.hooksPath .githooks
```

Or use the Makefile for one-step setup:

```bash
make dev-install  # Installs deps and enables hooks
```

## Quick Start

```python
from media_archive_sync import ArchiveConfig, crawl_archive, download_files

# Configure
config = ArchiveConfig(
    remote_base="https://archive.example.com/vods/",
    local_root="./downloads",
    workers=5,
)

# Crawl remote archive
media_list, dir_counts = crawl_archive(config=config)
print(f"Found {len(media_list)} files")

# Download
 download_files(config=config)
```

## CLI Usage

```bash
# Basic download
media-archive-sync --remote https://archive.example.com/vods/ --local ./downloads

# With organization by month
media-archive-sync --remote https://archive.example.com/vods/ --local ./media --organize

# Dry run (preview only)
media-archive-sync --remote https://archive.example.com/vods/ --dry-run

# Parallel downloads with 10 workers
media-archive-sync --remote https://archive.example.com/vods/ --workers 10
```

## Docker Usage

```bash
# Pull image
docker pull ghcr.io/djdembeck/media-archive-sync:latest

# Run with mounted volumes
docker run --rm \
    -v /host/media:/media:rw \
    -v /host/cache:/app/.cache:rw \
    ghcr.io/djdembeck/media-archive-sync:latest \
    --remote https://archive.example.com/vods/ \
    --local /media
```

## Configuration

See [Configuration Guide](docs/configuration.md) for all available options.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) file.
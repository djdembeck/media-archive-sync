# Media Archive Sync

[![CI](https://github.com/djdembeck/media-archive-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/djdembeck/media-archive-sync/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/media-archive-sync.svg)](https://badge.fury.io/py/media-archive-sync/)
[![Python versions](https://img.shields.io/pypi/pyversions/media-archive-sync.svg)](https://pypi.org/project/media-archive-sync/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Download and organize media files from web archives (Apache-style directory listings).

## Long Description

Media Archive Sync is a Python library and CLI for downloading and organizing media from web archives that expose Apache-style directory listings. It crawls listings, downloads files in parallel with resume support, organizes local files, writes Kodi-compatible NFO sidecars, and can merge multipart videos.

Use it to maintain local copies of publicly hosted VOD archives, stream recordings, and other media collections without an archive API or RSS feed. The project provides both a Python API and a CLI, is ready for Docker deployments, and keeps configuration in `ArchiveConfig` rather than external configuration files.

## Table of Contents

- [Long Description](#long-description)
- [Table of Contents](#table-of-contents)
- [Background](#background)
- [Install](#install)
  - [Docker](#docker)
  - [PyPI](#pypi)
- [Usage](#usage)
  - [CLI](#cli)
  - [Library API](#library-api)
- [Features](#features)
- [Configuration](#configuration)
- [API](#api)
- [Examples](#examples)
- [Building](#building)
  - [Development install](#development-install)
  - [Build the Docker image](#build-the-docker-image)
- [Contributing](#contributing)
- [License](#license)

## Background

Public media archives often consist of flat Apache directory listings with no structured API. Media Archive Sync turns that listing into a repeatable pipeline: crawl the archive, download files with retries and cached progress, optionally organize and merge them, and preserve metadata in sidecar files.

## Install

Python package installation requires Python 3.11 or newer. Docker is the fastest way to run the pre-built image.

### Docker

Pull the pre-built image and mount the media and cache directories:

```bash
docker pull ghcr.io/djdembeck/media-archive-sync:latest

docker run --rm \
    -v /host/media:/media:rw \
    -v /host/cache:/app/.cache:rw \
    ghcr.io/djdembeck/media-archive-sync:latest \
    --remote https://archive.example.com/vods/ \
    --local /media
```

The image entrypoint is `media-archive-sync`; its declared volumes are `/media` and `/app/.cache`.

### PyPI

Install the released package with pip:

```bash
pip install media-archive-sync
```

## Usage

### CLI

The console script requires a remote archive URL. Start with the minimal command, then add the options you need:

```bash
# Basic download
media-archive-sync --remote https://archive.example.com/vods/ --local ./media

# Preview a download without writing files, and suppress non-essential output
media-archive-sync \
    --remote https://archive.example.com/vods/ \
    --local ./media \
    --dry-run --quiet

# Download in parallel with 10 workers
media-archive-sync \
    --remote https://archive.example.com/vods/ \
    --local ./media \
    --workers 10

# Organize downloaded files by month
media-archive-sync \
    --remote https://archive.example.com/vods/ \
    --local ./media \
    --workers 5 \
    --organize
```

Use `media-archive-sync --help` for the complete option list. The available flags are `--remote`, `--local`, `--workers`, `--dry-run`, `--organize`, and `--quiet`.

### Library API

Use the library when you need to integrate crawling and downloading into a Python program. The crawler returns `(URL, filename)` pairs; convert those entries to local paths before passing them to `download_files`:

```python
from pathlib import Path

from media_archive_sync import ArchiveConfig, crawl_archive, download_files

config = ArchiveConfig(
    remote_base="https://archive.example.com/vods/",
    local_root=Path("./media"),
    workers=5,
)

media_list, _ = crawl_archive(
    remote_base=config.remote_base,
    max_depth=config.max_depth,
    video_extensions=config.video_extensions,
)
download_files(
    [
        (url, config.local_root / filename)
        for url, filename in media_list
    ],
    workers=config.workers,
    skip_existing=config.skip_existing,
    timeout=config.request_timeout,
    max_retries=config.max_retries,
)
```

The [`examples/basic_download.py`](examples/basic_download.py) script contains the same crawl-and-download flow. For custom progress and organization, see [`examples/custom_organizer.py`](examples/custom_organizer.py).

## Features

- 🌐 **Web Archive Crawling** - Crawl Apache-style directory listings
- 📥 **Parallel Downloads** - Download multiple files simultaneously with resume support
- 📁 **Smart Organization** - Organize by date, custom naming conventions
- 📝 **Metadata Support** - Generate NFO sidecar files
- 🎬 **Video Merging** - Concatenate multipart video files
- 💾 **Resume Support** - Cache progress and resume interrupted downloads
- 🔧 **Configurable** - Everything is configurable, no hardcoded values

## Configuration

Configure the library with the `ArchiveConfig` dataclass. There are no external configuration files. The CLI maps its options to this configuration object for each run.

<details>
<summary>Common configuration fields</summary>

| Field | Default | Purpose |
| --- | --- | --- |
| `remote_base` | `""` | Base URL for the remote archive |
| `local_root` | `Path("./media")` | Local media directory |
| `workers` | `3` | Number of parallel download workers |
| `skip_existing` | `True` | Skip files that already exist |
| `cache_backend` | `"sqlite"` | Progress cache backend; `"sqlite"` or `"json"` |
| `cache_dir` | `None` | Directory for cache data |
| `write_nfo` | `True` | Write NFO sidecar files |
| `use_month_folders` | `True` | Enable month-based organization |
| `video_extensions` | `.mp4`, `.mkv`, `.avi`, `.mov`, `.webm` | File extensions to crawl |
| `max_retries` | `3` | Maximum retry attempts per file |
| `request_timeout` | `15` seconds | HTTP request timeout |

</details>

## API

The package exports the configuration object and the core crawl and download functions directly:

```python
from media_archive_sync import ArchiveConfig, crawl_archive, download_files
```

- `ArchiveConfig` stores remote, local, download, cache, organization, metadata, and HTTP settings.
- `crawl_archive` walks Apache-style listings and returns media entries plus per-directory counts.
- `download_files` downloads `(url, local_path)` pairs in parallel with resume, skip-existing, timeout, and retry options.
- `download_with_config` applies an `ArchiveConfig` to a download operation.
- `DownloadManager` is available when you need manager-level control over downloads.

The package also exports organizer, metadata, cache, string-normalization, and multipart-video merge functions. Inspect the module source or Python signatures for the complete public surface.

## Examples

- [`examples/basic_download.py`](examples/basic_download.py) — crawl an archive and download all discovered files.
- [`examples/custom_organizer.py`](examples/custom_organizer.py) — use `DownloadManager`, month-based organization, NFO generation, and filename token stripping.

## Building

The following commands are for contributors building from the repository rather than running the released package or pre-built image.

### Development install

Clone the repository and install the development dependencies:

```bash
git clone https://github.com/djdembeck/media-archive-sync.git
cd media-archive-sync
pip install -e ".[dev]"
```

The Makefile provides the equivalent setup and enables the repository hooks:

```bash
make dev-install
```

To configure the hooks separately:

```bash
make install-hooks
```

The development toolchain includes pytest, coverage, pytest-xdist, pytest-timeout, Ruff, Black, MyPy, responses, and httmock. See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and checks.

### Build the Docker image

Build the multi-stage image from the repository and run its help command:

```bash
docker build -t media-archive-sync .
docker run --rm media-archive-sync --help
```

The Dockerfile builds with `python:3.13-slim`, installs the package into a separate runtime stage, and includes `ffmpeg`, `mkvtoolnix`, and `mediainfo` in that runtime image.

For a non-editable package install directly from the repository, use:

```bash
pip install git+https://github.com/djdembeck/media-archive-sync.git
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, git hooks, quality checks, and pull request guidelines.

## License

MIT ([SPDX identifier](https://spdx.org/licenses/MIT.html)); see [LICENSE](LICENSE).

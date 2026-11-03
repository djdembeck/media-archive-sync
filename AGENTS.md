# Agents

## Downloader changes

This Python package is consumed as a library by `sync_wubby_modular`. Read `src/media_archive_sync/downloader.py` before downloader changes; use README and CONTRIBUTING.md for configuration and public-CLI contracts.

- Preserve fail-closed URL/address checks; the local-address exception is scoped to the operator's initial URL, never crawled follow-ups.
- Identity conflicts must stay bounded and consume the retry budget rather than allowing unbounded restarts.
- Confined downloads pin parent directories and use no-follow leaf operations. Do not replace descriptor-based confinement with a one-time path check that races symlink swaps.

## Public API changes

Preserve compatibility for library consumers such as `sync_wubby_modular`; moving or removing a public export requires a coordinated consumer migration, not a private refactor.

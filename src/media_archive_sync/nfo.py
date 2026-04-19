"""NFO file generation for video metadata.

This module provides functions to generate and write Kodi-compatible NFO XML
files from media metadata dictionaries.
"""

import contextlib
import html
import os
import re
import tempfile
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from .logging import get_logger

StrCollection = str | Sequence[str] | set[str]

logger = get_logger(__name__)


def parse_release_date(candidate, *, validate_epoch: bool = True) -> str | None:
    """Parse a date candidate into ISO format (YYYY-MM-DD).

    Attempts multiple parsing strategies in order:
    1. Timestamp (Unix seconds or milliseconds)
    2. ISO 8601 format (with Z timezone handling)
    3. First 10 characters as fallback

    Args:
        candidate: The date value to parse (string, number, or None).
        validate_epoch: When True (default), only treat numeric values as
            timestamps if they fall within a realistic epoch range
            (1e9 < val < 2e12). When False, skip epoch range validation
            and attempt timestamp conversion for any numeric value.

    Returns:
        ISO formatted date string (YYYY-MM-DD) or None if parsing fails.
    """
    if candidate is None:
        return None

    # Try timestamp parsing
    try:
        val = float(candidate)
        if validate_epoch and not (1e9 < val < 2e12):
            # Only treat as timestamp if in realistic epoch range (>1e9, <2e12)
            raise ValueError("Outside epoch range")
        if val > 1e11:
            val = val / 1000.0
        dt = datetime.fromtimestamp(val, tz=UTC)
        return dt.date().isoformat()
    except (ValueError, TypeError, OverflowError):
        pass

    # Try ISO format parsing
    try:
        s = str(candidate).strip()
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.date().isoformat()
    except (ValueError, TypeError):
        pass

    # Fallback to first 10 characters with YYYY-MM-DD validation
    try:
        s2 = str(candidate).strip()
        if len(s2) >= 10:
            candidate_date = s2[:10]
            # Validate YYYY-MM-DD format and ensure it's a real calendar date
            if re.match(
                r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$", candidate_date
            ):
                try:
                    datetime.strptime(candidate_date, "%Y-%m-%d")
                    return candidate_date
                except ValueError:
                    # Not a valid calendar date (e.g., 2023-02-30)
                    pass
    except (ValueError, TypeError):
        pass

    return None


def build_movie_nfo(
    title: str,
    year: int | None = None,
    plot: str | None = None,
    director: str | None = None,
    actors: StrCollection | None = None,
    genres: StrCollection | None = None,
    runtime: int | None = None,
    rating: float | None = None,
    original_title: str | None = None,
    releasedate: str | None = None,
    collections: StrCollection | None = None,
    uniqueid: dict[str, str] | None = None,
    *,
    kick_suffix: bool = False,
    kick_tag: bool = False,
    validate_epoch: bool = True,
) -> str:
    """Build an NFO XML string from media metadata.

    Constructs a Kodi-compatible movie NFO XML document with title,
    original title, sort title, year, rating, release date, collections,
    genres, actors, and director information.

    Args:
        title: The main title of the movie/video.
        year: Release year (optional).
        plot: Plot/summary text (optional).
        director: Director name (optional).
        actors: List of actor names (optional).
        genres: List of genre/tag names (optional).
        runtime: Runtime in minutes (optional).
        rating: Rating value (optional).
        original_title: Original title (e.g., non-English) (optional).
        releasedate: Release date in ISO format (optional).
        collections: List of collection/set names (optional).
        uniqueid: Dictionary of unique IDs (e.g., {"imdb": "tt12345"}) (optional).
        kick_suffix: If True, append `` (KICK)`` to the title (optional).
        kick_tag: If True, add a ``Kick Vod`` genre tag (optional).

    Returns:
        XML string representation of the movie NFO.
    """
    movie = ET.Element("movie")

    def _add_text(tag: str, text: str | None):
        """Add a text subelement if text is not empty."""
        if text is None:
            return
        s = str(text).strip()
        if not s:
            return
        el = ET.SubElement(movie, tag)
        el.text = s

    # Parse release date if provided
    if releasedate:
        parsed = parse_release_date(releasedate, validate_epoch=validate_epoch)
        releasedate = parsed

    effective_title = f"{title} (KICK)" if kick_suffix else title

    _add_text("title", effective_title)
    _add_text("originaltitle", original_title)
    _add_text("sorttitle", original_title if original_title else effective_title)
    _add_text("year", year)
    _add_text("plot", plot)
    _add_text("runtime", runtime)
    _add_text("rating", rating)
    _add_text("releasedate", releasedate)

    # Add director
    _add_text("director", director)

    # Add collections/sets
    # Normalize and filter entries first, sort sets for determinism
    collection_entries: list[str] = []
    if collections:
        if isinstance(collections, list | tuple | set):
            items = collections
        else:
            items = [collections]
        for s in items:
            if s is None:
                continue
            ss = str(s).strip()
            if ss:
                collection_entries.append(ss)
        # Sort if original was a set to ensure deterministic order
        if isinstance(collections, set):
            collection_entries = sorted(collection_entries)
    # Only create wrapper element if there are valid entries
    if collection_entries:
        c_el = ET.SubElement(movie, "collections")
        for ss in collection_entries:
            set_el = ET.SubElement(c_el, "set")
            set_el.text = ss

    if actors:
        seen_actors = set()
        if isinstance(actors, str):
            actor_list = [actors]
        elif isinstance(actors, set):
            actor_list = sorted(actors)
        else:
            actor_list = actors
        if not isinstance(actor_list, list | tuple):
            actor_list = [actor_list]
        for actor_name in actor_list:
            if not actor_name:
                continue
            name = str(actor_name).strip()
            if not name:
                continue
            key = name.lower()
            if key in seen_actors:
                continue
            seen_actors.add(key)
            actor_el = ET.SubElement(movie, "actor")
            name_el = ET.SubElement(actor_el, "name")
            name_el.text = name

    seen_genres: set[str] = set()
    if genres:
        if isinstance(genres, str):
            genre_list = [genres]
        elif isinstance(genres, set):
            genre_list = sorted(genres)
        else:
            genre_list = genres
        if not isinstance(genre_list, list | tuple):
            genre_list = [genre_list]
        for genre_name in genre_list:
            if not genre_name:
                continue
            name = str(genre_name).strip()
            if not name:
                continue
            with contextlib.suppress(ValueError):
                name = html.unescape(name)
            key = name.lower()
            if key not in seen_genres:
                seen_genres.add(key)
                _add_text("genre", name)

    if kick_tag:
        kick_key = "kick vod"
        if kick_key not in seen_genres:
            seen_genres.add(kick_key)
            _add_text("genre", "Kick Vod")

    # Add unique IDs
    if uniqueid:
        for id_type, id_value in uniqueid.items():
            if id_value:
                uid_el = ET.SubElement(movie, "uniqueid")
                uid_el.set("type", id_type)
                uid_el.set("default", "true" if id_type == "imdb" else "false")
                uid_el.text = str(id_value)

    return ET.tostring(movie, encoding="unicode")


def generate_nfo(
    meta: dict[str, Any],
    *,
    validate_epoch: bool = True,
    kick_suffix: bool = False,
    kick_tag: bool = False,
) -> str:
    """Generate an NFO XML string from a metadata dictionary.

    A dict-based convenience wrapper around :func:`build_movie_nfo` that
    extracts known keys from *meta* and passes them as keyword arguments.
    This matches the calling convention used by downstream consumers that
    work with raw metadata dicts.

    The *releasedate* value is parsed through :func:`parse_release_date`
    with the given *validate_epoch* setting before being forwarded.

    Args:
        meta: Dictionary containing media metadata. Recognised keys:
            ``title``, ``originaltitle``, ``year``, ``plot``, ``director``,
            ``actors``, ``genres``, ``runtime``, ``rating``, ``releasedate``,
            ``collections``, ``uniqueid``.
        validate_epoch: Forwarded to :func:`parse_release_date`.
        kick_suffix: Forwarded to :func:`build_movie_nfo`.
        kick_tag: Forwarded to :func:`build_movie_nfo`.

    Returns:
        XML string representation of the movie NFO.
    """
    releasedate = meta.get("releasedate")

    return build_movie_nfo(
        title=meta.get("title", ""),
        year=meta.get("year"),
        plot=meta.get("plot"),
        director=meta.get("director"),
        actors=meta.get("actors"),
        genres=meta.get("genres") if "genres" in meta else meta.get("tags"),
        runtime=meta.get("runtime"),
        rating=meta.get("rating"),
        original_title=meta.get("originaltitle"),
        releasedate=releasedate,
        collections=meta.get("collections"),
        uniqueid=meta.get("uniqueid"),
        kick_suffix=kick_suffix,
        kick_tag=kick_tag,
        validate_epoch=validate_epoch,
    )


def write_nfo_for_path(
    video_path: str | Path, nfo_data: str, overwrite: bool = False
) -> bool:
    """Write an NFO file for a media file.

    Writes the provided NFO XML content alongside the media file.
    If the NFO already exists with identical content, skips writing.
    Directory creation errors are suppressed internally.

    Args:
        video_path: Path to the media file (str or Path).
        nfo_data: NFO XML content string.
        overwrite: If True, overwrite existing NFO even if content matches.

    Returns:
        True if NFO was written or updated, False if skipped (identical content).

    Raises:
        OSError: If file write fails (directory creation errors are suppressed).
    """
    ppath = Path(video_path)
    p = ppath.with_suffix(".nfo")

    if p.is_file():
        if not overwrite:
            try:
                existing = p.read_text(encoding="utf-8")
                if existing == nfo_data:
                    logger.debug("NFO unchanged, skipping: %s", p)
                    return False
            except OSError as e:
                # Read failed, log and proceed to attempt write
                logger.warning("Cannot read existing NFO %s: %s", p, e)
        else:
            logger.info("Overwriting existing NFO: %s", p)

    with contextlib.suppress(OSError):
        p.parent.mkdir(parents=True, exist_ok=True)

    # Write to temp file and atomically replace to avoid corruption on crash
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", delete=False, dir=p.parent, suffix=".tmp"
    ) as tf:
        temp_path = tf.name
        try:
            tf.write(nfo_data)
            tf.flush()
            tf.close()
            os.replace(temp_path, p)
        except Exception:
            tf.close()
            with contextlib.suppress(OSError):
                os.unlink(temp_path)
            raise
    return True

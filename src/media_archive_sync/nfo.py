"""NFO file generation for video metadata.

This module provides functions to generate and write Kodi-compatible NFO XML
files from media metadata dictionaries.
"""

import html
import re
from datetime import UTC, datetime
from pathlib import Path
from xml.etree import ElementTree as ET

from .logging import get_logger

logger = get_logger(__name__)


def parse_release_date(candidate) -> str | None:
    """Parse a date candidate into ISO format (YYYY-MM-DD).

    Attempts multiple parsing strategies in order:
    1. Timestamp (Unix seconds or milliseconds)
    2. ISO 8601 format (with Z timezone handling)
    3. First 10 characters as fallback

    Args:
        candidate: The date value to parse (string, number, or None).

    Returns:
        ISO formatted date string (YYYY-MM-DD) or None if parsing fails.
    """
    if candidate is None:
        return None

    # Try timestamp parsing (validate realistic epoch range)
    try:
        val = float(candidate)
        # Only treat as timestamp if in realistic epoch range (>1e9, <1e12)
        if 1e9 < val < 1e12:
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
            # Validate YYYY-MM-DD format
            if re.match(
                r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$", candidate_date
            ):
                return candidate_date
    except (ValueError, TypeError):
        pass

    return None


def build_movie_nfo(
    title: str,
    year: int | None = None,
    plot: str | None = None,
    director: str | None = None,
    actors: list | None = None,
    genres: list | None = None,
    runtime: int | None = None,
    rating: float | None = None,
    original_title: str | None = None,
    releasedate: str | None = None,
    collections: list | None = None,
    uniqueid: dict[str, str] | None = None,
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
        parsed = parse_release_date(releasedate)
        if parsed:
            releasedate = parsed

    # Add basic metadata fields
    _add_text("title", title)
    _add_text("originaltitle", original_title)
    _add_text("sorttitle", original_title if original_title else title)
    _add_text("year", year)
    _add_text("plot", plot)
    _add_text("runtime", runtime)
    _add_text("rating", rating)
    _add_text("releasedate", releasedate)

    # Add director
    _add_text("director", director)

    # Add collections/sets
    if collections:
        c_el = ET.SubElement(movie, "collections")
        if isinstance(collections, (list, tuple)):
            for s in collections:
                if s is None:
                    continue
                ss = str(s).strip()
                if not ss:
                    continue
                set_el = ET.SubElement(c_el, "set")
                set_el.text = ss
        else:
            ss = str(collections).strip()
            if ss:
                set_el = ET.SubElement(c_el, "set")
                set_el.text = ss

    if actors:
        seen_actors = set()
        actor_list = [actors] if isinstance(actors, str) else actors
        if not isinstance(actor_list, (list, tuple, set)):
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
            try:
                actor_el = ET.SubElement(movie, "actor")
                name_el = ET.SubElement(actor_el, "name")
                name_el.text = name
            except Exception:
                pass

    if genres:
        seen_genres = set()
        genre_list = [genres] if isinstance(genres, str) else genres
        if not isinstance(genre_list, (list, tuple, set)):
            genre_list = [genre_list]
        for genre_name in genre_list:
            if not genre_name:
                continue
            name = str(genre_name).strip()
            if not name:
                continue
            try:
                name = html.unescape(name)
            except Exception:
                pass
            key = name.lower()
            if key not in seen_genres:
                seen_genres.add(key)
                _add_text("genre", name)

    # Add unique IDs
    if uniqueid:
        for id_type, id_value in uniqueid.items():
            if id_value:
                uid_el = ET.SubElement(movie, "uniqueid")
                uid_el.set("type", id_type)
                uid_el.set("default", "true" if id_type == "imdb" else "false")
                uid_el.text = str(id_value)

    return ET.tostring(movie, encoding="unicode")


def write_nfo_for_path(video_path, nfo_data: str, overwrite: bool = False) -> bool:
    """Write an NFO file for a media file.

    Writes the provided NFO XML content alongside the media file.
    If the NFO already exists with identical content, skips writing.

    Args:
        video_path: Path to the media file (str or Path).
        nfo_data: NFO XML content string.
        overwrite: If True, overwrite existing NFO even if content matches.

    Returns:
        True if NFO was written or updated, False if skipped (identical content).

    Raises:
        OSError: If file creation or directory creation fails.
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
            except OSError:
                # If we can't read the existing file, skip to be safe
                logger.debug("Cannot read existing NFO, skipping: %s", p)
                return False
        else:
            logger.info("Overwriting existing NFO: %s", p)

    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass

    p.write_text(nfo_data, encoding="utf-8")
    return True

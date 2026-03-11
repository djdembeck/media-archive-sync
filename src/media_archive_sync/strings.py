"""String transformation utilities for media archive synchronization.

This module contains string processing functions with no I/O dependencies,
making them easy to test and reuse across the codebase.
"""

import re
import urllib.parse


def urldecode(url: str) -> str:
    """Decode a percent-encoded URL string.

    Args:
        url: The URL string to decode.

    Returns:
        The decoded URL string.

    Raises:
        TypeError: If url is not a string.
    """
    if not isinstance(url, str):
        raise TypeError("url must be a str")
    return urllib.parse.unquote(url)


def normalise_string(s: str) -> str:
    """Lower-case, strip punctuation, collapse whitespace.

    Used to compare titles with server filenames. Strips file extensions
    and replaces punctuation with spaces so separators become word boundaries.

    Args:
        s: The string to normalize.

    Returns:
        The normalized string in lower case with collapsed whitespace.
    """
    # Handle None or non-string input
    if not s or not isinstance(s, str):
        return ""

    # Common video file extensions to strip when normalizing filenames
    video_extensions = {
        "mp4",
        "mkv",
        "avi",
        "mov",
        "webm",
        "m4v",
        "mpg",
        "mpeg",
        "flv",
        "wmv",
    }
    # Check if string ends with a known video extension
    if "." in s:
        ext = s.rsplit(".", 1)[1].lower()
        if ext in video_extensions:
            s = s.rsplit(".", 1)[0]

    # Replace punctuation with spaces so separators (.,-, etc.) become word
    # boundaries instead of being removed and gluing words together.
    # Treat underscore as a separator too (it's considered a word char \w)
    s = s.replace("_", " ")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)  # collapse whitespace
    return s.lower().strip()


def _strip_bang_tokens(title: str, tokens: set[str] | None = None) -> str:
    """Remove bang operator tokens like '!gg', '!tts', '!ad' and trailing
    '!tag' suffixes (case-insensitive).

    Central helper used by JSON title extraction and filename sanitisation.

    Args:
        title: The string to strip bang tokens from.
        tokens: Set of bang tokens to strip (e.g., {'gg', 'tts', 'ad'}).
            If None, uses an empty set.

    Returns:
        The string with bang tokens removed, or original string on error.
    """
    bang_tokens = tokens if tokens is not None else set()

    parts = []
    toks = str(title).split()
    for i, p in enumerate(toks):
        # Strip explicit advertising tags like '#ad' (case-insensitive)
        if p.lower() == "#ad":
            continue

        # If a '!tag' is attached to the end of a token without a space
        # (e.g., 'D!gg' or 'word!tts'), try to strip the '!tag' part when
        # it looks like an operator/tag we normally remove.
        if "!" in p and not p.startswith("!"):
            idx = p.rfind("!")
            prefix = p[:idx]
            suffix = p[idx + 1 :]
            is_known = suffix.lower() in bang_tokens or suffix.islower()
            if suffix and is_known:
                if prefix:
                    parts.append(prefix)
                continue

        # Handle operator tokens that begin with '!'
        if p.startswith("!"):
            tag = p[1:]
            if tag.lower() in bang_tokens:
                continue
            if tag.islower():
                continue
            # Keep bang-prefixed tokens when they are at the start or
            # when they are not the final token; otherwise drop trailing
            # uppercase bang tokens which are likely metadata.
            if i == 0 or i != len(toks) - 1:
                parts.append(p)
            continue

        parts.append(p)
    return " ".join(parts)


def sanitize_title_for_filename(
    title: str,
    replacements: dict | None = None,
    strip_tokens: set[str] | None = None,
) -> str:
    """Sanitize a title into a safe filename fragment.

    This is the canonical implementation used by download logic.
    It preserves word characters, hyphens and spaces, collapses whitespace,
    converts spaces/hyphens to periods, collapses repeated periods,
    and truncates to 120 chars.

    Args:
        title: The title string to sanitize.
        replacements: Optional dict of character replacements.
        strip_tokens: Optional set of bang tokens to strip (e.g., {'gg', 'tts', 'ad'}).

    Returns:
        A safe filename fragment, or "no.title" if result is empty.
    """
    if not title:
        return "no.title"

    # Strip bang tokens using the provided token set
    s = _strip_bang_tokens(str(title), strip_tokens)

    # Apply custom replacements if provided
    if replacements:
        for old, new in replacements.items():
            s = s.replace(old, new)

    s = re.sub(r"[^\w\- ]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("-", ".")
    s = s.replace(" ", ".")
    s = re.sub(r"\.{2,}", ".", s)
    # Strip leading/trailing dots then truncate, and ensure no trailing
    # dot remains after truncation (truncation can cut mid-token and
    # leave a trailing dot).
    s = s.strip(".")
    s = s[:120].rstrip(".")

    # Fallback if result is empty
    if not s:
        return "no.title"

    return s

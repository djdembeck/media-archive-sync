"""String transformation utilities for media archive synchronization.

This module contains string processing functions with no I/O dependencies,
making them easy to test and reuse across the codebase.
"""

import re
import urllib.parse

_DEFAULT_VIDEO_EXTENSIONS = frozenset(
    {
        ".mp4",
        ".mkv",
        ".avi",
        ".mov",
        ".webm",
        ".m4v",
        ".mpg",
        ".mpeg",
        ".flv",
        ".wmv",
        ".ts",
    }
)


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


def normalise_string(
    s: str,
    video_extensions: set[str] | None = None,
) -> str:
    """Lower-case, strip punctuation, collapse whitespace.

    Used to compare titles with server filenames. Strips file extensions
    and replaces punctuation with spaces so separators become word boundaries.

    Args:
        s: The string to normalize.
        video_extensions: Set of video extensions to strip (e.g., {'mp4', 'mkv'}).
            When None (default), strips a built-in set of common media extensions.
            When provided, only extensions in the set are stripped.

    Returns:
        The normalized string in lower case with collapsed whitespace.
    """
    # Handle None or non-string input
    if not s or not isinstance(s, str):
        return ""

    if video_extensions is not None:
        # Normalize extensions: strip leading dots and lowercase
        normalized_exts = {ext.lstrip(".").lower() for ext in video_extensions}
        # Explicit set: only strip known extensions
        if "." in s:
            ext = s.rsplit(".", 1)[1].lower()
            if ext in normalized_exts:
                s = s.rsplit(".", 1)[0]
    else:
        if "." in s:
            ext = s.rsplit(".", 1)[1].lower()
            if f".{ext}" in _DEFAULT_VIDEO_EXTENSIONS:
                s = s.rsplit(".", 1)[0]

    # Replace punctuation with spaces so separators (.,-, etc.) become word
    # boundaries instead of being removed and gluing words together.
    # Treat underscore as a separator too (it's considered a word char \w)
    s = s.replace("_", " ")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)  # collapse whitespace
    return s.lower().strip()


def _strip_bang_tokens(
    title: str,
    tokens: set[str] | None = None,
    strict_tokens: bool = True,
) -> str:
    """Remove bang operator tokens like '!gg', '!tts', '!ad' and trailing
    '!tag' suffixes (case-insensitive).

    Central helper used by JSON title extraction and filename sanitisation.

    Args:
        title: The string to strip bang tokens from.
        tokens: Set of bang tokens to strip (e.g., {'gg', 'tts', 'ad'}).
            If None, uses an empty set.
        strict_tokens: When True (default), strip only suffixes that match
            the provided *tokens* set (existing library behaviour). When
            False, strip ANY lowercase suffix after '!' — this matches the
            lenient behaviour of the legacy local implementation.

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
            if strict_tokens:
                # Only strip explicit bang tokens, not all lowercase suffixes
                is_known = suffix.lower() in bang_tokens
            else:
                is_known = suffix.lower() in bang_tokens or (
                    suffix.isalnum() and not suffix.isupper()
                )
            if suffix and is_known:
                if prefix:
                    parts.append(prefix)
                continue

        # Handle operator tokens that begin with '!'
        if p.startswith("!"):
            tag = p[1:]
            if tag.lower() in bang_tokens:
                continue
            if not strict_tokens and (tag.isalnum() and not tag.isupper()):
                continue
            # Keep bang-prefixed tokens when they are at the start or
            # when they are not the final token; otherwise drop trailing
            # uppercase bang tokens which are likely metadata.
            if i == 0 or i != len(toks) - 1 or not p[1:].isupper():
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
        strip_tokens: Optional set of bang tokens to strip (e.g., {\'gg\', \'tts\', \'ad\'}).

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


def normalise_stem(s: str) -> str:
    """Normalise a filename stem for matching.

    Normalizes without stripping a trailing dot-separated token, avoiding
    treating a final ALLCAPS token as an extension. Intended for filesystem
    stems (Path.stem).

    Args:
        s: The filename stem to normalize.

    Returns:
        The normalized stem in lower case with collapsed whitespace,
        or empty string if input is not a string.

    Raises:
        TypeError: If s is not a string.
    """
    if not isinstance(s, str):
        raise TypeError(f"Expected string, got {type(s).__name__}")

    s = s.replace("_", " ")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()


_WINDOWS_RESERVED = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }
)
_MAX_DIR_NAME_LENGTH = 255


def sanitize_dir_name(name: object | None = None) -> str:
    """Sanitize a collection name into a safe directory name.

    Strips characters illegal in directory names (slashes, colons,
    leading/trailing dots and spaces) while preserving spaces and
    most punctuation. Also strips null bytes, control characters,
    and enforces filesystem length limits.

    Note on '/' handling: forward slashes are replaced with ' - ' because
    they represent path separators, so preserving the semantic meaning as a
    separator makes sense. Other illegal characters (colons, asterisks, etc.)
    are simply removed since they have no meaningful separator interpretation.

    Args:
        name: The collection name to sanitize (accepts None or non-string).

    Returns:
        A sanitized safe directory name, or "Untitled" if input is empty.
    """
    if name is None:
        return "Untitled"
    if not isinstance(name, str):
        name = str(name)

    if not name or not name.strip():
        return "Untitled"

    s = name.strip()
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    s = s.replace("/", " - ")
    s = re.sub(r'[\\:*?"<>|]', "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.strip(". ")

    if s.upper().split(".")[0] in _WINDOWS_RESERVED:
        s = f"_{s}"

    while True:
        try:
            if len(s.encode("utf-8")) <= _MAX_DIR_NAME_LENGTH:
                break
        except UnicodeEncodeError:
            pass
        s = s[:-1]
    s = s.rstrip(". ")

    return s or "Untitled"


def server_basename_variants(
    name: str,
    bang_tokens: set[str] | None = None,
) -> list[str]:
    """Return plausible basename variants for a server filename.

    Used by orphan-JSON fallback to try matching moved files whose server
    names contain day prefixes, epoch suffixes or operator-like tokens.

    Args:
        name: The server filename to generate variants for.
        bang_tokens: Set of bang tokens to strip from variants
            (e.g., {'gg', 'tts', 'ad'}). If None, no token-based
            stripping is performed.

    Returns:
        A list of unique basename variants.
    """
    out: list[str] = []
    if not name:
        return out

    tokens = bang_tokens if bang_tokens is not None else set()

    try:
        base: str = str(name).rsplit(".", 1)[0]
        out.append(base)
        out.append(re.sub(r"^\d+_", "", base))
        out.append(re.sub(r"_(\d{1,4})$", "", base))
        out.append(re.sub(r"_(\d{9,13}(?:_\d+)?)$", "", base))
        v: str = re.sub(r"^\d+_", "", base)
        v = re.sub(r"_(\d{9,13}(?:_\d+)?)$", "", v)
        out.append(v)
        for tok in list(tokens):
            try:
                pat = re.compile(rf"[\._-]?{re.escape(tok)}$", re.I)
                out.append(re.sub(pat, "", base))
                out.append(re.sub(pat, "", v))
            except re.error:
                continue
    except AttributeError:
        pass

    seen: set[str] = set()
    res: list[str] = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            res.append(x)
    return res

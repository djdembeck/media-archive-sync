#!/usr/bin/env python3
"""Display utilities for terminal output and progress tracking.

This module provides generic progress bar support using tqdm and Rich,
with automatic fallback when Rich is not available.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterable
from contextlib import contextmanager
from typing import Any, TextIO


class _DummyTqdm:
    def __init__(self, iterable=None, *, desc=None, total=None, **kwargs):
        self.iterable = iterable
        self.desc = desc
        self.total = total
        self.n = 0

    def __iter__(self):
        if self.iterable is not None:
            yield from self.iterable

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def update(self, n=1):
        self.n += n

    def close(self):
        pass

    def refresh(self):
        pass

    def set_description_str(self, desc):
        self.desc = desc

    @staticmethod
    def write(msg, file=sys.stderr, end="\n"):
        print(msg, file=file, end=end)


try:
    from tqdm import tqdm
except ImportError:
    tqdm = _DummyTqdm


# Rich is optional - provides beautiful progress bars
try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskID,
        TextColumn,
        TimeRemainingColumn,
    )

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


# Global force progress flag - can be set by CLI
FORCE_PROGRESS = False

# Check if colors should be disabled (NO_COLOR environment variable)
# NO_COLOR is considered set if the key exists, even if the value is empty
NO_COLOR = "NO_COLOR" in os.environ


def _stderr_is_tty() -> bool:
    """Check if stderr is attached to a TTY."""
    return hasattr(sys.stderr, "isatty") and sys.stderr.isatty()


def tqdm_or_stderr(
    iterable: Iterable | None = None,
    desc: str | None = None,
    total: int | None = None,
    leave: bool = True,
    file: TextIO | None = None,
    disable: bool | None = None,
    unit: str = "it",
    unit_scale: bool | int | float = False,
    **kwargs,
):
    """tqdm wrapper that writes to stderr and respects FORCE_PROGRESS.

    Args:
        iterable: Iterable to decorate with a progressbar.
        desc: Prefix for the progressbar.
        total: The number of expected iterations.
        leave: If True, keeps all traces of the progressbar upon termination.
        file: Specifies where to output the progress messages (defaults to stderr).
        disable: Whether to disable the progress bar entirely.
        unit: String that will be scaled and displayed.
        unit_scale: If True, the number of iterations will be scaled automatically.
        **kwargs: Additional keyword arguments passed to tqdm.

    Returns:
        A decorated iterator or a tqdm progress bar instance.
    """
    if file is None:
        file = sys.stderr
    if disable is None:
        disable = not (FORCE_PROGRESS or _stderr_is_tty())

    return tqdm(
        iterable=iterable,
        desc=desc,
        total=total,
        leave=leave,
        file=file,
        disable=disable,
        unit=unit,
        unit_scale=unit_scale,
        **kwargs,
    )


class _TqdmProgressWrapper:
    """Simple wrapper to provide a consistent context manager interface for tqdm."""

    def __init__(
        self,
        desc: str | None = None,
        total: int | None = None,
        disable: bool = False,
        unit: str = "items",
    ):
        self.desc = desc
        self.total = total
        self.disable = disable
        self.unit = unit
        self._pbar: Any | None = None
        self.n = 0

    def __enter__(self) -> _TqdmProgressWrapper:
        self._pbar = tqdm_or_stderr(
            desc=self.desc,
            total=self.total,
            disable=self.disable,
            unit=self.unit,
        )
        return self

    def __exit__(self, *exc) -> bool:
        if self._pbar is not None:
            self._pbar.close()
        return False

    def update(self, n: float = 1) -> None:
        """Update progress by n units."""
        self.n += n
        if self._pbar is not None:
            self._pbar.update(n)

    def set_progress(self, value: float) -> None:
        """Set absolute progress value."""
        if self._pbar is not None and self.total is not None:
            self._pbar.n = int(value)
            self._pbar.refresh()
        self.n = value

    def set_description(self, text: str) -> None:
        """Update task description."""
        if self._pbar is not None:
            self._pbar.set_description_str(text)

    def write(self, s: str) -> None:
        """Write a message above the progress bar."""
        tqdm.write(s, file=sys.stderr)


if RICH_AVAILABLE:

    class _RichProgressWrapper:
        """Wrapper providing a consistent interface for Rich-based progress."""

        def __init__(
            self,
            desc: str | None = None,
            total: int | None = None,
            disable: bool = False,
            unit: str = "items",
        ):
            self.desc = desc or "Progress"
            self.total = total
            self.disable = disable
            self.unit = unit
            self._progress: Progress | None = None
            self._task_id: TaskID | None = None
            self.n = 0

        def __enter__(self) -> _RichProgressWrapper:
            if not self.disable:
                self._progress = Progress(
                    SpinnerColumn(),
                    TextColumn("{task.description}"),
                    BarColumn(),
                    TextColumn("[{task.completed}/{task.total}]"),
                    TimeRemainingColumn(),
                    console=Console(stderr=True, no_color=NO_COLOR),
                    disable=self.disable,
                )
                self._progress.start()
                self._task_id = self._progress.add_task(self.desc, total=self.total)
            return self

        def __exit__(self, *exc) -> bool:
            if self._progress is not None:
                self._progress.stop()
            return False

        def update(self, n: float = 1) -> None:
            """Update progress by n units."""
            self.n += n
            if self._progress is not None and self._task_id is not None:
                self._progress.advance(self._task_id, n)

        def set_progress(self, value: float) -> None:
            """Set absolute progress value."""
            self.n = value
            if self._progress is not None and self._task_id is not None:
                self._progress.update(self._task_id, completed=value)

        def set_description(self, text: str) -> None:
            """Update task description."""
            if self._progress is not None and self._task_id is not None:
                self._progress.update(self._task_id, description=text)

        def write(self, s: str) -> None:
            """Write a message above the progress bar."""
            if self._progress is not None:
                self._progress.console.print(s)
            else:
                print(s, file=sys.stderr)


def rich_progress_or_stderr(
    desc: str | None = None,
    total: int | None = None,
    disable: bool | None = None,
    unit: str = "items",
) -> Any:
    """Returns a Rich-based progress context manager if available, otherwise tqdm.

    This function provides a unified interface for progress display that
    automatically uses Rich-based progress bars when available, falling
    back to tqdm when Rich is not installed.

    Args:
        desc: Description text shown next to the progress bar.
        total: The total number of expected iterations.
        disable: Whether to disable the progress bar entirely.
        unit: The unit label displayed (e.g., "files", "items", "bytes").

    Returns:
        A context manager that can be used with 'with' statement.
        The returned object has update(), set_progress(), set_description(),
        and write() methods.

    Example:
        >>> with rich_progress_or_stderr(desc="Processing", total=100) as pbar:
        ...     for i in range(100):
        ...         pbar.update(1)
        ...         pbar.write(f"Processing item {i}")
    """
    if disable is None:
        disable = not (FORCE_PROGRESS or _stderr_is_tty())

    if RICH_AVAILABLE:
        return _RichProgressWrapper(desc=desc, total=total, disable=disable, unit=unit)
    else:
        return _TqdmProgressWrapper(desc=desc, total=total, disable=disable, unit=unit)


@contextmanager
def simple_progress(
    desc: str = "Processing",
    total: int | None = None,
    disable: bool | None = None,
):
    """Simple context manager for progress tracking.

    This is a convenience wrapper around rich_progress_or_stderr that
    provides a simpler interface for basic use cases.

    Args:
        desc: Description of the operation being tracked.
        total: Total number of items to process (None for unknown).
        disable: Whether to disable the progress display (None for autodetect).

    Yields:
        A progress object with update() and write() methods.

    Example:
        >>> items = ["a", "b", "c"]
        >>> with simple_progress("Processing items", total=len(items)) as p:
        ...     for item in items:
        ...         process(item)
        ...         p.update(1)
    """
    progress = rich_progress_or_stderr(desc=desc, total=total, disable=disable)
    try:
        with progress as p:
            yield p
    finally:
        pass


def safe_print(message: str, *args) -> None:
    """Print a message safely without interfering with progress bars.

    Uses stderr and handles the case where progress bars might be active.

    Args:
        message: The message format string.
        *args: Arguments to format into the message.
    """
    if args:
        message = message % args
    print(message, file=sys.stderr)


__all__ = [
    "rich_progress_or_stderr",
    "simple_progress",
    "tqdm_or_stderr",
    "safe_print",
    "FORCE_PROGRESS",
]

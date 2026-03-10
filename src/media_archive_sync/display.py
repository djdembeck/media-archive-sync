"""Display utilities for terminal output."""

import sys
from typing import Optional
from contextlib import contextmanager

try:
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


@contextmanager
def rich_progress_or_stderr(desc: str = "Working", unit: str = "items"):
    """Context manager for progress display.

    Uses rich if available, falls back to tqdm, then stderr.
    """
    if RICH_AVAILABLE:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
        ) as progress:
            yield progress
    elif TQDM_AVAILABLE:
        with tqdm(desc=desc, unit=unit) as pbar:
            yield pbar
    else:
        # Fallback: no progress, just yield a dummy object
        class DummyProgress:
            def update(self, n=1): pass
            def write(self, s): print(s, file=sys.stderr)
        yield DummyProgress()
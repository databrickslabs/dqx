"""Cross-process wake signal for the background scheduler.

The scheduler runs in exactly one process per container: whichever uvicorn
worker wins the ``/tmp/.dqx_scheduler.lock`` file lock (see ``app.py``). The
shipped launch commands start a single worker, but the command is a
user-overridable bundle variable, so a request can land in a worker that does
not host the scheduler. An in-memory event would never reach the scheduler
from such a worker, so the wake signal is a marker file next to the lock that
every process on the container can write.

Signalling appends one byte to the marker (``O_APPEND`` writes are atomic and
change both size and mtime, so back-to-back signals are never lost to coarse
mtime resolution). The scheduler loop compares the marker's version between
short waits while idle and cuts its sleep short when it changes. Both sides
are a single syscall: no SQL, no network.
"""

import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Lives in the container's temp directory (where the scheduler lock lives) so
# every worker on the container sees it.
SCHEDULER_WAKE_MARKER_PATH = Path(tempfile.gettempdir()) / ".dqx_scheduler.wake"

# The marker grows by one byte per signal; the scheduler truncates it once it
# passes this size so it stays tiny across long uptimes.
_MAX_MARKER_BYTES = 1024

MarkerVersion = tuple[int, int] | None


class SchedulerWakeMarker:
    """A file every process can touch to wake the scheduler from its idle sleep."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def signal(self) -> None:
        """Ask the scheduler to run its next tick now (best-effort, never raises)."""
        try:
            fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                os.write(fd, b".")
            finally:
                os.close(fd)
        except OSError:
            logger.warning("Could not signal the scheduler; it picks up new work on its next poll")

    def version(self) -> MarkerVersion:
        """Return a value that changes on every :meth:`signal`, or None when the marker is absent."""
        try:
            stat = os.stat(self._path)
        except OSError:
            return None
        return stat.st_mtime_ns, stat.st_size

    def compact(self) -> MarkerVersion:
        """Shrink an overgrown marker and return its current version.

        Called by the scheduler right after it consumed a signal. A signal
        that lands between the size check and the truncate is absorbed — the
        scheduler is about to tick anyway, which picks up whatever it was for.
        """
        current = self.version()
        if current is not None and current[1] > _MAX_MARKER_BYTES:
            try:
                os.truncate(self._path, 0)
            except OSError:
                logger.warning("Could not compact the scheduler wake marker")
            return self.version()
        return current

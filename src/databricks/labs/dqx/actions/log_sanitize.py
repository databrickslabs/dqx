"""Shared log-sanitization helper for the DQX actions subsystem.

User-supplied values (action names, conditions, destination names, table identifiers)
are embedded in log messages. Newlines and other control characters in those values can
forge log entries or corrupt log pipelines (CWE-117), so strip them before logging.

A single implementation lives here so every action module sanitizes identically — an
earlier divergence (one module stripped only CR/LF) let control characters slip through
on one code path.
"""

import re

__all__ = ["sanitize_for_log"]

_CONTROL_CHAR_RE = re.compile(r"[\r\n\t\x00-\x1f\x7f]")


def sanitize_for_log(text: str) -> str:
    """Replace newlines and control characters in *text* with spaces (CWE-117).

    Args:
        text: An arbitrary, possibly user-supplied string.

    Returns:
        The string with CR, LF, tab, and other C0/DEL control characters replaced by spaces.
    """
    return _CONTROL_CHAR_RE.sub(" ", text)

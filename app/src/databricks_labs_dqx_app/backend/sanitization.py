"""Shared sanitization helpers for untrusted display text."""

import unicodedata


def replace_control_characters(value: str, *, replacement: str = " ") -> str:
    """Replace Unicode control characters in untrusted text.

    Args:
        value: Text that may contain control characters.
        replacement: Text substituted for each control character.

    Returns:
        Text with every Unicode *Cc* character replaced.
    """
    return "".join(replacement if unicodedata.category(character) == "Cc" else character for character in value)


def safe_log_value(value: str) -> str:
    """Make an untrusted value safe to embed in a log message (CWE-117).

    Replaces every non-printable character (CR, LF, other control characters and
    Unicode line/paragraph separators) with ``?`` so the value cannot forge or split
    log entries.

    Args:
        value: Untrusted text such as a path or query id.

    Returns:
        The value with non-printable characters replaced.
    """
    return "".join(character if character.isprintable() else "?" for character in value)

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

"""Shared sanitization helpers for untrusted display text.

One definition of an unsafe character backs every helper here: any Unicode
control character (category *Cc*, which covers C0, DEL and C1 controls such as
U+0085 NEXT LINE) plus the Unicode line and paragraph separators (U+2028,
U+2029). Each of them can break a line in a log viewer, terminal or text field.
"""

import unicodedata

_LINE_SEPARATOR_CATEGORIES = frozenset({"Zl", "Zp"})  # U+2028 LINE SEPARATOR, U+2029 PARAGRAPH SEPARATOR


def is_unsafe_character(character: str) -> bool:
    """Whether *character* is a control character or a Unicode line/paragraph separator."""
    category = unicodedata.category(character)
    return category == "Cc" or category in _LINE_SEPARATOR_CATEGORIES


def has_unsafe_characters(value: str) -> bool:
    """Whether *value* contains any character :func:`is_unsafe_character` rejects."""
    return any(is_unsafe_character(character) for character in value)


def replace_control_characters(value: str, *, replacement: str = " ") -> str:
    """Replace control characters and line/paragraph separators in untrusted text.

    Args:
        value: Text that may contain control characters.
        replacement: Text substituted for each unsafe character.

    Returns:
        Text with every character :func:`is_unsafe_character` rejects replaced.
    """
    return "".join(replacement if is_unsafe_character(character) else character for character in value)


def safe_log_value(value: str) -> str:
    """Make an untrusted value safe to embed in a log message (CWE-117).

    Replaces everything :func:`replace_control_characters` replaces, plus any other
    non-printable character (for example bidi overrides), with ``?`` so the value
    cannot forge, split or visually disguise log entries.

    Args:
        value: Untrusted text such as a path or query id.

    Returns:
        The value with unsafe and non-printable characters replaced.
    """
    scrubbed = replace_control_characters(value, replacement="?")
    return "".join(character if character.isprintable() else "?" for character in scrubbed)

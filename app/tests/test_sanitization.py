"""Tests for shared untrusted-text sanitization helpers."""

from databricks_labs_dqx_app.backend.sanitization import (
    has_unsafe_characters,
    replace_control_characters,
    safe_log_value,
)


def test_replace_control_characters_handles_c0_and_c1_controls() -> None:
    assert replace_control_characters("admin\nname\u0085suffix") == "admin name suffix"
    assert replace_control_characters("admin\nname\u0085suffix", replacement="") == "adminnamesuffix"


def test_safe_log_value_strips_line_breaks_and_control_characters() -> None:
    assert safe_log_value("p1\r\nINFO forged entry") == "p1??INFO forged entry"
    assert safe_log_value("a\x00b\x1bc\u0085d\u2028e") == "a?b?c?d?e"
    assert safe_log_value("plain-id_123.x") == "plain-id_123.x"


def test_line_and_paragraph_separators_are_unsafe_everywhere() -> None:
    for separator in ("\u2028", "\u2029", "\u0085", "\n", "\x9b"):
        assert has_unsafe_characters(f"a{separator}b")
        assert replace_control_characters(f"a{separator}b") == "a b"
        assert safe_log_value(f"a{separator}b") == "a?b"
    assert not has_unsafe_characters("plain value=x.y_%")


def test_safe_log_value_also_scrubs_other_non_printable_characters() -> None:
    assert safe_log_value("a\u202eb") == "a?b"

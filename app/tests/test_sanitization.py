"""Tests for shared untrusted-text sanitization helpers."""

from databricks_labs_dqx_app.backend.sanitization import replace_control_characters, safe_log_value


def test_replace_control_characters_handles_c0_and_c1_controls() -> None:
    assert replace_control_characters("admin\nname\u0085suffix") == "admin name suffix"
    assert replace_control_characters("admin\nname\u0085suffix", replacement="") == "adminnamesuffix"


def test_safe_log_value_strips_line_breaks_and_control_characters() -> None:
    assert safe_log_value("p1\r\nINFO forged entry") == "p1??INFO forged entry"
    assert safe_log_value("a\x00b\x1bc\u0085d\u2028e") == "a?b?c?d?e"
    assert safe_log_value("plain-id_123.x") == "plain-id_123.x"

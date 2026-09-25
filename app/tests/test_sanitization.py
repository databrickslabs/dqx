"""Tests for shared untrusted-text sanitization helpers."""

from databricks_labs_dqx_app.backend.sanitization import replace_control_characters


def test_replace_control_characters_handles_c0_and_c1_controls() -> None:
    assert replace_control_characters("admin\nname\u0085suffix") == "admin name suffix"
    assert replace_control_characters("admin\nname\u0085suffix", replacement="") == "adminnamesuffix"

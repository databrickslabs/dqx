"""Unit tests for the shared log-sanitization helper."""

from databricks.labs.dqx.actions.log_sanitize import sanitize_for_log


def test_strips_cr_lf() -> None:
    assert sanitize_for_log("a\r\nb") == "a  b"


def test_strips_full_control_char_range() -> None:
    """Must strip tab and other C0/DEL control chars, not only CR/LF (the prior weak variant)."""
    raw = "name\tvalue\x1b[31m\x00\x7fend"
    cleaned = sanitize_for_log(raw)
    for control_char in ("\t", "\x1b", "\x00", "\x7f", "\r", "\n"):
        assert control_char not in cleaned
    assert "name" in cleaned and "end" in cleaned


def test_leaves_plain_text_unchanged() -> None:
    assert sanitize_for_log("error_row_count > 0") == "error_row_count > 0"

import os
import time

import pytest

from tests.conftest import apply_tz


@pytest.fixture
def force_non_utc_start():
    """Force a non-UTC starting timezone regardless of the host's actual default."""
    original = os.environ.get("TZ")
    # POSIX TZ string (not an IANA name), so glibc resolves it without needing tzdata
    apply_tz("EST5EDT")
    yield
    apply_tz(original)


@pytest.mark.skipif(
    not hasattr(time, "tzset"),
    reason="set_utc_timezone applies TZ via time.tzset(), which is POSIX-only; on Windows the "
    "fixture cannot force the process timezone, so this assertion is not meaningful there.",
)
def test_set_utc_timezone_overrides_a_non_utc_host(force_non_utc_start, set_utc_timezone):
    assert time.tzname[0] == "UTC"

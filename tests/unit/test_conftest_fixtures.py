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


def test_set_utc_timezone_overrides_a_non_utc_host(force_non_utc_start, set_utc_timezone):
    assert time.tzname[0] == "UTC"

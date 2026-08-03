import os
import time

import pytest

from tests.conftest import restore_tz_env


@pytest.fixture
def force_non_utc_start():
    """Force a non-UTC starting timezone regardless of the host's actual default."""
    original = os.environ.get("TZ")
    os.environ["TZ"] = "America/New_York"
    if hasattr(time, "tzset"):
        time.tzset()
    if time.tzname[0] == "UTC":
        pytest.skip("Host's tzdata doesn't resolve America/New_York; glibc fell back to UTC")
    yield
    restore_tz_env(original)


def test_set_utc_timezone_overrides_a_non_utc_host(force_non_utc_start, set_utc_timezone):
    assert time.tzname[0] == "UTC"

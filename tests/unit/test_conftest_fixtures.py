import os
import time

import pytest


@pytest.fixture
def force_non_utc_start():
    """Force a non-UTC starting timezone regardless of the host's actual default."""
    original = os.environ.get("TZ")
    os.environ["TZ"] = "America/New_York"
    if hasattr(time, "tzset"):
        time.tzset()
    yield
    if original is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = original
    if hasattr(time, "tzset"):
        time.tzset()


def test_set_utc_timezone_overrides_a_non_utc_host(force_non_utc_start, set_utc_timezone):
    assert time.tzname[0] == "UTC"

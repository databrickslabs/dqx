from databricks.labs.dqx.io import is_one_time_trigger


def test_trigger_is_none():
    assert not is_one_time_trigger(None)


def test_trigger_contains_once():
    trigger = {"once": True}
    assert is_one_time_trigger(trigger)


def test_trigger_contains_available_now():
    trigger = {"availableNow": True}
    assert is_one_time_trigger(trigger)


def test_trigger_does_not_contain_once_or_available_now():
    trigger = {"processingTime": "10 seconds"}
    assert not is_one_time_trigger(trigger)

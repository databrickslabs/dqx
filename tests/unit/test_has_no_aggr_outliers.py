"""Unit tests for has_no_aggr_outliers — validation and registration only (no Spark required)."""

import pytest

from databricks.labs.dqx.check_funcs import has_no_aggr_outliers
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY


class TestIsAggrNotAnomalousRegistration:
    """Verify the rule is correctly wired into the DQX registry."""

    def test_rule_is_registered_as_dataset(self):
        """has_no_aggr_outliers must appear in the registry with type 'dataset'."""
        assert "has_no_aggr_outliers" in CHECK_FUNC_REGISTRY
        assert CHECK_FUNC_REGISTRY["has_no_aggr_outliers"] == "dataset"

    def test_rule_returns_tuple_of_two(self):
        """Calling the function must return (Column, Callable)."""
        result = has_no_aggr_outliers("value", "event_date")
        assert isinstance(result, tuple)
        assert len(result) == 2
        _, apply_fn = result
        assert callable(apply_fn)

    # Exact parameter name, order, and kind for every registered check are asserted centrally by the
    # signature-contract test, which supersedes the parameter-name subset check that used to live here.


class TestIsAggrNotAnomalousValidation:
    """Parameter validation — must raise InvalidParameterError on bad inputs."""

    def test_validates_sigma_positive(self):
        """sigma <= 0 must raise InvalidParameterError."""
        with pytest.raises(InvalidParameterError, match="sigma"):
            has_no_aggr_outliers("value", "ts", sigma=0)

    def test_validates_sigma_negative(self):
        """Negative sigma must also raise."""
        with pytest.raises(InvalidParameterError, match="sigma"):
            has_no_aggr_outliers("value", "ts", sigma=-1.5)

    def test_validates_lookback_num_intervals_minimum(self):
        """lookback_num_intervals < 2 must raise (stddev undefined for 1 point)."""
        with pytest.raises(InvalidParameterError, match="lookback_num_intervals"):
            has_no_aggr_outliers("value", "ts", lookback_num_intervals=1)

    def test_validates_warmup_num_intervals_must_be_at_least_one(self):
        """warmup_num_intervals < 1 must raise."""
        with pytest.raises(InvalidParameterError, match="warmup_num_intervals"):
            has_no_aggr_outliers("value", "ts", warmup_num_intervals=0)

    def test_validates_warmup_num_intervals_must_not_exceed_lookback(self):
        """warmup_num_intervals > lookback_num_intervals must raise."""
        with pytest.raises(InvalidParameterError, match="warmup_num_intervals"):
            has_no_aggr_outliers("value", "ts", lookback_num_intervals=7, warmup_num_intervals=8)

    def test_validates_time_interval_unknown_value(self):
        """An unrecognised time_interval must raise."""
        with pytest.raises(InvalidParameterError, match="time_interval"):
            has_no_aggr_outliers("value", "ts", time_interval="quarter")

    def test_validates_time_interval_case_sensitive(self):
        """time_interval is case-sensitive; 'Day' (capital D) must raise."""
        with pytest.raises(InvalidParameterError, match="time_interval"):
            has_no_aggr_outliers("value", "ts", time_interval="Day")

    def test_validates_star_column_requires_count(self):
        """column='*' is only meaningful for count(*); other aggregates must raise (#1435)."""
        with pytest.raises(InvalidParameterError, match="only supported with the 'count' aggregate"):
            has_no_aggr_outliers("*", "ts", aggr_type="sum")

    def test_valid_parameters_do_not_raise(self):
        """Typical valid call must not raise any exception."""
        has_no_aggr_outliers(
            "revenue",
            "event_date",
            aggr_type="sum",
            sigma=2.5,
            lookback_num_intervals=10,
            warmup_num_intervals=5,
            time_interval="hour",
            group_by=["region"],
        )

    def test_all_valid_truncations_accepted(self):
        """Each of the five allowed time_interval values must be accepted."""
        for trunc in ("minute", "hour", "day", "week", "month"):
            has_no_aggr_outliers("value", "ts", time_interval=trunc)

    def test_non_curated_aggr_type_warns_not_raises(self):
        """A non-curated aggr_type should warn but not raise an error."""
        with pytest.warns(UserWarning, match="non-curated"):
            has_no_aggr_outliers("value", "ts", aggr_type="some_custom_agg")

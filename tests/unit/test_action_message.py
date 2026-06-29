"""Unit tests for AlertMessage and StandardMessageBuilder (Task 3)."""

from datetime import datetime, timezone

import pytest

from databricks.labs.dqx.actions.message import AlertMessage, StandardMessageBuilder


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_RUN_TIME = datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
_METRICS: dict[str, object] = {"error_row_count": 5, "warning_row_count": 2, "total_rows": 100}


# ---------------------------------------------------------------------------
# AlertMessage dataclass
# ---------------------------------------------------------------------------


class TestAlertMessage:
    def test_is_frozen(self):
        msg = AlertMessage(
            title="Test",
            summary="Test summary",
            condition="error_row_count > 0",
            table="catalog.schema.table",
            observed_metrics={"error_row_count": 5},
            run_id="run-001",
            run_time=_RUN_TIME,
            severity="error",
            fields={"key": "value"},
        )
        with pytest.raises(Exception):
            msg.title = "mutated"  # type: ignore[misc]

    def test_condition_can_be_none(self):
        msg = AlertMessage(
            title="Test",
            summary="Test summary",
            condition=None,
            table=None,
            observed_metrics={},
            run_id="run-001",
            run_time=_RUN_TIME,
            severity="error",
            fields={},
        )
        assert msg.condition is None

    def test_table_can_be_none(self):
        msg = AlertMessage(
            title="Test",
            summary="Test summary",
            condition=None,
            table=None,
            observed_metrics={},
            run_id="run-001",
            run_time=_RUN_TIME,
            severity="error",
            fields={},
        )
        assert msg.table is None


# ---------------------------------------------------------------------------
# StandardMessageBuilder.build — happy path
# ---------------------------------------------------------------------------


class TestStandardMessageBuilderBuild:
    def test_title_contains_action_name(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert "notify_on_errors" in msg.title

    def test_condition_stored_on_message(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert msg.condition == "error_row_count > 0"

    def test_condition_appears_in_summary(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert "error_row_count > 0" in msg.summary

    def test_table_stored_on_message(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert msg.table == "catalog.schema.table"

    def test_table_appears_in_summary(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert "catalog.schema.table" in msg.summary

    def test_run_id_stored_on_message(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-abc-123",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert msg.run_id == "run-abc-123"

    def test_run_time_stored_on_message(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert msg.run_time == _RUN_TIME

    def test_observed_metrics_equals_input(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert msg.observed_metrics == _METRICS

    def test_fields_has_entry_per_metric(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        for metric_name in _METRICS:
            assert metric_name in msg.fields

    def test_fields_metric_values_are_strings(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        for metric_name, metric_value in _METRICS.items():
            assert msg.fields[metric_name] == str(metric_value)

    def test_fields_contains_run_id(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert "run_id" in msg.fields
        assert msg.fields["run_id"] == "run-001"

    def test_fields_contains_run_time(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert "run_time" in msg.fields
        assert msg.fields["run_time"] == str(_RUN_TIME)

    def test_fields_contains_condition(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert "condition" in msg.fields
        assert msg.fields["condition"] == "error_row_count > 0"

    def test_fields_contains_table(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert "table" in msg.fields
        assert msg.fields["table"] == "catalog.schema.table"

    def test_severity_defaults_to_error(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert msg.severity == "error"

    def test_severity_can_be_overridden(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
            severity="warn",
        )
        assert msg.severity == "warn"

    def test_returns_alert_message_instance(self):
        msg = StandardMessageBuilder.build(
            action_name="notify_on_errors",
            condition="error_row_count > 0",
            metrics=_METRICS,
            run_id="run-001",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert isinstance(msg, AlertMessage)


# ---------------------------------------------------------------------------
# Null / None condition (fire-unconditionally case)
# ---------------------------------------------------------------------------


class TestStandardMessageBuilderNoneCondition:
    def test_none_condition_stored(self):
        msg = StandardMessageBuilder.build(
            action_name="always_alert",
            condition=None,
            metrics={"error_row_count": 3},
            run_id="run-002",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        assert msg.condition is None

    def test_none_condition_summary_does_not_leak_literal_none(self):
        msg = StandardMessageBuilder.build(
            action_name="always_alert",
            condition=None,
            metrics={"error_row_count": 3},
            run_id="run-002",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        # "None" must not appear literally in the summary
        assert "None" not in msg.summary

    def test_none_condition_summary_mentions_unconditional(self):
        msg = StandardMessageBuilder.build(
            action_name="always_alert",
            condition=None,
            metrics={"error_row_count": 3},
            run_id="run-002",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        # The summary should communicate that the action fires unconditionally
        assert "unconditional" in msg.summary.lower()

    def test_none_condition_field_not_none_string(self):
        msg = StandardMessageBuilder.build(
            action_name="always_alert",
            condition=None,
            metrics={"error_row_count": 3},
            run_id="run-002",
            run_time=_RUN_TIME,
            table="catalog.schema.table",
        )
        # The condition field in fields dict should not be the literal "None"
        assert msg.fields.get("condition") != "None"

    def test_none_table_is_handled_gracefully(self):
        msg = StandardMessageBuilder.build(
            action_name="always_alert",
            condition=None,
            metrics={"error_row_count": 3},
            run_id="run-002",
            run_time=_RUN_TIME,
            table=None,
        )
        assert msg.table is None
        assert "None" not in msg.summary

    def test_none_condition_fields_still_contain_metrics(self):
        metrics = {"error_row_count": 3}
        msg = StandardMessageBuilder.build(
            action_name="always_alert",
            condition=None,
            metrics=metrics,
            run_id="run-002",
            run_time=_RUN_TIME,
            table=None,
        )
        assert "error_row_count" in msg.fields

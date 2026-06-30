"""Unit tests for ActionSerializer (src/databricks/labs/dqx/actions/serializer.py)
and the *build_replace_where_predicate* predicate helper in definition_storage.py.

Tests are written in TDD order: they must FAIL before the implementation exists.
No Spark session, no workspace connection — pure Python unit tests.
"""

from __future__ import annotations

import logging
from typing import cast

import pytest

from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn
from databricks.labs.dqx.actions.base import (
    Action,
    ActionContext,
    ActionResult,
    ActionServices,
    ActionStatus,
)
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.definition_storage import build_replace_where_predicate
from databricks.labs.dqx.actions.destinations.callback import CallbackDQAlertDestination
from databricks.labs.dqx.actions.destinations.slack import SlackDQAlertDestination
from databricks.labs.dqx.actions.destinations.teams import TeamsDQAlertDestination
from databricks.labs.dqx.actions.destinations.webhook import WebhookDQAlertDestination
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.serializer import ActionSerializer
from databricks.labs.dqx.config import DQSecret
from databricks.labs.dqx.errors import InvalidActionError, UnsafeSqlQueryError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_slack_dq_action(condition: str | None = None, name: str = "") -> DQAction:
    """Build a DQAction wrapping a DQAlert with a Slack destination."""
    dest = SlackDQAlertDestination(name="slack-dest", webhook_url="https://hooks.slack.com/T/B/x")
    alert = DQAlert(
        destinations=[dest],
        name="my-alert",
        alert_frequency=DQAlertFrequency.HOURLY,
        notify_on=NotifyOn.STATUS_CHANGE,
        severity="warning",
    )
    return DQAction(action=alert, condition=condition, name=name)


def _as_dict(value: object) -> dict[str, object]:
    """Assert *value* is a dict[str, object] and return it."""
    assert isinstance(value, dict)
    return cast("dict[str, object]", value)


def _as_list(value: object) -> list[object]:
    """Assert *value* is a list and return it."""
    assert isinstance(value, list)
    return value


# ---------------------------------------------------------------------------
# DQAlert — Slack destination round-trip
# ---------------------------------------------------------------------------


class TestSlackRoundTrip:
    def test_to_dict_produces_expected_shape(self) -> None:
        action = _make_slack_dq_action()
        result = ActionSerializer.to_dict(action)

        action_data = _as_dict(result["action"])
        assert action_data["type"] == "alert"
        assert action_data["name"] == "my-alert"
        assert action_data["alert_frequency"] == "hourly"
        assert action_data["notify_on"] == "status_change"
        assert action_data["severity"] == "warning"
        dests = _as_list(action_data["destinations"])
        assert len(dests) == 1
        dest_data = _as_dict(dests[0])
        assert dest_data["type"] == "slack"
        assert dest_data["name"] == "slack-dest"
        assert dest_data["webhook_url"] == "https://hooks.slack.com/T/B/x"

    def test_condition_omitted_when_none(self) -> None:
        action = _make_slack_dq_action(condition=None)
        result = ActionSerializer.to_dict(action)
        assert "condition" not in result

    def test_condition_preserved_when_set(self) -> None:
        action = _make_slack_dq_action(condition="error_row_count > 0")
        result = ActionSerializer.to_dict(action)
        assert result["condition"] == "error_row_count > 0"

    def test_name_round_trips(self) -> None:
        action = _make_slack_dq_action(name="my-dq-action")
        result = ActionSerializer.to_dict(action)
        assert result["name"] == "my-dq-action"

    def test_from_dict_reconstructs_dq_action(self) -> None:
        action = _make_slack_dq_action(condition="error_row_count > 0", name="roundtrip")
        raw = ActionSerializer.to_dict(action)
        restored = ActionSerializer.from_dict(raw)

        assert restored.name == action.name
        assert restored.condition == action.condition
        assert isinstance(restored.action, DQAlert)
        if isinstance(restored.action, DQAlert):
            assert restored.action.name == "my-alert"
            assert restored.action.alert_frequency == DQAlertFrequency.HOURLY
            assert restored.action.notify_on == NotifyOn.STATUS_CHANGE
            assert restored.action.severity == "warning"
            assert len(restored.action.destinations) == 1
            dest = restored.action.destinations[0]
            assert isinstance(dest, SlackDQAlertDestination)
            assert dest.name == "slack-dest"
            assert dest.webhook_url == "https://hooks.slack.com/T/B/x"

    def test_from_dict_condition_absent_yields_none(self) -> None:
        raw = ActionSerializer.to_dict(_make_slack_dq_action())
        raw.pop("condition", None)
        restored = ActionSerializer.from_dict(raw)
        assert restored.condition is None


# ---------------------------------------------------------------------------
# Teams destination round-trip
# ---------------------------------------------------------------------------


class TestTeamsRoundTrip:
    def test_round_trip(self) -> None:
        dest = TeamsDQAlertDestination(
            name="teams-dest",
            webhook_url="https://webhook.office.com/some/path",
        )
        alert = DQAlert(destinations=[dest], name="teams-alert")
        action = DQAction(action=alert)

        raw = ActionSerializer.to_dict(action)
        action_data = _as_dict(raw["action"])
        dests = _as_list(action_data["destinations"])
        assert _as_dict(dests[0])["type"] == "teams"

        restored = ActionSerializer.from_dict(raw)
        assert isinstance(restored.action, DQAlert)
        if isinstance(restored.action, DQAlert):
            dest_restored = restored.action.destinations[0]
            assert isinstance(dest_restored, TeamsDQAlertDestination)
            assert dest_restored.webhook_url == "https://webhook.office.com/some/path"


# ---------------------------------------------------------------------------
# Generic Webhook destination round-trip (no auth)
# ---------------------------------------------------------------------------


class TestWebhookRoundTrip:
    def test_round_trip_no_auth(self) -> None:
        dest = WebhookDQAlertDestination(
            name="webhook-dest",
            webhook_url="https://example.com/hook",
        )
        alert = DQAlert(destinations=[dest], name="webhook-alert")
        action = DQAction(action=alert)

        raw = ActionSerializer.to_dict(action)
        action_data = _as_dict(raw["action"])
        dests = _as_list(action_data["destinations"])
        dest_data = _as_dict(dests[0])
        assert dest_data["type"] == "webhook"
        assert dest_data["webhook_url"] == "https://example.com/hook"
        assert dest_data.get("username") is None
        assert dest_data.get("password") is None

        restored = ActionSerializer.from_dict(raw)
        assert isinstance(restored.action, DQAlert)
        if isinstance(restored.action, DQAlert):
            dest_r = restored.action.destinations[0]
            assert isinstance(dest_r, WebhookDQAlertDestination)
            assert dest_r.webhook_url == "https://example.com/hook"
            assert dest_r.username is None
            assert dest_r.password is None


# ---------------------------------------------------------------------------
# DQSecret round-trips
# ---------------------------------------------------------------------------


class TestDQSecretSerialization:
    def test_webhook_url_as_dq_secret_round_trips(self) -> None:
        secret = DQSecret(scope="my-scope", key="my-key")
        dest = SlackDQAlertDestination(name="slack-dest", webhook_url=secret)
        alert = DQAlert(destinations=[dest], name="secret-alert")
        action = DQAction(action=alert)

        raw = ActionSerializer.to_dict(action)
        action_data = _as_dict(raw["action"])
        dests = _as_list(action_data["destinations"])
        dest_data = _as_dict(dests[0])
        # DQSecret must be serialised as tagged dict {"secret": "scope/key"}
        assert dest_data["webhook_url"] == {"secret": "my-scope/my-key"}

        restored = ActionSerializer.from_dict(raw)
        assert isinstance(restored.action, DQAlert)
        if isinstance(restored.action, DQAlert):
            dest_r = restored.action.destinations[0]
            assert isinstance(dest_r, SlackDQAlertDestination)
            assert isinstance(dest_r.webhook_url, DQSecret)
            assert dest_r.webhook_url.scope == "my-scope"
            assert dest_r.webhook_url.key == "my-key"

    def test_plain_string_webhook_url_stays_string(self) -> None:
        dest = SlackDQAlertDestination(name="sd", webhook_url="https://hooks.slack.com/T/B/x")
        alert = DQAlert(destinations=[dest], name="plain-alert")
        action = DQAction(action=alert)

        raw = ActionSerializer.to_dict(action)
        action_data = _as_dict(raw["action"])
        dests = _as_list(action_data["destinations"])
        assert isinstance(_as_dict(dests[0])["webhook_url"], str)

        restored = ActionSerializer.from_dict(raw)
        assert isinstance(restored.action, DQAlert)
        if isinstance(restored.action, DQAlert):
            dest_r = restored.action.destinations[0]
            assert isinstance(dest_r, SlackDQAlertDestination)
            assert isinstance(dest_r.webhook_url, str)

    def test_webhook_dest_username_password_as_dq_secret(self) -> None:
        u_secret = DQSecret(scope="s1", key="user")
        p_secret = DQSecret(scope="s2", key="pass")
        dest = WebhookDQAlertDestination(
            name="wh",
            webhook_url="https://example.com/hook",
            username=u_secret,
            password=p_secret,
        )
        alert = DQAlert(destinations=[dest], name="auth-alert")
        action = DQAction(action=alert)

        raw = ActionSerializer.to_dict(action)
        action_data = _as_dict(raw["action"])
        dests = _as_list(action_data["destinations"])
        dest_data = _as_dict(dests[0])
        assert dest_data["username"] == {"secret": "s1/user"}
        assert dest_data["password"] == {"secret": "s2/pass"}

        restored = ActionSerializer.from_dict(raw)
        assert isinstance(restored.action, DQAlert)
        if isinstance(restored.action, DQAlert):
            dest_r = restored.action.destinations[0]
            assert isinstance(dest_r, WebhookDQAlertDestination)
            assert isinstance(dest_r.username, DQSecret)
            assert dest_r.username.scope == "s1"
            assert dest_r.username.key == "user"
            assert isinstance(dest_r.password, DQSecret)
            assert dest_r.password.scope == "s2"
            assert dest_r.password.key == "pass"


# ---------------------------------------------------------------------------
# FailPipeline round-trip
# ---------------------------------------------------------------------------


class TestFailPipelineRoundTrip:
    def test_round_trip_with_message(self) -> None:
        fail_action = FailPipeline(message="custom failure", name="fail_pipeline")
        action = DQAction(action=fail_action)

        raw = ActionSerializer.to_dict(action)
        action_data = _as_dict(raw["action"])
        assert action_data["type"] == "fail_pipeline"
        assert action_data["message"] == "custom failure"

        restored = ActionSerializer.from_dict(raw)
        assert isinstance(restored.action, FailPipeline)
        if isinstance(restored.action, FailPipeline):
            assert restored.action.message == "custom failure"

    def test_round_trip_without_message(self) -> None:
        fail_action = FailPipeline()
        action = DQAction(action=fail_action)

        raw = ActionSerializer.to_dict(action)
        action_data = _as_dict(raw["action"])
        assert action_data.get("message") is None

        restored = ActionSerializer.from_dict(raw)
        assert isinstance(restored.action, FailPipeline)
        if isinstance(restored.action, FailPipeline):
            assert restored.action.message is None

    def test_fail_pipeline_with_condition(self) -> None:
        fail_action = FailPipeline()
        action = DQAction(action=fail_action, condition="error_row_count > 100")

        raw = ActionSerializer.to_dict(action)
        assert raw["condition"] == "error_row_count > 100"

        restored = ActionSerializer.from_dict(raw)
        assert restored.condition == "error_row_count > 100"
        assert isinstance(restored.action, FailPipeline)


# ---------------------------------------------------------------------------
# Enum serialization
# ---------------------------------------------------------------------------


class TestEnumSerialization:
    def test_alert_frequency_enum_round_trips(self) -> None:
        for freq in DQAlertFrequency:
            dest = SlackDQAlertDestination(name="sd", webhook_url="https://hooks.slack.com/T/B/x")
            alert = DQAlert(destinations=[dest], alert_frequency=freq)
            action = DQAction(action=alert)
            raw = ActionSerializer.to_dict(action)
            action_data = _as_dict(raw["action"])
            assert action_data["alert_frequency"] == freq.value
            restored = ActionSerializer.from_dict(raw)
            assert isinstance(restored.action, DQAlert)
            if isinstance(restored.action, DQAlert):
                assert restored.action.alert_frequency == freq

    def test_notify_on_enum_round_trips(self) -> None:
        for notify in NotifyOn:
            dest = SlackDQAlertDestination(name="sd", webhook_url="https://hooks.slack.com/T/B/x")
            alert = DQAlert(destinations=[dest], notify_on=notify)
            action = DQAction(action=alert)
            raw = ActionSerializer.to_dict(action)
            action_data = _as_dict(raw["action"])
            assert action_data["notify_on"] == notify.value
            restored = ActionSerializer.from_dict(raw)
            assert isinstance(restored.action, DQAlert)
            if isinstance(restored.action, DQAlert):
                assert restored.action.notify_on == notify

    def test_severity_round_trips(self) -> None:
        dest = SlackDQAlertDestination(name="sd", webhook_url="https://hooks.slack.com/T/B/x")
        alert = DQAlert(destinations=[dest], severity="critical")
        action = DQAction(action=alert)
        raw = ActionSerializer.to_dict(action)
        action_data = _as_dict(raw["action"])
        assert action_data["severity"] == "critical"
        restored = ActionSerializer.from_dict(raw)
        assert isinstance(restored.action, DQAlert)
        if isinstance(restored.action, DQAlert):
            assert restored.action.severity == "critical"


# ---------------------------------------------------------------------------
# Callback destination skipped with warning
# ---------------------------------------------------------------------------


class TestCallbackDestinationHandling:
    def test_callback_skipped_and_warning_emitted(self, caplog: pytest.LogCaptureFixture) -> None:
        callback_dest = CallbackDQAlertDestination(
            name="cb-dest",
            callback=lambda msg, ctx: None,
        )
        slack_dest = SlackDQAlertDestination(name="slack-dest", webhook_url="https://hooks.slack.com/T/B/x")
        alert = DQAlert(destinations=[callback_dest, slack_dest], name="mixed-alert")
        action = DQAction(action=alert)

        with caplog.at_level(logging.WARNING):
            raw = ActionSerializer.to_dict(action)

        # Callback destination must not appear in serialized output
        action_data = _as_dict(raw["action"])
        dests = _as_list(action_data["destinations"])
        dest_types = [_as_dict(dest)["type"] for dest in dests]
        assert "callback" not in dest_types
        assert "slack" in dest_types

        # Warning must have been logged
        assert any("cb-dest" in record.message for record in caplog.records)

    def test_all_callback_destinations_produces_empty_destinations_list(self, caplog: pytest.LogCaptureFixture) -> None:
        callback_dest = CallbackDQAlertDestination(
            name="only-callback",
            callback=lambda msg, ctx: None,
        )
        alert = DQAlert(destinations=[callback_dest], name="all-callback-alert")
        action = DQAction(action=alert)

        with caplog.at_level(logging.WARNING):
            raw = ActionSerializer.to_dict(action)

        action_data = _as_dict(raw["action"])
        assert action_data["destinations"] == []
        assert any("only-callback" in record.message for record in caplog.records)


# ---------------------------------------------------------------------------
# Unknown type raises InvalidActionError
# ---------------------------------------------------------------------------


class TestUnknownTypeErrors:
    def test_unknown_action_type_raises(self) -> None:
        raw: dict[str, object] = {
            "action": {"type": "nonexistent_action_type"},
            "name": "bad",
        }
        with pytest.raises(InvalidActionError):
            ActionSerializer.from_dict(raw)

    def test_unknown_destination_type_raises(self) -> None:
        raw: dict[str, object] = {
            "action": {
                "type": "alert",
                "name": "a",
                "destinations": [{"type": "no_such_destination", "name": "x", "webhook_url": "https://x.com"}],
            },
            "name": "bad",
        }
        with pytest.raises(InvalidActionError):
            ActionSerializer.from_dict(raw)


# ---------------------------------------------------------------------------
# DQAction.name field
# ---------------------------------------------------------------------------


class TestDQActionNameField:
    def test_explicit_name_preserved(self) -> None:
        dest = SlackDQAlertDestination(name="sd", webhook_url="https://hooks.slack.com/T/B/x")
        alert = DQAlert(destinations=[dest], name="alert")
        action = DQAction(action=alert, name="my-explicit-name")
        raw = ActionSerializer.to_dict(action)
        assert raw["name"] == "my-explicit-name"
        restored = ActionSerializer.from_dict(raw)
        assert restored.name == "my-explicit-name"

    def test_derived_name_preserved(self) -> None:
        dest = SlackDQAlertDestination(name="sd", webhook_url="https://hooks.slack.com/T/B/x")
        alert = DQAlert(destinations=[dest], name="alert")
        action = DQAction(action=alert)
        # name should be auto-derived from action.name
        raw = ActionSerializer.to_dict(action)
        assert "name" in raw
        restored = ActionSerializer.from_dict(raw)
        assert restored.name == action.name


# ---------------------------------------------------------------------------
# build_replace_where_predicate — SQL injection guard (CWE-89)
# ---------------------------------------------------------------------------


class TestBuildReplaceWhere:
    """Tests for the *build_replace_where_predicate* predicate helper.

    These tests exercise the pure function in isolation — no Spark session is
    needed, which makes them fast and deterministic.
    """

    def test_safe_name_produces_correct_predicate(self) -> None:
        predicate = build_replace_where_predicate("default")
        assert predicate == "run_config_name = 'default'"

    def test_name_with_dots_and_dashes_is_accepted(self) -> None:
        predicate = build_replace_where_predicate("my-run.config_v1")
        assert predicate == "run_config_name = 'my-run.config_v1'"

    def test_single_quote_injection_raises(self) -> None:
        """A value with a single quote must be rejected — not silently included."""
        with pytest.raises(UnsafeSqlQueryError):
            build_replace_where_predicate("a' OR '1'='1")

    def test_semicolon_injection_raises(self) -> None:
        with pytest.raises(UnsafeSqlQueryError):
            build_replace_where_predicate("default; DROP TABLE actions;--")

    def test_space_in_name_raises(self) -> None:
        with pytest.raises(UnsafeSqlQueryError):
            build_replace_where_predicate("has space")

    def test_newline_injection_raises(self) -> None:
        with pytest.raises(UnsafeSqlQueryError):
            build_replace_where_predicate("line1\nline2")

    def test_empty_string_raises(self) -> None:
        """An empty run_config_name does not match the safe-chars pattern."""
        with pytest.raises(UnsafeSqlQueryError):
            build_replace_where_predicate("")


# ---------------------------------------------------------------------------
# Unknown action type — rejected at DQAction construction
# ---------------------------------------------------------------------------


class TestUnknownActionTypeRejected:
    """An *Action* subclass outside the *AnyAction* union is rejected when wrapped."""

    def test_unknown_action_type_raises_on_construction(self) -> None:
        """A concrete Action subclass not in the discriminated union must raise InvalidActionError."""

        class _UnknownAction(Action):
            name: str = "unknown_test_action"

            def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
                return ActionResult(action_name=self.name, fired=False, status=ActionStatus.HEALTHY)

        with pytest.raises(InvalidActionError):
            DQAction(action=_UnknownAction())

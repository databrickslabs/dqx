"""Unit tests for webhook alert destinations (Task 7).

Tests cover:
- AlertDestination and WebhookAlertDestination are abstract (inspect.isabstract)
- Each destination delivers to resolved URL with correct allowed_host_suffixes
- Slack payload has top-level 'blocks' key
- Teams payload has '@type': 'MessageCard' and 'sections' key
- Generic webhook payload contains title, condition, observed_metrics keys
- DQSecret webhook_url is resolved through the resolver before posting
- Generic webhook username+password (including DQSecret) passes WebhookAuth; without creds passes auth=None
- validate() raises InvalidActionError for empty name / missing/empty url
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.delivery import WebhookAuth, WebhookClient
from databricks.labs.dqx.actions.destinations import (
    AlertDestination,
    SlackDQAlertDestination,
    TeamsDQAlertDestination,
    WebhookAlertDestination,
    WebhookDQAlertDestination,
)
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.actions.secrets import SecretResolver
from databricks.labs.dqx.config import DQSecret
from databricks.labs.dqx.errors import InvalidActionError


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


@dataclass
class _Call:
    """Records a single WebhookClient.post() invocation."""

    url: str
    payload: dict
    auth: WebhookAuth | None
    allowed_host_suffixes: list[str] | None


class FakeWebhookClient:
    """Records calls to post() without performing any HTTP I/O."""

    def __init__(self) -> None:
        self.calls: list[_Call] = []

    def post(
        self,
        url: str,
        payload: dict,
        *,
        auth: WebhookAuth | None = None,
        allowed_host_suffixes: list[str] | None = None,
    ) -> None:
        self.calls.append(_Call(url=url, payload=payload, auth=auth, allowed_host_suffixes=allowed_host_suffixes))


class FakeSecretResolver:
    """Returns 'resolved_<scope>_<key>' for DQSecret; passes through plain strings."""

    def resolve(self, value: str | DQSecret) -> str:
        if isinstance(value, DQSecret):
            return f"resolved_{value.scope}_{value.key}"
        return value


def _make_message(
    *,
    title: str = "DQX alert: test",
    summary: str = "Test summary",
    condition: str | None = "error_row_count > 0",
    table: str | None = "catalog.schema.table",
    observed_metrics: dict[str, object] | None = None,
    run_id: str = "run-abc",
    run_time: datetime | None = None,
    severity: str = "error",
    fields: dict[str, str] | None = None,
) -> AlertMessage:
    if observed_metrics is None:
        observed_metrics = {"error_row_count": 5}
    if run_time is None:
        run_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    if fields is None:
        fields = {"condition": condition or "unconditional", "run_id": run_id}
    return AlertMessage(
        title=title,
        summary=summary,
        condition=condition,
        table=table,
        observed_metrics=observed_metrics,
        run_id=run_id,
        run_time=run_time,
        severity=severity,
        fields=fields,
    )


def _make_services(
    secret_resolver: FakeSecretResolver | None = None,
    webhook_client: FakeWebhookClient | None = None,
) -> ActionServices:
    resolver = secret_resolver or FakeSecretResolver()
    fake_client = webhook_client or FakeWebhookClient()
    # Wrap FakeSecretResolver in a create_autospec so ActionServices receives a
    # proper SecretResolver-typed mock; the fake's resolve() drives the side effect.
    spec_resolver = create_autospec(SecretResolver, instance=True)
    spec_resolver.resolve.side_effect = resolver.resolve
    # Wrap FakeWebhookClient similarly so ActionServices receives a WebhookClient-
    # typed mock, satisfying mypy.  The fake's post() drives the side effect and
    # records every call on fake_client.calls.
    spec_client = create_autospec(WebhookClient, instance=True)
    spec_client.post.side_effect = fake_client.post
    return ActionServices(
        secret_resolver=spec_resolver,
        webhook_client=spec_client,
    )


# ---------------------------------------------------------------------------
# Abstractness tests
# ---------------------------------------------------------------------------


def test_alert_destination_is_abstract():
    assert inspect.isabstract(AlertDestination), "AlertDestination must be abstract"


def test_webhook_alert_destination_is_abstract():
    assert inspect.isabstract(WebhookAlertDestination), "WebhookAlertDestination must be abstract"


# ---------------------------------------------------------------------------
# Slack destination tests
# ---------------------------------------------------------------------------


def test_slack_delivers_to_resolved_url():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = SlackDQAlertDestination(name="slack_test", webhook_url="https://hooks.slack.com/services/T/B/x")
    dest.deliver(_make_message(), context, services)

    assert len(client.calls) == 1
    assert client.calls[0].url == "https://hooks.slack.com/services/T/B/x"


def test_slack_allowed_host_suffixes():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = SlackDQAlertDestination(name="slack_test", webhook_url="https://hooks.slack.com/services/T/B/x")
    dest.deliver(_make_message(), context, services)

    assert client.calls[0].allowed_host_suffixes == ["hooks.slack.com"]


def test_slack_payload_has_blocks():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = SlackDQAlertDestination(name="slack_test", webhook_url="https://hooks.slack.com/services/T/B/x")
    dest.deliver(_make_message(), context, services)

    payload = client.calls[0].payload
    assert "blocks" in payload, "Slack payload must contain 'blocks' key"
    assert isinstance(payload["blocks"], list), "'blocks' must be a list"
    assert len(payload["blocks"]) > 0, "'blocks' list must not be empty"


def test_slack_blocks_are_valid_block_kit():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = SlackDQAlertDestination(name="slack_test", webhook_url="https://hooks.slack.com/services/T/B/x")
    dest.deliver(_make_message(), context, services)

    for block in client.calls[0].payload["blocks"]:
        assert "type" in block, f"Block {block!r} must have 'type'"


def test_slack_no_auth_passed():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = SlackDQAlertDestination(name="slack_test", webhook_url="https://hooks.slack.com/services/T/B/x")
    dest.deliver(_make_message(), context, services)

    assert client.calls[0].auth is None


def test_slack_type_classvar():
    assert SlackDQAlertDestination.type == "slack"


# ---------------------------------------------------------------------------
# Teams destination tests
# ---------------------------------------------------------------------------


def test_teams_delivers_to_resolved_url():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = TeamsDQAlertDestination(name="teams_test", webhook_url="https://my.webhook.office.com/webhookb2/x")
    dest.deliver(_make_message(), context, services)

    assert client.calls[0].url == "https://my.webhook.office.com/webhookb2/x"


def test_teams_allowed_host_suffixes():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = TeamsDQAlertDestination(name="teams_test", webhook_url="https://my.webhook.office.com/webhookb2/x")
    dest.deliver(_make_message(), context, services)

    suffixes = client.calls[0].allowed_host_suffixes
    assert suffixes is not None
    assert "webhook.office.com" in suffixes
    assert "office.com" in suffixes


def test_teams_payload_has_message_card_type():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = TeamsDQAlertDestination(name="teams_test", webhook_url="https://my.webhook.office.com/webhookb2/x")
    dest.deliver(_make_message(), context, services)

    payload = client.calls[0].payload
    assert payload.get("@type") == "MessageCard"


def test_teams_payload_has_sections():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = TeamsDQAlertDestination(name="teams_test", webhook_url="https://my.webhook.office.com/webhookb2/x")
    dest.deliver(_make_message(), context, services)

    payload = client.calls[0].payload
    assert "sections" in payload
    assert isinstance(payload["sections"], list)


def test_teams_payload_context_field():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = TeamsDQAlertDestination(name="teams_test", webhook_url="https://my.webhook.office.com/webhookb2/x")
    dest.deliver(_make_message(), context, services)

    payload = client.calls[0].payload
    assert payload.get("@context") == "http://schema.org/extensions"


def test_teams_type_classvar():
    assert TeamsDQAlertDestination.type == "teams"


# ---------------------------------------------------------------------------
# Generic webhook destination tests
# ---------------------------------------------------------------------------


def test_webhook_delivers_to_resolved_url():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = WebhookDQAlertDestination(name="wh_test", webhook_url="https://example.com/hook")
    dest.deliver(_make_message(), context, services)

    assert client.calls[0].url == "https://example.com/hook"


def test_webhook_allowed_host_suffixes_none():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = WebhookDQAlertDestination(name="wh_test", webhook_url="https://example.com/hook")
    dest.deliver(_make_message(), context, services)

    assert client.calls[0].allowed_host_suffixes is None


def test_webhook_payload_contains_required_keys():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = WebhookDQAlertDestination(name="wh_test", webhook_url="https://example.com/hook")
    dest.deliver(_make_message(), context, services)

    payload = client.calls[0].payload
    assert "title" in payload
    assert "condition" in payload
    assert "observed_metrics" in payload


def test_webhook_payload_run_time_is_isoformat():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))
    run_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    msg = _make_message(run_time=run_time)

    dest = WebhookDQAlertDestination(name="wh_test", webhook_url="https://example.com/hook")
    dest.deliver(msg, context, services)

    payload = client.calls[0].payload
    assert payload["run_time"] == run_time.isoformat()


def test_webhook_type_classvar():
    assert WebhookDQAlertDestination.type == "webhook"


# ---------------------------------------------------------------------------
# DQSecret URL resolution
# ---------------------------------------------------------------------------


def test_dqsecret_url_is_resolved_before_posting():
    client = FakeWebhookClient()
    fake_resolver = FakeSecretResolver()
    services = _make_services(secret_resolver=fake_resolver, webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    secret_url = DQSecret(scope="myscope", key="mykey")
    dest = WebhookDQAlertDestination(name="wh_test", webhook_url=secret_url)
    dest.deliver(_make_message(), context, services)

    # The resolver should have been called and the resolved URL used.
    assert client.calls[0].url == "resolved_myscope_mykey"


def test_slack_dqsecret_url_resolved():
    client = FakeWebhookClient()
    fake_resolver = FakeSecretResolver()
    services = _make_services(secret_resolver=fake_resolver, webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    secret_url = DQSecret(scope="slack_scope", key="slack_key")
    dest = SlackDQAlertDestination(name="slack_secret", webhook_url=secret_url)
    dest.deliver(_make_message(), context, services)

    assert client.calls[0].url == "resolved_slack_scope_slack_key"


# ---------------------------------------------------------------------------
# Webhook auth tests
# ---------------------------------------------------------------------------


def test_webhook_no_credentials_passes_auth_none():
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = WebhookDQAlertDestination(name="wh_test", webhook_url="https://example.com/hook")
    dest.deliver(_make_message(), context, services)

    assert client.calls[0].auth is None


def test_webhook_with_plain_credentials_passes_webhook_auth():
    client = FakeWebhookClient()
    fake_resolver = FakeSecretResolver()
    services = _make_services(secret_resolver=fake_resolver, webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = WebhookDQAlertDestination(
        name="wh_test",
        webhook_url="https://example.com/hook",
        username="user1",
        password="pass1",
    )
    dest.deliver(_make_message(), context, services)

    auth = client.calls[0].auth
    assert isinstance(auth, WebhookAuth)
    assert auth.username == "user1"
    assert auth.password == "pass1"


def test_webhook_with_dqsecret_credentials_resolves_and_passes_webhook_auth():
    client = FakeWebhookClient()
    fake_resolver = FakeSecretResolver()
    services = _make_services(secret_resolver=fake_resolver, webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = WebhookDQAlertDestination(
        name="wh_test",
        webhook_url="https://example.com/hook",
        username=DQSecret(scope="creds", key="username"),
        password=DQSecret(scope="creds", key="password"),
    )
    dest.deliver(_make_message(), context, services)

    auth = client.calls[0].auth
    assert isinstance(auth, WebhookAuth)
    assert auth.username == "resolved_creds_username"
    assert auth.password == "resolved_creds_password"


def test_webhook_only_username_no_auth():
    """Only username set (no password) → auth=None."""
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = WebhookDQAlertDestination(
        name="wh_test",
        webhook_url="https://example.com/hook",
        username="user1",
    )
    dest.deliver(_make_message(), context, services)

    assert client.calls[0].auth is None


def test_webhook_only_password_no_auth():
    """Only password set (no username) → auth=None."""
    client = FakeWebhookClient()
    services = _make_services(webhook_client=client)
    context = ActionContext(metrics={}, run_id="r1", run_time=datetime.now(timezone.utc))

    dest = WebhookDQAlertDestination(
        name="wh_test",
        webhook_url="https://example.com/hook",
        password="pass1",
    )
    dest.deliver(_make_message(), context, services)

    assert client.calls[0].auth is None


# ---------------------------------------------------------------------------
# validate() tests
# ---------------------------------------------------------------------------


def test_slack_validate_raises_for_empty_name():
    dest = SlackDQAlertDestination(name="", webhook_url="https://hooks.slack.com/services/T/B/x")
    with pytest.raises(InvalidActionError):
        dest.validate()


def test_slack_validate_raises_for_empty_url():
    dest = SlackDQAlertDestination(name="slack_test", webhook_url="")
    with pytest.raises(InvalidActionError):
        dest.validate()


def test_teams_validate_raises_for_empty_name():
    dest = TeamsDQAlertDestination(name="", webhook_url="https://my.webhook.office.com/hook")
    with pytest.raises(InvalidActionError):
        dest.validate()


def test_teams_validate_raises_for_empty_url():
    dest = TeamsDQAlertDestination(name="teams_test", webhook_url="")
    with pytest.raises(InvalidActionError):
        dest.validate()


def test_webhook_validate_raises_for_empty_name():
    dest = WebhookDQAlertDestination(name="", webhook_url="https://example.com/hook")
    with pytest.raises(InvalidActionError):
        dest.validate()


def test_webhook_validate_raises_for_empty_url_string():
    dest = WebhookDQAlertDestination(name="wh_test", webhook_url="")
    with pytest.raises(InvalidActionError):
        dest.validate()


def test_webhook_validate_passes_for_dqsecret_url():
    """A DQSecret webhook_url should be accepted by validate()."""
    dest = WebhookDQAlertDestination(
        name="wh_test",
        webhook_url=DQSecret(scope="myscope", key="mykey"),
    )
    dest.validate()  # must not raise

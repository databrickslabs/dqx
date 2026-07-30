"""Unit tests for action-related config dataclasses and errors."""

import pytest

from databricks.labs.dqx.config import (
    ActionEventsConfig,
    DQSecret,
    LakebaseActionsStorageConfig,
    RunConfig,
    TableActionsStorageConfig,
)
from databricks.labs.dqx.errors import (
    AlertDeliveryError,
    DQXError,
    InvalidActionError,
    InvalidConditionError,
    InvalidConfigError,
    InvalidParameterError,
    PipelineFailedError,
    TerminalActionError,
    UnsafeWebhookUrlError,
)


# ---------------------------------------------------------------------------
# DQSecret
# ---------------------------------------------------------------------------


class TestDQSecret:
    def test_as_reference(self):
        secret = DQSecret(scope="myscope", key="mykey")
        assert secret.as_reference() == "myscope/mykey"

    def test_from_reference_round_trips(self):
        ref = "myscope/mykey"
        secret = DQSecret.from_reference(ref)
        assert secret.scope == "myscope"
        assert secret.key == "mykey"
        assert secret.as_reference() == ref

    def test_from_reference_with_slash_in_key(self):
        # Only the FIRST "/" splits scope/key; extra slashes belong to the key
        secret = DQSecret.from_reference("scope/key/extra")
        assert secret.scope == "scope"
        assert secret.key == "key/extra"

    def test_from_reference_raises_for_no_slash(self):
        with pytest.raises(InvalidParameterError):
            DQSecret.from_reference("noslash")

    def test_from_reference_raises_for_empty_scope(self):
        with pytest.raises(InvalidParameterError):
            DQSecret.from_reference("/key")

    def test_from_reference_raises_for_empty_key(self):
        with pytest.raises(InvalidParameterError):
            DQSecret.from_reference("scope/")

    def test_from_reference_strips_whitespace(self):
        # Leading/trailing whitespace around scope/key is trimmed so a stray space in config does
        # not produce a scope/key that fails at secret lookup with a confusing error.
        secret = DQSecret.from_reference("  myscope / mykey  ")
        assert secret.scope == "myscope"
        assert secret.key == "mykey"

    def test_from_reference_raises_for_whitespace_only_part(self):
        # A part that is only whitespace is empty after stripping and must still be rejected.
        with pytest.raises(InvalidParameterError):
            DQSecret.from_reference("scope/   ")

    def test_frozen_dataclass(self):
        secret = DQSecret(scope="s", key="k")
        with pytest.raises((AttributeError, TypeError)):
            setattr(secret, "scope", "other")


# ---------------------------------------------------------------------------
# TableActionsStorageConfig
# ---------------------------------------------------------------------------


class TestTableActionsStorageConfig:
    def test_valid_config(self):
        cfg = TableActionsStorageConfig(location="catalog.schema.table")
        assert cfg.location == "catalog.schema.table"
        assert cfg.run_config_name == "default"
        assert cfg.mode == "append"

    def test_empty_location_raises(self):
        with pytest.raises(InvalidConfigError):
            TableActionsStorageConfig(location="")

    def test_custom_mode_and_run_config(self):
        cfg = TableActionsStorageConfig(location="a.b.c", run_config_name="prod", mode="overwrite")
        assert cfg.run_config_name == "prod"
        assert cfg.mode == "overwrite"

    def test_invalid_mode_raises(self):
        with pytest.raises(InvalidConfigError, match="Invalid mode"):
            TableActionsStorageConfig(location="a.b.c", mode="upsert")


# ---------------------------------------------------------------------------
# LakebaseActionsStorageConfig
# ---------------------------------------------------------------------------


class TestLakebaseActionsStorageConfig:
    def test_valid_config(self):
        cfg = LakebaseActionsStorageConfig(location="db.schema.tbl", instance_name="my-instance")
        assert cfg.database_name == "db"
        assert cfg.schema_name == "schema"
        assert cfg.table_name == "tbl"

    def test_non_three_part_location_raises(self):
        with pytest.raises(InvalidConfigError):
            LakebaseActionsStorageConfig(location="db.schema", instance_name="inst")

    def test_empty_instance_name_raises(self):
        with pytest.raises(InvalidParameterError):
            LakebaseActionsStorageConfig(location="db.schema.tbl", instance_name="")

    def test_none_instance_name_raises(self):
        # instance_name is a required str field; passing None satisfies no
        # static type but Python dataclasses do not enforce types at runtime —
        # The model validator catches the None value and raises.
        with pytest.raises(InvalidParameterError):
            LakebaseActionsStorageConfig(location="db.schema.tbl", instance_name=None)

    def test_empty_location_raises(self):
        with pytest.raises(InvalidParameterError):
            LakebaseActionsStorageConfig(location="", instance_name="inst")

    def test_defaults(self):
        cfg = LakebaseActionsStorageConfig(location="db.sc.tbl", instance_name="inst")
        assert cfg.port == "5432"
        assert cfg.run_config_name == "default"
        assert cfg.mode == "append"
        assert cfg.client_id is None


# ---------------------------------------------------------------------------
# ActionEventsConfig
# ---------------------------------------------------------------------------


class TestActionEventsConfig:
    def test_valid_config(self):
        cfg = ActionEventsConfig(location="catalog.schema.events")
        assert cfg.location == "catalog.schema.events"
        assert cfg.mode == "append"

    def test_empty_location_raises(self):
        with pytest.raises(InvalidConfigError):
            ActionEventsConfig(location="")

    def test_invalid_mode_raises(self):
        with pytest.raises(InvalidConfigError, match="Invalid mode"):
            ActionEventsConfig(location="catalog.schema.events", mode="upsert")

    def test_file_location_raises(self):
        # Action events must go to a UC/Lakebase table, never a file path.
        with pytest.raises(InvalidConfigError, match="must be a Unity Catalog table"):
            ActionEventsConfig(location="/Volumes/cat/sch/vol/events.json")

    def test_three_part_file_extension_location_raises(self):
        # A 3-part name that is actually a serializer file (e.g. .yml/.json) must be rejected as a
        # file, not accepted as a table (regression guard for the table-vs-file check).
        with pytest.raises(InvalidConfigError, match="must be a Unity Catalog table"):
            ActionEventsConfig(location="my.checks.yml")
        with pytest.raises(InvalidConfigError, match="must be a Unity Catalog table"):
            ActionEventsConfig(location="cat.schema.events.json")

    def test_leading_digit_location_is_accepted(self):
        # Databricks allows leading digits in unquoted identifiers (verified against a live
        # workspace: '1catalog.schema.events' resolves as a table name, not a parse error).
        cfg = ActionEventsConfig(location="1catalog.schema.events")
        assert cfg.location == "1catalog.schema.events"

    def test_backtick_quoted_dotted_name_is_accepted(self):
        # A backtick-quoted part containing a dot is a single valid identifier, not four parts.
        cfg = ActionEventsConfig(location="cat.schema.`weird.name`")
        assert cfg.location == "cat.schema.`weird.name`"

    def test_two_part_table_location_is_accepted(self):
        # A two-part schema.table name (default catalog) is a valid UC table location.
        cfg = ActionEventsConfig(location="schema.events")
        assert cfg.location == "schema.events"

    def test_single_part_location_raises(self):
        # A bare single-part name is not a table location.
        with pytest.raises(InvalidConfigError, match="must be a Unity Catalog table"):
            ActionEventsConfig(location="events")


# ---------------------------------------------------------------------------
# RunConfig.actions_location
# ---------------------------------------------------------------------------


class TestRunConfigActionsLocation:
    def test_actions_location_defaults_to_none(self):
        cfg = RunConfig()
        assert cfg.actions_location is None

    def test_actions_location_can_be_set(self):
        cfg = RunConfig(actions_location="catalog.schema.actions")
        assert cfg.actions_location == "catalog.schema.actions"


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class TestExceptions:
    def test_terminal_action_error_is_dqx_error(self):
        assert issubclass(TerminalActionError, DQXError)

    def test_pipeline_failed_error_is_terminal(self):
        assert issubclass(PipelineFailedError, TerminalActionError)

    def test_invalid_condition_error_is_dqx_error(self):
        assert issubclass(InvalidConditionError, DQXError)

    def test_invalid_action_error_is_dqx_error(self):
        assert issubclass(InvalidActionError, DQXError)

    def test_alert_delivery_error_is_dqx_error(self):
        assert issubclass(AlertDeliveryError, DQXError)

    def test_unsafe_webhook_url_error_is_dqx_error(self):
        assert issubclass(UnsafeWebhookUrlError, DQXError)

    def test_exceptions_are_raisable(self):
        with pytest.raises(TerminalActionError):
            raise TerminalActionError("test")

        with pytest.raises(PipelineFailedError):
            raise PipelineFailedError("test")

        with pytest.raises(InvalidConditionError):
            raise InvalidConditionError("test")

        with pytest.raises(InvalidActionError):
            raise InvalidActionError("test")

        with pytest.raises(AlertDeliveryError):
            raise AlertDeliveryError("test")

        with pytest.raises(UnsafeWebhookUrlError):
            raise UnsafeWebhookUrlError("test")

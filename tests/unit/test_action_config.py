"""Unit tests for action-related config dataclasses and errors (Task 1)."""

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
    InvalidActionError,
    InvalidConditionError,
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

    def test_frozen_dataclass(self):
        secret = DQSecret(scope="s", key="k")
        with pytest.raises((AttributeError, TypeError)):
            secret.scope = "other"  # type: ignore[misc]


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
        from databricks.labs.dqx.errors import InvalidConfigError

        with pytest.raises(InvalidConfigError):
            TableActionsStorageConfig(location="")

    def test_custom_mode_and_run_config(self):
        cfg = TableActionsStorageConfig(location="a.b.c", run_config_name="prod", mode="overwrite")
        assert cfg.run_config_name == "prod"
        assert cfg.mode == "overwrite"


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
        from databricks.labs.dqx.errors import InvalidConfigError

        with pytest.raises(InvalidConfigError):
            LakebaseActionsStorageConfig(location="db.schema", instance_name="inst")

    def test_empty_instance_name_raises(self):
        with pytest.raises(InvalidParameterError):
            LakebaseActionsStorageConfig(location="db.schema.tbl", instance_name="")

    def test_none_instance_name_raises(self):
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
        from databricks.labs.dqx.errors import InvalidConfigError

        with pytest.raises(InvalidConfigError):
            ActionEventsConfig(location="")


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
        from databricks.labs.dqx.errors import DQXError

        assert issubclass(TerminalActionError, DQXError)

    def test_pipeline_failed_error_is_terminal(self):
        assert issubclass(PipelineFailedError, TerminalActionError)

    def test_invalid_condition_error_is_dqx_error(self):
        from databricks.labs.dqx.errors import DQXError

        assert issubclass(InvalidConditionError, DQXError)

    def test_invalid_action_error_is_dqx_error(self):
        from databricks.labs.dqx.errors import DQXError

        assert issubclass(InvalidActionError, DQXError)

    def test_alert_delivery_error_is_dqx_error(self):
        from databricks.labs.dqx.errors import DQXError

        assert issubclass(AlertDeliveryError, DQXError)

    def test_unsafe_webhook_url_error_is_dqx_error(self):
        from databricks.labs.dqx.errors import DQXError

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

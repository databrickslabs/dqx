"""Unit tests for the DQX App backend modules."""

import base64
import json
import logging
from unittest.mock import create_autospec

import pytest
from databricks_labs_dqx_app.backend.dependencies import get_generator, get_obo_ws, get_spark
from databricks_labs_dqx_app.backend.logger import CustomFormatter, setup_logger, get_logger
from databricks_labs_dqx_app.backend.models import (
    DryRunIn,
    DryRunOut,
    InstallationSettings,
    RuleCatalogEntryOut,
    SaveRulesIn,
    SetStatusIn,
)
from databricks_labs_dqx_app.backend.routes.v1.dryrun import dry_run
from databricks_labs_dqx_app.backend.routes.v1.rules import (
    approve_rules,
    delete_rules,
    get_rules,
    list_rules,
    reject_rules,
    save_rules,
    submit_for_approval,
)
from databricks_labs_dqx_app.backend.services.rules_catalog_service import (
    RuleCatalogEntry,
    RulesCatalogService,
)
from databricks_labs_dqx_app.backend.settings import SettingsManager
from fastapi import HTTPException
from pydantic import ValidationError
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.iam import User
from databricks.sdk.service.sql import (
    ResultData,
    ServiceError,
    StatementResponse,
    StatementState,
    StatementStatus,
)
from databricks.sdk.service.workspace import ExportResponse


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    ws = create_autospec(WorkspaceClient)
    mock_user = create_autospec(User)
    mock_user.user_name = "test_user@example.com"
    ws.current_user.me.return_value = mock_user
    return ws


def _ok_response(data_array: list[list[str]] | None = None) -> StatementResponse:
    """Build a successful StatementResponse with optional result rows."""
    result = ResultData(data_array=data_array) if data_array else None
    return StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        result=result,
    )


def _failed_response(message: str = "boom") -> StatementResponse:
    """Build a failed StatementResponse."""
    return StatementResponse(
        status=StatementStatus(
            state=StatementState.FAILED,
            error=ServiceError(message=message),
        ),
    )


_SAMPLE_CHECKS = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]

_SAMPLE_ROW = [
    "catalog.schema.table",
    json.dumps(_SAMPLE_CHECKS),
    "3",
    "draft",
    "alice@example.com",
    "2025-01-01T00:00:00",
    "bob@example.com",
    "2025-06-15T12:00:00",
]


class TestGetOboWs:
    """Unit tests for the get_obo_ws dependency function."""

    def test_raises_when_no_token(self):
        """Should raise HTTPException when no token is provided."""
        with pytest.raises(HTTPException) as exc_info:
            get_obo_ws(token=None)
        assert exc_info.value.status_code == 401
        assert "Authentication required" in exc_info.value.detail

    def test_raises_when_empty_token(self):
        """Should raise HTTPException when empty token is provided."""
        with pytest.raises(HTTPException) as exc_info:
            get_obo_ws(token="")
        assert exc_info.value.status_code == 401
        assert "Authentication required" in exc_info.value.detail


class TestCustomFormatter:
    """Unit tests for CustomFormatter."""

    def _create_log_record(self, module: str, func_name: str, msg: str = "Test") -> logging.LogRecord:
        """Helper to create a LogRecord with specific module and function name."""
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=f"{module}.py",
            lineno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )
        record.module = module
        record.funcName = func_name
        return record

    def test_format_short_location(self):
        """Should include full location when it fits within max_length."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("router", "get_config")
        result = formatter.format(record)

        assert "router.get_config" in result

    def test_format_long_location_abbreviates_module(self):
        """Should abbreviate module parts when location is too long."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("databricks_labs_dqx_app.backend.router", "get_install_folder")
        result = formatter.format(record)

        # The location should be abbreviated to fit within 20 chars
        # Original would be "databricks_labs_dqx_app.backend.router.get_install_folder" (57 chars)
        # Abbreviated should be something like "d.l.d.b.r.get_instal" (20 chars)
        # Check that the full long name is NOT in the output
        assert "databricks_labs_dqx_app.backend.router.get_install_folder" not in result

    def test_format_module_level_code(self):
        """Should handle <module> function name by showing just module."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("router", "<module>")
        result = formatter.format(record)

        assert "router" in result
        assert "<module>" not in result

    def test_format_main_module(self):
        """Should handle __main__ module by showing just function name."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("__main__", "run")
        result = formatter.format(record)

        # __main__ should be stripped, leaving just the function name
        assert "__main__" not in result or "run" in result

    def test_format_includes_message(self):
        """Should include the log message in output."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("test", "test_func", msg="Hello World")
        result = formatter.format(record)

        assert "Hello World" in result

    def test_format_includes_level(self):
        """Should include the log level in output."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("test", "test_func")
        result = formatter.format(record)

        assert "INFO" in result

    def test_format_uses_pipe_separator(self):
        """Should use pipe-separated format."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("test", "test_func")
        result = formatter.format(record)

        assert "|" in result


class TestSetupLogger:
    """Unit tests for setup_logger function."""

    def test_creates_logger_with_name(self):
        """Should create logger with specified name."""
        log = setup_logger("test_logger", level=logging.DEBUG, use_colors=False)

        assert log.name == "test_logger"
        assert log.level == logging.DEBUG
        assert len(log.handlers) == 1

    def test_creates_logger_without_name(self):
        """Should create root logger when no name provided."""
        log = setup_logger(None, level=logging.WARNING, use_colors=False)

        assert log.name == "root"
        assert log.level == logging.WARNING

    def test_clears_existing_handlers(self):
        """Should clear existing handlers to avoid duplicates."""
        log = setup_logger("dup_test", level=logging.INFO, use_colors=False)
        initial_handlers = len(log.handlers)

        # Setup again - should still have same number of handlers
        log = setup_logger("dup_test", level=logging.INFO, use_colors=False)

        assert len(log.handlers) == initial_handlers


class TestGetLogger:
    """Unit tests for get_logger function."""

    def test_returns_default_logger_when_no_name(self):
        """Should return the default app logger when no name provided."""
        log = get_logger(None)
        assert log is not None

    def test_returns_named_logger(self):
        """Should return a new logger with specified name."""
        log = get_logger("custom_logger")
        assert log.name == "custom_logger"


class TestSettingsManager:
    """Unit tests for SettingsManager class."""

    def test_init_sets_paths(self, mock_workspace_client):
        """Should initialize with correct paths based on user."""
        manager = SettingsManager(mock_workspace_client)

        assert manager.user_home == "/Users/test_user@example.com"
        assert manager.default_dqx_folder == "/Users/test_user@example.com/.dqx"
        assert manager.app_settings_path == "/Users/test_user@example.com/.dqx/app.yml"

    def test_get_default_install_folder(self, mock_workspace_client):
        """Should return the default .dqx folder path."""
        manager = SettingsManager(mock_workspace_client)
        result = manager.get_default_install_folder()

        assert result == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_when_file_not_found(self, mock_workspace_client):
        """Should return default settings when app.yml doesn't exist."""
        mock_workspace_client.workspace.export.side_effect = ResourceDoesNotExist("Not found")

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_custom_when_file_exists(self, mock_workspace_client):
        """Should return custom settings when app.yml exists."""
        yaml_content = "install_folder: /custom/path"
        encoded = base64.b64encode(yaml_content.encode()).decode()

        mock_response = ExportResponse(content=encoded)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/custom/path"

    def test_get_settings_returns_default_when_file_has_default_path(self, mock_workspace_client):
        """Should return default settings when app.yml contains default path."""
        yaml_content = "install_folder: /Users/test_user@example.com/.dqx"
        encoded = base64.b64encode(yaml_content.encode()).decode()

        mock_response = ExportResponse(content=encoded)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_when_yaml_malformed(self, mock_workspace_client):
        """Should return default settings when app.yml has malformed YAML."""
        yaml_content = "install_folder: [unclosed bracket"
        encoded = base64.b64encode(yaml_content.encode()).decode()

        mock_response = ExportResponse(content=encoded)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_when_missing_install_folder_key(self, mock_workspace_client):
        """Should return default settings when app.yml is missing install_folder key."""
        yaml_content = "some_other_key: value"
        encoded = base64.b64encode(yaml_content.encode()).decode()

        mock_response = ExportResponse(content=encoded)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_when_content_empty(self, mock_workspace_client):
        """Should return default settings when app.yml has empty content."""
        mock_response = ExportResponse(content=None)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_on_generic_exception(self, mock_workspace_client):
        """Should return default settings when export throws unexpected error."""
        mock_workspace_client.workspace.export.side_effect = Exception("Unexpected error")

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_save_settings_creates_file_for_custom_path(self, mock_workspace_client):
        """Should create app.yml when saving custom path."""
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/custom/path")

        result = manager.save_settings(settings)

        assert mock_workspace_client.workspace.mkdirs.call_count == 2
        mock_workspace_client.workspace.upload.assert_called_once()
        assert result.install_folder == "/custom/path"

    def test_save_settings_creates_file_for_default_path(self, mock_workspace_client):
        """Should create app.yml even when saving default path."""
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/Users/test_user@example.com/.dqx")

        result = manager.save_settings(settings)

        assert mock_workspace_client.workspace.mkdirs.call_count == 1
        mock_workspace_client.workspace.upload.assert_called_once()
        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_save_settings_strips_whitespace(self, mock_workspace_client):
        """Should strip whitespace from install folder path."""
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="  /custom/path  ")

        result = manager.save_settings(settings)

        assert result.install_folder == "/custom/path"
        # Verify mkdirs was called with stripped path
        assert mock_workspace_client.workspace.mkdirs.call_args_list[1][0][0] == "/custom/path"

    def test_save_settings_raises_error_when_dqx_folder_creation_fails(self, mock_workspace_client):
        """Should raise ValueError when .dqx folder creation fails."""
        mock_workspace_client.workspace.mkdirs.side_effect = [Exception("Permission denied"), None]

        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/custom/path")

        with pytest.raises(ValueError, match="Could not create .dqx folder"):
            manager.save_settings(settings)

    def test_save_settings_raises_error_when_install_folder_creation_fails(self, mock_workspace_client):
        """Should raise ValueError when install folder creation fails."""
        mock_workspace_client.workspace.mkdirs.side_effect = [None, Exception("Permission denied")]

        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/custom/path")

        with pytest.raises(ValueError, match="Could not create install folder"):
            manager.save_settings(settings)

    def test_save_settings_raises_error_when_upload_fails(self, mock_workspace_client):
        """Should raise ValueError when upload fails."""
        mock_workspace_client.workspace.upload.side_effect = Exception("Upload failed")

        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/custom/path")

        with pytest.raises(ValueError, match="Could not save app settings to"):
            manager.save_settings(settings)

    def test_save_settings_with_empty_install_folder(self, mock_workspace_client):
        """Should handle empty install folder path."""
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="")

        result = manager.save_settings(settings)

        # Empty string strips to empty string
        assert result.install_folder == ""


# ============================================================================
# Tests for Pydantic models
# ============================================================================


class TestDryRunIn:
    """Unit tests for DryRunIn model validation."""

    def test_valid_defaults(self):
        """Should accept valid input with default sample_size."""
        body = DryRunIn(table_fqn="catalog.schema.table", checks=_SAMPLE_CHECKS)
        assert body.sample_size == 100

    def test_custom_sample_size(self):
        """Should accept a custom sample_size within bounds."""
        body = DryRunIn(table_fqn="c.s.t", checks=_SAMPLE_CHECKS, sample_size=500)
        assert body.sample_size == 500

    def test_rejects_sample_size_above_max(self):
        """Should reject sample_size > 1000."""
        with pytest.raises(ValidationError, match="sample_size"):
            DryRunIn(table_fqn="c.s.t", checks=_SAMPLE_CHECKS, sample_size=1001)


class TestDryRunOut:
    """Unit tests for DryRunOut model."""

    def test_round_trips(self):
        """Should serialize and deserialize correctly."""
        out = DryRunOut(
            total_rows=100,
            valid_rows=90,
            invalid_rows=10,
            error_summary=[{"error": "null", "count": 10}],
            sample_invalid=[{"id": None}],
        )
        assert out.total_rows == 100
        assert len(out.error_summary) == 1


class TestSaveRulesIn:
    """Unit tests for SaveRulesIn model."""

    def test_accepts_valid_payload(self):
        body = SaveRulesIn(table_fqn="catalog.schema.table", checks=_SAMPLE_CHECKS)
        assert body.table_fqn == "catalog.schema.table"
        assert len(body.checks) == 1


class TestSetStatusIn:
    """Unit tests for SetStatusIn model."""

    def test_defaults_expected_version_to_none(self):
        body = SetStatusIn(status="approved")
        assert body.expected_version is None

    def test_accepts_expected_version(self):
        body = SetStatusIn(status="approved", expected_version=5)
        assert body.expected_version == 5


class TestRuleCatalogEntryOut:
    """Unit tests for RuleCatalogEntryOut model."""

    def test_optional_fields_default_to_none(self):
        out = RuleCatalogEntryOut(table_fqn="c.s.t", checks=[], version=1, status="draft")
        assert out.created_by is None
        assert out.updated_at is None


# ============================================================================
# Tests for RuleCatalogEntry
# ============================================================================


class TestRuleCatalogEntry:
    """Unit tests for the RuleCatalogEntry dataclass."""

    def test_defaults(self):
        entry = RuleCatalogEntry(table_fqn="c.s.t", checks=[])
        assert entry.version == 1
        assert entry.status == "draft"
        assert entry.created_by is None

    def test_all_fields(self):
        entry = RuleCatalogEntry(
            table_fqn="c.s.t",
            checks=_SAMPLE_CHECKS,
            version=5,
            status="approved",
            created_by="a@b.com",
            created_at="2025-01-01",
            updated_by="c@d.com",
            updated_at="2025-06-01",
        )
        assert entry.version == 5
        assert entry.status == "approved"


# ============================================================================
# Tests for RulesCatalogService
# ============================================================================


class TestRulesCatalogService:
    """Unit tests for RulesCatalogService with mocked WorkspaceClient."""

    @pytest.fixture
    def ws(self):
        mock_ws = create_autospec(WorkspaceClient)
        return mock_ws

    @pytest.fixture
    def svc(self, ws):
        return RulesCatalogService(ws=ws, warehouse_id="wh-123", catalog="cat", schema="sch")

    # ---------- ensure_table ----------

    def test_ensure_table_executes_create(self, svc, ws):
        """Should execute a CREATE TABLE IF NOT EXISTS statement."""
        ws.statement_execution.execute_statement.return_value = _ok_response()
        svc.ensure_table()
        sql_arg = ws.statement_execution.execute_statement.call_args.kwargs["statement"]
        assert "CREATE TABLE IF NOT EXISTS" in sql_arg
        assert "cat.sch.dq_quality_rules" in sql_arg

    # ---------- get ----------

    def test_get_returns_entry_when_found(self, svc, ws):
        """Should return a RuleCatalogEntry when a row exists."""
        ws.statement_execution.execute_statement.return_value = _ok_response([_SAMPLE_ROW])
        entry = svc.get("catalog.schema.table")

        assert entry is not None
        assert entry.table_fqn == "catalog.schema.table"
        assert entry.version == 3
        assert entry.status == "draft"
        assert entry.checks == _SAMPLE_CHECKS

    def test_get_returns_none_when_not_found(self, svc, ws):
        """Should return None when no rows match."""
        ws.statement_execution.execute_statement.return_value = _ok_response()
        assert svc.get("no.such.table") is None

    # ---------- list_rules ----------

    def test_list_rules_returns_all_entries(self, svc, ws):
        """Should return all entries when no status filter is given."""
        ws.statement_execution.execute_statement.return_value = _ok_response([_SAMPLE_ROW, _SAMPLE_ROW])
        entries = svc.list_rules()
        assert len(entries) == 2

    def test_list_rules_filters_by_status(self, svc, ws):
        """Should add a WHERE clause when status is provided."""
        ws.statement_execution.execute_statement.return_value = _ok_response([_SAMPLE_ROW])
        svc.list_rules(status="approved")
        sql_arg = ws.statement_execution.execute_statement.call_args.kwargs["statement"]
        assert "WHERE status = 'approved'" in sql_arg

    def test_list_rules_returns_empty(self, svc, ws):
        """Should return empty list when no rows exist."""
        ws.statement_execution.execute_statement.return_value = _ok_response()
        assert svc.list_rules() == []

    # ---------- save ----------

    def test_save_returns_entry_from_get(self, svc, ws):
        """After MERGE, should re-read and return the persisted entry."""
        ws.statement_execution.execute_statement.side_effect = [
            _ok_response(),  # MERGE
            _ok_response([_SAMPLE_ROW]),  # GET after save
        ]
        entry = svc.save("catalog.schema.table", _SAMPLE_CHECKS, "alice@example.com")
        assert entry.table_fqn == "catalog.schema.table"

    def test_save_returns_fallback_when_get_is_none(self, svc, ws):
        """Should return a constructed entry if the re-read returns nothing."""
        ws.statement_execution.execute_statement.side_effect = [
            _ok_response(),  # MERGE
            _ok_response(),  # GET returns empty
        ]
        entry = svc.save("c.s.t", _SAMPLE_CHECKS, "alice@example.com")
        assert entry.table_fqn == "c.s.t"
        assert entry.version == 1
        assert entry.status == "draft"

    # ---------- delete ----------

    def test_delete_executes_delete_sql(self, svc, ws):
        """Should execute a DELETE statement for the given table."""
        ws.statement_execution.execute_statement.return_value = _ok_response()
        svc.delete("c.s.t", "bob@example.com")
        sql_arg = ws.statement_execution.execute_statement.call_args.kwargs["statement"]
        assert "DELETE FROM" in sql_arg
        assert "c.s.t" in sql_arg

    # ---------- set_status ----------

    def test_set_status_valid_transition(self, svc, ws):
        """Should update status for a valid transition (draft → pending_approval)."""
        draft_row = list(_SAMPLE_ROW)
        draft_row[3] = "draft"
        pending_row = list(_SAMPLE_ROW)
        pending_row[3] = "pending_approval"

        ws.statement_execution.execute_statement.side_effect = [
            _ok_response([draft_row]),  # get (before)
            _ok_response(),  # UPDATE
            _ok_response([pending_row]),  # get (after)
        ]
        entry = svc.set_status("catalog.schema.table", "pending_approval", "alice@example.com")
        assert entry.status == "pending_approval"

    def test_set_status_rejects_invalid_status(self, svc, ws):
        """Should raise ValueError for an unknown status value."""
        with pytest.raises(ValueError, match="Invalid status"):
            svc.set_status("c.s.t", "unknown_status", "a@b.com")

    def test_set_status_rejects_invalid_transition(self, svc, ws):
        """Should raise ValueError when the transition is not allowed."""
        draft_row = list(_SAMPLE_ROW)
        draft_row[3] = "draft"
        ws.statement_execution.execute_statement.return_value = _ok_response([draft_row])

        with pytest.raises(ValueError, match="Cannot transition"):
            svc.set_status("catalog.schema.table", "approved", "a@b.com")

    def test_set_status_raises_when_not_found(self, svc, ws):
        """Should raise RuntimeError when no rule set exists for the table."""
        ws.statement_execution.execute_statement.return_value = _ok_response()
        with pytest.raises(RuntimeError, match="Rule set not found"):
            svc.set_status("no.such.table", "pending_approval", "a@b.com")

    def test_set_status_version_conflict(self, svc, ws):
        """Should raise RuntimeError when expected_version doesn't match."""
        draft_row = list(_SAMPLE_ROW)
        draft_row[3] = "draft"
        draft_row[2] = "3"
        ws.statement_execution.execute_statement.return_value = _ok_response([draft_row])

        with pytest.raises(RuntimeError, match="Version conflict"):
            svc.set_status("catalog.schema.table", "pending_approval", "a@b.com", expected_version=1)

    def test_set_status_detects_concurrent_modification(self, svc, ws):
        """Should raise RuntimeError when the re-read shows the update didn't take effect."""
        draft_row = list(_SAMPLE_ROW)
        draft_row[3] = "draft"
        still_draft = list(_SAMPLE_ROW)
        still_draft[3] = "draft"

        ws.statement_execution.execute_statement.side_effect = [
            _ok_response([draft_row]),  # get (before)
            _ok_response(),  # UPDATE
            _ok_response([still_draft]),  # get (after) — status unchanged
        ]
        with pytest.raises(RuntimeError, match="did not take effect"):
            svc.set_status("catalog.schema.table", "pending_approval", "a@b.com")

    # ---------- _execute / _query error handling ----------

    def test_execute_raises_on_sql_failure(self, svc, ws):
        """Should raise RuntimeError when the SQL statement fails."""
        ws.statement_execution.execute_statement.return_value = _failed_response("syntax error")
        with pytest.raises(RuntimeError, match="SQL execution failed: syntax error"):
            svc.ensure_table()

    def test_query_raises_on_sql_failure(self, svc, ws):
        """Should raise RuntimeError when a query fails."""
        ws.statement_execution.execute_statement.return_value = _failed_response("timeout")
        with pytest.raises(RuntimeError, match="SQL execution failed: timeout"):
            svc.get("c.s.t")


class TestRulesCatalogServiceRowConversion:
    """Unit tests for RulesCatalogService._row_to_entry edge cases."""

    @pytest.fixture
    def svc(self):
        mock_ws = create_autospec(WorkspaceClient)
        return RulesCatalogService(ws=mock_ws, warehouse_id="wh", catalog="cat", schema="sch")

    def test_row_to_entry_handles_missing_checks(self, svc):
        """Should return empty checks list when checks column is empty."""
        row = ["c.s.t", "", "1", "draft", None, None, None, None]
        entry = svc._row_to_entry(row)  # pylint: disable=protected-access
        assert entry.checks == []
        assert entry.version == 1

    def test_row_to_entry_handles_missing_version(self, svc):
        """Should default to version 1 when version column is empty."""
        row = ["c.s.t", "[]", "", "draft", None, None, None, None]
        entry = svc._row_to_entry(row)  # pylint: disable=protected-access
        assert entry.version == 1

    def test_row_to_entry_handles_missing_status(self, svc):
        """Should default to 'draft' when status column is None."""
        row = ["c.s.t", "[]", "1", None, None, None, None, None]
        entry = svc._row_to_entry(row)  # pylint: disable=protected-access
        assert entry.status == "draft"


# ============================================================================
# Tests for rules routes
# ============================================================================


class TestRulesRoutesRead:
    """Unit tests for rules route GET/list handlers."""

    @pytest.fixture
    def mock_svc(self):
        svc = create_autospec(RulesCatalogService)
        return svc

    @pytest.fixture
    def sample_entry(self):
        return RuleCatalogEntry(
            table_fqn="catalog.schema.table",
            checks=_SAMPLE_CHECKS,
            version=1,
            status="draft",
            created_by="alice@example.com",
            created_at="2025-01-01T00:00:00",
            updated_by="alice@example.com",
            updated_at="2025-01-01T00:00:00",
        )

    def test_list_rules_returns_entries(self, mock_svc, sample_entry):
        mock_svc.list_rules.return_value = [sample_entry]
        result = list_rules(svc=mock_svc, status=None)
        assert len(result) == 1
        assert result[0].table_fqn == "catalog.schema.table"

    def test_list_rules_passes_status_filter(self, mock_svc):
        mock_svc.list_rules.return_value = []
        list_rules(svc=mock_svc, status="approved")
        mock_svc.list_rules.assert_called_once_with(status="approved")

    def test_list_rules_raises_500_on_error(self, mock_svc):
        mock_svc.list_rules.side_effect = RuntimeError("db error")
        with pytest.raises(HTTPException) as exc:
            list_rules(svc=mock_svc, status=None)
        assert exc.value.status_code == 500

    def test_get_rules_found(self, mock_svc, sample_entry):
        mock_svc.get.return_value = sample_entry
        result = get_rules(table_fqn="catalog.schema.table", svc=mock_svc)
        assert result.table_fqn == "catalog.schema.table"

    def test_get_rules_not_found(self, mock_svc):
        mock_svc.get.return_value = None
        with pytest.raises(HTTPException) as exc:
            get_rules(table_fqn="no.such.table", svc=mock_svc)
        assert exc.value.status_code == 404


class TestRulesRoutesWrite:
    """Unit tests for rules route POST/DELETE/status handlers."""

    @pytest.fixture
    def mock_svc(self):
        svc = create_autospec(RulesCatalogService)
        return svc

    @pytest.fixture
    def mock_obo_ws(self):
        obo_ws = create_autospec(WorkspaceClient)
        user = create_autospec(User)
        user.user_name = "alice@example.com"
        obo_ws.current_user.me.return_value = user
        return obo_ws

    @pytest.fixture
    def sample_entry(self):
        return RuleCatalogEntry(
            table_fqn="catalog.schema.table",
            checks=_SAMPLE_CHECKS,
            version=1,
            status="draft",
            created_by="alice@example.com",
            created_at="2025-01-01T00:00:00",
            updated_by="alice@example.com",
            updated_at="2025-01-01T00:00:00",
        )

    def test_save_rules_success(self, mock_svc, mock_obo_ws, sample_entry):
        mock_svc.save.return_value = sample_entry
        body = SaveRulesIn(table_fqn="catalog.schema.table", checks=_SAMPLE_CHECKS)
        result = save_rules(body=body, svc=mock_svc, obo_ws=mock_obo_ws)
        assert result.version == 1
        mock_svc.save.assert_called_once_with("catalog.schema.table", _SAMPLE_CHECKS, "alice@example.com")

    def test_save_rules_500_on_error(self, mock_svc, mock_obo_ws):
        mock_svc.save.side_effect = RuntimeError("db error")
        body = SaveRulesIn(table_fqn="c.s.t", checks=_SAMPLE_CHECKS)
        with pytest.raises(HTTPException) as exc:
            save_rules(body=body, svc=mock_svc, obo_ws=mock_obo_ws)
        assert exc.value.status_code == 500

    def test_delete_rules_success(self, mock_svc, mock_obo_ws):
        result = delete_rules(table_fqn="c.s.t", svc=mock_svc, obo_ws=mock_obo_ws)
        assert result["status"] == "deleted"
        mock_svc.delete.assert_called_once_with("c.s.t", "alice@example.com")

    def test_submit_for_approval_success(self, mock_svc, mock_obo_ws, sample_entry):
        sample_entry.status = "pending_approval"
        mock_svc.set_status.return_value = sample_entry
        result = submit_for_approval(table_fqn="c.s.t", svc=mock_svc, obo_ws=mock_obo_ws, body=None)
        assert result.status == "pending_approval"
        mock_svc.set_status.assert_called_once_with("c.s.t", "pending_approval", "alice@example.com", None)

    def test_submit_for_approval_with_expected_version(self, mock_svc, mock_obo_ws, sample_entry):
        sample_entry.status = "pending_approval"
        mock_svc.set_status.return_value = sample_entry
        body = SetStatusIn(status="pending_approval", expected_version=3)
        submit_for_approval(table_fqn="c.s.t", svc=mock_svc, obo_ws=mock_obo_ws, body=body)
        mock_svc.set_status.assert_called_once_with("c.s.t", "pending_approval", "alice@example.com", 3)

    def test_submit_returns_400_on_value_error(self, mock_svc, mock_obo_ws):
        mock_svc.set_status.side_effect = ValueError("bad transition")
        with pytest.raises(HTTPException) as exc:
            submit_for_approval(table_fqn="c.s.t", svc=mock_svc, obo_ws=mock_obo_ws, body=None)
        assert exc.value.status_code == 400

    def test_submit_returns_409_on_runtime_error(self, mock_svc, mock_obo_ws):
        mock_svc.set_status.side_effect = RuntimeError("version conflict")
        with pytest.raises(HTTPException) as exc:
            submit_for_approval(table_fqn="c.s.t", svc=mock_svc, obo_ws=mock_obo_ws, body=None)
        assert exc.value.status_code == 409

    def test_approve_rules_success(self, mock_svc, mock_obo_ws, sample_entry):
        sample_entry.status = "approved"
        mock_svc.set_status.return_value = sample_entry
        result = approve_rules(table_fqn="c.s.t", svc=mock_svc, obo_ws=mock_obo_ws, body=None)
        assert result.status == "approved"
        mock_svc.set_status.assert_called_once_with("c.s.t", "approved", "alice@example.com", None)

    def test_reject_rules_success(self, mock_svc, mock_obo_ws, sample_entry):
        sample_entry.status = "rejected"
        mock_svc.set_status.return_value = sample_entry
        result = reject_rules(table_fqn="c.s.t", svc=mock_svc, obo_ws=mock_obo_ws, body=None)
        assert result.status == "rejected"
        mock_svc.set_status.assert_called_once_with("c.s.t", "rejected", "alice@example.com", None)


# ============================================================================
# Tests for dryrun route
# ============================================================================


class TestDryRunRoute:
    """Unit tests for the dry_run route handler."""

    @pytest.fixture
    def mock_engine(self):
        engine = create_autospec(DQEngine, instance=True)
        engine.spark = create_autospec(SparkSession)
        return engine

    def test_dry_run_returns_400_on_invalid_checks(self, mock_engine, monkeypatch):
        """Should return 400 when check validation fails."""
        validation_result = create_autospec(DQEngine.validate_checks, instance=False)
        validation_result.has_errors = True
        validation_result.errors = ["bad check"]

        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.routes.v1.dryrun.DQEngine.validate_checks",
            lambda _: validation_result,
        )
        body = DryRunIn(table_fqn="c.s.t", checks=[{"bad": "check"}])
        with pytest.raises(HTTPException) as exc:
            dry_run(body=body, engine=mock_engine)
        assert exc.value.status_code == 400
        assert "Invalid checks" in exc.value.detail

    def test_dry_run_returns_500_on_unexpected_error(self, mock_engine, monkeypatch):
        """Should return 500 when Spark read fails."""
        validation_result = create_autospec(DQEngine.validate_checks, instance=False)
        validation_result.has_errors = False

        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.routes.v1.dryrun.DQEngine.validate_checks",
            lambda _: validation_result,
        )
        mock_engine.spark.read.table.side_effect = RuntimeError("connection lost")
        body = DryRunIn(table_fqn="c.s.t", checks=_SAMPLE_CHECKS)
        with pytest.raises(HTTPException) as exc:
            dry_run(body=body, engine=mock_engine)
        assert exc.value.status_code == 500

    def test_dry_run_success_all_valid(self, mock_engine, monkeypatch):
        """Should return correct counts when all rows are valid."""
        validation_result = create_autospec(DQEngine.validate_checks, instance=False)
        validation_result.has_errors = False

        sampled_df = create_autospec(DataFrame)
        sampled_df.count.return_value = 10
        mock_engine.spark.read.table.return_value.limit.return_value = sampled_df

        valid_df = create_autospec(DataFrame)
        valid_df.count.return_value = 10
        invalid_df = create_autospec(DataFrame)
        invalid_df.count.return_value = 0
        mock_engine.apply_checks_by_metadata_and_split.return_value = (valid_df, invalid_df)

        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.routes.v1.dryrun.DQEngine.validate_checks",
            lambda _: validation_result,
        )
        body = DryRunIn(table_fqn="c.s.t", checks=_SAMPLE_CHECKS)
        result = dry_run(body=body, engine=mock_engine)

        assert result.total_rows == 10
        assert result.valid_rows == 10
        assert result.invalid_rows == 0
        assert result.error_summary == []
        assert result.sample_invalid == []


# ============================================================================
# Tests for dependency functions
# ============================================================================


class TestGetSpark:
    """Unit tests for get_spark dependency."""

    def test_raises_when_no_token(self):
        with pytest.raises(HTTPException) as exc:
            get_spark(token=None)
        assert exc.value.status_code == 401

    def test_raises_when_empty_token(self):
        with pytest.raises(HTTPException) as exc:
            get_spark(token="")
        assert exc.value.status_code == 401


class TestGetGenerator:
    """Unit tests for get_generator dependency."""

    def test_raises_when_no_token(self):
        mock_ws = create_autospec(WorkspaceClient)
        mock_spark = create_autospec(SparkSession)

        with pytest.raises(HTTPException) as exc:
            get_generator(obo_ws=mock_ws, spark=mock_spark, token=None)
        assert exc.value.status_code == 401

"""Unit tests for the DQX App backend modules."""

import asyncio
import base64
import json
import logging
from unittest.mock import create_autospec

import pytest
from databricks_labs_dqx_app.backend.cache import CacheFactory
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import get_obo_ws
from databricks_labs_dqx_app.backend.logger import CustomFormatter, get_logger, setup_logger
from databricks_labs_dqx_app.backend.migrations import MIGRATIONS, MigrationRunner
from databricks_labs_dqx_app.backend.models import (
    DryRunIn,
    DryRunOut,
    DryRunResultsOut,
    DryRunSubmitOut,
    InstallationSettings,
    ProfileResultsOut,
    ProfileRunIn,
    ProfileRunOut,
    RuleCatalogEntryOut,
    RunStatusOut,
    SaveRulesIn,
    SetStatusIn,
)
from databricks_labs_dqx_app.backend.routes.v1.dryrun import (
    get_dry_run_results,
    get_dry_run_status,
    submit_dry_run,
)
from databricks_labs_dqx_app.backend.routes.v1.profiler import (
    get_profile_run_results,
    get_profile_run_status,
    list_profile_runs,
    submit_profile_run,
)
from databricks_labs_dqx_app.backend.routes.v1.rules import (
    approve_rules,
    delete_rules,
    get_rules,
    list_rules,
    reject_rules,
    save_rules,
    submit_for_approval,
)
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService
from databricks_labs_dqx_app.backend.services.job_service import JobService, RunStatus
from databricks_labs_dqx_app.backend.services.rules_catalog_service import (
    RuleCatalogEntry,
    RulesCatalogService,
)
from databricks_labs_dqx_app.backend.services.view_service import ViewService
from databricks_labs_dqx_app.backend.settings import SettingsManager
from fastapi import HTTPException
from pydantic import ValidationError

from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.catalog import CatalogInfo, ColumnInfo as CatalogColumnInfo, SchemaInfo, TableInfo
from databricks.sdk.service.iam import User
from databricks.sdk.service.jobs import Run, RunLifeCycleState, RunResultState
from databricks.sdk.service.sql import (
    ColumnInfo as SQLColumnInfo,
    ResultData,
    ResultManifest,
    ResultSchema,
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

        async def _() -> None:
            with pytest.raises(HTTPException) as exc_info:
                await get_obo_ws(token=None)
            assert exc_info.value.status_code == 401
            assert "Authentication required" in exc_info.value.detail

        asyncio.run(_())

    def test_raises_when_empty_token(self):
        """Should raise HTTPException when empty token is provided."""

        async def _() -> None:
            with pytest.raises(HTTPException) as exc_info:
                await get_obo_ws(token="")
            assert exc_info.value.status_code == 401
            assert "Authentication required" in exc_info.value.detail

        asyncio.run(_())


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
        assert body.sample_size == 1000

    def test_custom_sample_size(self):
        """Should accept a custom sample_size within bounds."""
        body = DryRunIn(table_fqn="c.s.t", checks=_SAMPLE_CHECKS, sample_size=500)
        assert body.sample_size == 500

    def test_rejects_sample_size_above_max(self):
        """Should reject sample_size > 10000."""
        with pytest.raises(ValidationError, match="sample_size"):
            DryRunIn(table_fqn="c.s.t", checks=_SAMPLE_CHECKS, sample_size=10001)


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
            _ok_response(),  # _record_history INSERT
            _ok_response([_SAMPLE_ROW]),  # GET after save
        ]
        entry = svc.save("catalog.schema.table", _SAMPLE_CHECKS, "alice@example.com")
        assert entry.table_fqn == "catalog.schema.table"

    def test_save_returns_fallback_when_get_is_none(self, svc, ws):
        """Should return a constructed entry if the re-read returns nothing."""
        ws.statement_execution.execute_statement.side_effect = [
            _ok_response(),  # MERGE
            _ok_response(),  # _record_history INSERT
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
        calls = ws.statement_execution.execute_statement.call_args_list
        sql_stmts = [c.kwargs["statement"] for c in calls]
        assert any("DELETE FROM" in s and "c.s.t" in s for s in sql_stmts)

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
            _ok_response(),  # _record_history INSERT
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
            _ok_response(),  # _record_history INSERT
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
        entry = svc.row_to_entry(row)
        assert entry.checks == []
        assert entry.version == 1

    def test_row_to_entry_handles_missing_version(self, svc):
        """Should default to version 1 when version column is empty."""
        row = ["c.s.t", "[]", "", "draft", None, None, None, None]
        entry = svc.row_to_entry(row)
        assert entry.version == 1

    def test_row_to_entry_handles_missing_status(self, svc):
        """Should default to 'draft' when status column is None."""
        row = ["c.s.t", "[]", "1", None, None, None, None, None]
        entry = svc.row_to_entry(row)
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
        mock_svc.save.assert_called_once_with("catalog.schema.table", _SAMPLE_CHECKS, "alice@example.com", source="ui")

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
# Helper for building tabular SQL responses (with named columns)
# ============================================================================


def _make_tabular_response(columns: list[str], rows: list[list[str]]) -> StatementResponse:
    """Build a StatementResponse with named columns and data rows."""
    return StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        result=ResultData(data_array=rows),
        manifest=ResultManifest(schema=ResultSchema(columns=[SQLColumnInfo(name=col_name) for col_name in columns])),
    )


# ============================================================================
# Tests for CacheFactory
# ============================================================================


class TestCacheFactory:
    """Unit tests for the async in-memory CacheFactory."""

    def test_get_returns_none_for_missing_key(self) -> None:
        """Missing key should return None."""

        async def _() -> None:
            cache = CacheFactory()
            assert await cache.get("absent") is None

        asyncio.run(_())

    def test_set_and_get_round_trips(self) -> None:
        """Value stored with set should be retrievable with get."""

        async def _() -> None:
            cache = CacheFactory()
            await cache.set("key", "hello")
            assert await cache.get("key") == "hello"

        asyncio.run(_())

    def test_get_returns_none_after_ttl_expires(self) -> None:
        """Expired entries should not be returned."""

        async def _() -> None:
            cache = CacheFactory(ttl=0)
            await cache.set("key", "value", ttl=0)
            # TTL=0 means already expired at the moment of set
            assert await cache.get("key") is None

        asyncio.run(_())

    def test_delete_removes_key(self) -> None:
        """Deleted key should not be retrievable."""

        async def _() -> None:
            cache = CacheFactory()
            await cache.set("key", "value")
            await cache.delete("key")
            assert await cache.get("key") is None

        asyncio.run(_())

    def test_clear_removes_all_keys(self) -> None:
        """clear() should remove all stored entries."""

        async def _() -> None:
            cache = CacheFactory()
            await cache.set("a", 1)
            await cache.set("b", 2)
            await cache.clear()
            assert await cache.get("a") is None
            assert await cache.get("b") is None

        asyncio.run(_())

    def test_set_fire_and_forget_stores_value(self) -> None:
        """set_fire_and_forget should schedule a write that resolves on next tick."""

        async def _() -> None:
            cache = CacheFactory()
            cache.set_fire_and_forget("key", "value")
            await asyncio.sleep(0)  # let the task execute
            assert await cache.get("key") == "value"

        asyncio.run(_())

    def test_set_reliable_stores_value(self) -> None:
        """set_reliable should await the write successfully."""

        async def _() -> None:
            cache = CacheFactory()
            await cache.set_reliable("key", "value")
            assert await cache.get("key") == "value"

        asyncio.run(_())

    def test_cached_decorator_calls_fn_on_miss(self) -> None:
        """On cache miss the decorated function should be invoked."""

        async def _() -> None:
            cache = CacheFactory()
            call_count = 0

            @cache.cached("test:items")
            async def get_items() -> list[str]:
                nonlocal call_count
                call_count += 1
                return ["a", "b"]

            result = await get_items()
            assert result == ["a", "b"]
            assert call_count == 1

        asyncio.run(_())

    def test_cached_decorator_returns_cached_on_hit(self) -> None:
        """Second call to a cached function should not invoke the function again."""

        async def _() -> None:
            cache = CacheFactory()
            call_count = 0

            @cache.cached("test:items2")
            async def get_items() -> list[str]:
                nonlocal call_count
                call_count += 1
                return ["a", "b"]

            await get_items()
            await asyncio.sleep(0)  # allow fire-and-forget cache write to complete
            await get_items()
            assert call_count == 1

        asyncio.run(_())

    def test_cached_decorator_resolves_user_key(self) -> None:
        """The {_user} placeholder should be resolved from self.user_id."""

        async def _() -> None:
            cache = CacheFactory()

            class Service:
                user_id = "alice"

                @cache.cached("discovery:{_user}:catalogs")
                async def list_catalogs(self) -> list[str]:
                    return ["cat1"]

            svc = Service()
            result1 = await svc.list_catalogs()
            result2 = await svc.list_catalogs()
            assert result1 == ["cat1"]
            assert result2 == ["cat1"]

        asyncio.run(_())


# ============================================================================
# Tests for RunStatus model
# ============================================================================


class TestRunStatus:
    """Unit tests for the RunStatus Pydantic model."""

    def test_defaults(self) -> None:
        status = RunStatus(state="RUNNING")
        assert status.result_state is None
        assert status.message is None

    def test_all_fields(self) -> None:
        status = RunStatus(state="TERMINATED", result_state="SUCCESS", message="Done")
        assert status.state == "TERMINATED"
        assert status.result_state == "SUCCESS"
        assert status.message == "Done"


# ============================================================================
# Tests for JobService
# ============================================================================


class TestJobService:
    """Unit tests for JobService."""

    @pytest.fixture
    def ws(self) -> WorkspaceClient:
        ws = create_autospec(WorkspaceClient)
        return ws

    @pytest.fixture
    def svc(self, ws: WorkspaceClient) -> JobService:
        return JobService(ws=ws, job_id="42", catalog="cat", schema="sch", warehouse_id="wh-1")

    def test_submit_run_raises_when_no_job_id(self, ws: WorkspaceClient) -> None:
        """Should raise RuntimeError when job_id is not configured."""
        svc = JobService(ws=ws, job_id="", catalog="cat", schema="sch", warehouse_id="wh-1")
        with pytest.raises(RuntimeError, match="DQX_JOB_ID is not configured"):
            svc.submit_run(
                task_type="dryrun",
                view_fqn="cat.sch.tmp_view_abc",
                config={"checks": []},
                run_id="run-001",
                requesting_user="alice@example.com",
            )

    def test_submit_run_calls_jobs_run_now(self, svc: JobService, ws: WorkspaceClient) -> None:
        """submit_run should call jobs.run_now with correct parameters and return run_id."""
        mock_run = create_autospec(Run)
        mock_run.run_id = 99999
        ws.jobs.run_now.return_value = mock_run  # type: ignore[attr-defined]

        job_run_id = svc.submit_run(
            task_type="profile",
            view_fqn="cat.sch.tmp_view_xyz",
            config={"sample_limit": 1000},
            run_id="run-002",
            requesting_user="bob@example.com",
        )

        assert job_run_id == 99999
        call_kwargs = ws.jobs.run_now.call_args.kwargs  # type: ignore[attr-defined]
        assert call_kwargs["job_id"] == 42
        params = call_kwargs["job_parameters"]
        assert params["task_type"] == "profile"
        assert params["run_id"] == "run-002"
        assert params["requesting_user"] == "bob@example.com"

    def test_get_run_status_returns_status(self, svc: JobService, ws: WorkspaceClient) -> None:
        """get_run_status should map life_cycle_state and result_state."""
        mock_run = create_autospec(Run)
        mock_run.state.life_cycle_state = RunLifeCycleState.TERMINATED
        mock_run.state.result_state = RunResultState.SUCCESS
        mock_run.state.state_message = None
        ws.jobs.get_run.return_value = mock_run  # type: ignore[attr-defined]

        status = svc.get_run_status(99999)

        assert status.state == "TERMINATED"
        assert status.result_state == "SUCCESS"
        assert status.message is None

    def test_get_run_status_handles_no_result_state(self, svc: JobService, ws: WorkspaceClient) -> None:
        """get_run_status should return None for result_state when not present."""
        mock_run = create_autospec(Run)
        mock_run.state.life_cycle_state = RunLifeCycleState.RUNNING
        mock_run.state.result_state = None
        mock_run.state.state_message = "In progress"
        ws.jobs.get_run.return_value = mock_run  # type: ignore[attr-defined]

        status = svc.get_run_status(12345)

        assert status.state == "RUNNING"
        assert status.result_state is None
        assert status.message == "In progress"

    def test_list_run_rows_returns_dict_list(self, svc: JobService, ws: WorkspaceClient) -> None:
        """list_run_rows should return a list of dicts keyed by column name."""
        ws.statement_execution.execute_statement.return_value = _make_tabular_response(  # type: ignore[attr-defined]
            columns=["run_id", "status"],
            rows=[["abc", "SUCCEEDED"], ["def", "FAILED"]],
        )

        rows = svc.list_run_rows("cat.sch.dq_profiling_results")

        assert len(rows) == 2
        assert rows[0] == {"run_id": "abc", "status": "SUCCEEDED"}
        assert rows[1] == {"run_id": "def", "status": "FAILED"}

    def test_list_run_rows_returns_empty_when_no_data(self, svc: JobService, ws: WorkspaceClient) -> None:
        """list_run_rows should return [] when the table has no rows."""
        resp = StatementResponse(
            status=StatementStatus(state=StatementState.SUCCEEDED),
            result=ResultData(data_array=[]),
        )
        ws.statement_execution.execute_statement.return_value = resp  # type: ignore[attr-defined]

        rows = svc.list_run_rows("cat.sch.dq_profiling_results")

        assert rows == []

    def test_list_run_rows_raises_on_sql_failure(self, svc: JobService, ws: WorkspaceClient) -> None:
        """list_run_rows should raise RuntimeError on SQL execution failure."""
        ws.statement_execution.execute_statement.return_value = _failed_response("timeout")  # type: ignore[attr-defined]

        with pytest.raises(RuntimeError, match="List query failed: timeout"):
            svc.list_run_rows("cat.sch.dq_profiling_results")

    def test_get_run_result_row_returns_dict(self, svc: JobService, ws: WorkspaceClient) -> None:
        """get_run_result_row should return the first row as a dict."""
        ws.statement_execution.execute_statement.return_value = _make_tabular_response(  # type: ignore[attr-defined]
            columns=["run_id", "status", "total_rows"],
            rows=[["run-001", "SUCCEEDED", "500"]],
        )

        row = svc.get_run_result_row("cat.sch.dq_validation_runs", "run-001")

        assert row is not None
        assert row["run_id"] == "run-001"
        assert row["status"] == "SUCCEEDED"
        assert row["total_rows"] == "500"

    def test_get_run_result_row_returns_none_when_empty(self, svc: JobService, ws: WorkspaceClient) -> None:
        """get_run_result_row should return None when no row matches."""
        resp = StatementResponse(
            status=StatementStatus(state=StatementState.SUCCEEDED),
            result=ResultData(data_array=[]),
        )
        ws.statement_execution.execute_statement.return_value = resp  # type: ignore[attr-defined]

        result = svc.get_run_result_row("cat.sch.dq_validation_runs", "run-missing")

        assert result is None

    def test_get_run_result_row_raises_on_sql_failure(self, svc: JobService, ws: WorkspaceClient) -> None:
        """get_run_result_row should raise RuntimeError on SQL failure."""
        ws.statement_execution.execute_statement.return_value = _failed_response("permission denied")  # type: ignore[attr-defined]

        with pytest.raises(RuntimeError, match="Results query failed: permission denied"):
            svc.get_run_result_row("cat.sch.dq_validation_runs", "run-001")


# ============================================================================
# Tests for ViewService
# ============================================================================


class TestViewService:
    """Unit tests for ViewService."""

    @pytest.fixture
    def ws(self) -> WorkspaceClient:
        ws = create_autospec(WorkspaceClient)
        return ws

    @pytest.fixture
    def svc(self, ws: WorkspaceClient) -> ViewService:
        return ViewService(ws=ws, warehouse_id="wh-1", catalog="cat", schema="sch")

    def test_create_view_returns_fqn(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """create_view should return a fully qualified view name."""
        ws.statement_execution.execute_statement.return_value = _ok_response()  # type: ignore[attr-defined]

        result = svc.create_view("cat.sch.src_table")

        assert result.startswith("cat.sch.tmp_view_")

    def test_create_view_executes_correct_sql(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """create_view should issue CREATE OR REPLACE VIEW referencing the source table."""
        ws.statement_execution.execute_statement.return_value = _ok_response()  # type: ignore[attr-defined]

        svc.create_view("cat.sch.src_table")

        calls = ws.statement_execution.execute_statement.call_args_list  # type: ignore[attr-defined]
        sql_stmts = [c.kwargs["statement"] for c in calls]
        assert any("CREATE OR REPLACE VIEW" in s and "cat.sch.src_table" in s for s in sql_stmts)

    def test_create_view_adds_limit_clause_when_sample_limit_given(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """create_view with sample_limit should append LIMIT to the SQL."""
        ws.statement_execution.execute_statement.return_value = _ok_response()  # type: ignore[attr-defined]

        svc.create_view("cat.sch.src_table", sample_limit=5000)

        calls = ws.statement_execution.execute_statement.call_args_list  # type: ignore[attr-defined]
        sql_stmts = [c.kwargs["statement"] for c in calls]
        assert any("LIMIT 5000" in s for s in sql_stmts)

    def test_create_view_raises_on_sql_failure(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """_ensure_schema should raise RuntimeError when the statement fails."""
        ws.statement_execution.execute_statement.return_value = _failed_response("access denied")  # type: ignore[attr-defined]

        with pytest.raises(RuntimeError, match="access denied"):
            svc.create_view("cat.sch.src_table")

    def test_drop_view_executes_drop_sql(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """drop_view should issue DROP VIEW IF EXISTS."""
        ws.statement_execution.execute_statement.return_value = _ok_response()  # type: ignore[attr-defined]

        svc.drop_view("cat.sch.tmp_view_abc")

        sql = ws.statement_execution.execute_statement.call_args.kwargs["statement"]  # type: ignore[attr-defined]
        assert "DROP VIEW IF EXISTS" in sql
        assert "cat.sch.tmp_view_abc" in sql

    def test_drop_view_does_not_raise_on_failure(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """drop_view is best-effort — failures should be swallowed."""
        ws.statement_execution.execute_statement.return_value = _failed_response("not found")  # type: ignore[attr-defined]

        # Should not raise
        svc.drop_view("cat.sch.tmp_view_gone")

    # ---------- create_view: FQN validation ----------

    def test_create_view_rejects_invalid_fqn(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """create_view should raise ValueError for a malformed FQN."""
        with pytest.raises(ValueError, match="Invalid fully qualified name"):
            svc.create_view("not_a_valid_fqn")

    def test_create_view_quotes_identifiers(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """create_view should backtick-quote both the source table and the view name in SQL."""
        ws.statement_execution.execute_statement.return_value = _ok_response()  # type: ignore[attr-defined]

        svc.create_view("cat.sch.src_table")

        calls = ws.statement_execution.execute_statement.call_args_list  # type: ignore[attr-defined]
        sql_stmts = [c.kwargs["statement"] for c in calls]
        create_stmts = [s for s in sql_stmts if "CREATE OR REPLACE VIEW" in s]
        assert len(create_stmts) == 1
        assert "`cat`.`sch`.`src_table`" in create_stmts[0]

    # ---------- create_view_from_sql: happy path ----------

    def test_create_view_from_sql_returns_fqn(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """create_view_from_sql with a safe query should return a fully qualified view name."""
        ws.statement_execution.execute_statement.return_value = _ok_response()  # type: ignore[attr-defined]

        result = svc.create_view_from_sql("SELECT id, name FROM cat.sch.src_table WHERE id > 0")

        assert result.startswith("cat.sch.tmp_view_")

    def test_create_view_from_sql_embeds_query_in_ddl(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """create_view_from_sql should embed the user query in CREATE OR REPLACE VIEW ... AS."""
        ws.statement_execution.execute_statement.return_value = _ok_response()  # type: ignore[attr-defined]
        query = "SELECT id FROM cat.sch.src_table"

        svc.create_view_from_sql(query)

        calls = ws.statement_execution.execute_statement.call_args_list  # type: ignore[attr-defined]
        sql_stmts = [c.kwargs["statement"] for c in calls]
        create_stmts = [s for s in sql_stmts if "CREATE OR REPLACE VIEW" in s]
        assert len(create_stmts) == 1
        assert query in create_stmts[0]

    # ---------- create_view_from_sql: is_sql_query_safe gate ----------

    def test_create_view_from_sql_rejects_multiple_statements(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """Queries containing DDL like DROP should be rejected."""
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        with pytest.raises(UnsafeSqlQueryError):
            svc.create_view_from_sql("SELECT 1; DROP TABLE cat.sch.important")

    def test_create_view_from_sql_rejects_ddl(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """Queries containing standalone DDL keywords should be rejected."""
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        for ddl in [
            "DROP TABLE cat.sch.t",
            "DELETE FROM cat.sch.t WHERE 1=1",
            "INSERT INTO cat.sch.t VALUES (1)",
            "UPDATE cat.sch.t SET x=1",
            "TRUNCATE TABLE cat.sch.t",
            "ALTER TABLE cat.sch.t ADD COLUMNS (x INT)",
            "GRANT SELECT ON cat.sch.t TO `user`",
        ]:
            with pytest.raises(UnsafeSqlQueryError, match="prohibited"):
                svc.create_view_from_sql(ddl)

    def test_create_view_from_sql_rejects_comment_escaped_ddl(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """Queries with forbidden keywords inside comments should still be rejected."""
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        with pytest.raises(UnsafeSqlQueryError):
            svc.create_view_from_sql("SELECT 1 /* delete trick */")

    def test_create_view_from_sql_does_not_execute_on_unsafe(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """When is_sql_query_safe rejects the query, no SQL should be executed."""
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        with pytest.raises(UnsafeSqlQueryError):
            svc.create_view_from_sql("SELECT 1; DROP TABLE cat.sch.important")

        ws.statement_execution.execute_statement.assert_not_called()  # type: ignore[attr-defined]

    def test_create_view_from_sql_accepts_safe_select_with_cte(self, svc: ViewService, ws: WorkspaceClient) -> None:
        """A CTE-based SELECT without forbidden keywords should be accepted."""
        ws.statement_execution.execute_statement.return_value = _ok_response()  # type: ignore[attr-defined]

        result = svc.create_view_from_sql(
            "WITH base AS (SELECT id, name FROM cat.sch.t) SELECT * FROM base WHERE id > 0"
        )

        assert result.startswith("cat.sch.tmp_view_")


# ============================================================================
# Tests for DiscoveryService (sync methods)
# ============================================================================


class TestDiscoveryService:
    """Unit tests for DiscoveryService synchronous methods."""

    @pytest.fixture
    def ws(self) -> WorkspaceClient:
        ws = create_autospec(WorkspaceClient)
        return ws

    @pytest.fixture
    def svc(self, ws: WorkspaceClient) -> DiscoveryService:
        return DiscoveryService(ws=ws, user_id="alice@example.com")

    def test_list_catalogs_returns_catalog_info_list(self, svc: DiscoveryService, ws: WorkspaceClient) -> None:
        """list_catalogs should return whatever the SDK yields."""
        cat = CatalogInfo(name="my_catalog")
        ws.catalogs.list.return_value = iter([cat])  # type: ignore[attr-defined]

        result = svc.list_catalogs()

        assert len(result) == 1
        assert result[0].name == "my_catalog"

    def test_list_schemas_passes_catalog_name(self, svc: DiscoveryService, ws: WorkspaceClient) -> None:
        """list_schemas should pass the catalog_name to the SDK."""
        schema = SchemaInfo(name="my_schema", catalog_name="cat")
        ws.schemas.list.return_value = iter([schema])  # type: ignore[attr-defined]

        result = svc.list_schemas("cat")

        assert len(result) == 1
        ws.schemas.list.assert_called_once_with(catalog_name="cat")  # type: ignore[attr-defined]

    def test_list_tables_passes_catalog_and_schema(self, svc: DiscoveryService, ws: WorkspaceClient) -> None:
        """list_tables should pass both catalog_name and schema_name to the SDK."""
        table = TableInfo(name="my_table", catalog_name="cat", schema_name="sch")
        ws.tables.list.return_value = iter([table])  # type: ignore[attr-defined]

        result = svc.list_tables("cat", "sch")

        assert len(result) == 1
        ws.tables.list.assert_called_once_with(catalog_name="cat", schema_name="sch")  # type: ignore[attr-defined]

    def test_get_table_columns_returns_table_columns(self, svc: DiscoveryService, ws: WorkspaceClient) -> None:
        """get_table_columns should convert SDK ColumnInfo into TableColumn objects."""
        col = CatalogColumnInfo(name="id", type_name=None, comment="pk", nullable=False, position=0)
        mock_table = create_autospec(TableInfo)
        mock_table.columns = [col]
        ws.tables.get.return_value = mock_table  # type: ignore[attr-defined]

        result = svc.get_table_columns("cat", "sch", "tbl")

        assert len(result) == 1
        assert result[0].name == "id"
        assert result[0].comment == "pk"
        assert result[0].nullable is False
        ws.tables.get.assert_called_once_with(full_name="cat.sch.tbl")  # type: ignore[attr-defined]

    def test_get_table_columns_returns_empty_when_no_columns(self, svc: DiscoveryService, ws: WorkspaceClient) -> None:
        """get_table_columns should return [] when the table has no column metadata."""
        mock_table = create_autospec(TableInfo)
        mock_table.columns = None
        ws.tables.get.return_value = mock_table  # type: ignore[attr-defined]

        result = svc.get_table_columns("cat", "sch", "tbl")

        assert result == []

    def test_user_id_is_stored(self, svc: DiscoveryService) -> None:
        """user_id should be accessible for use as cache key component."""
        assert svc.user_id == "alice@example.com"


# ============================================================================
# Tests for MigrationRunner
# ============================================================================


class TestMigrationRunner:
    """Unit tests for MigrationRunner with mocked WorkspaceClient."""

    @pytest.fixture
    def ws(self) -> WorkspaceClient:
        ws = create_autospec(WorkspaceClient)
        return ws

    def test_run_all_returns_zero_when_all_already_applied(self, ws: WorkspaceClient) -> None:
        """run_all should skip all migrations when every version is already recorded."""
        all_applied_rows = [[str(m.version), "2025-01-01T00:00:00"] for m in MIGRATIONS]
        ws.statement_execution.execute_statement.side_effect = [  # type: ignore[attr-defined]
            _ok_response(),  # _ensure_schema (bootstrap)
            _ok_response(),  # _ensure_meta_table
            _ok_response(all_applied_rows),  # _applied_at_map query
        ]

        runner = MigrationRunner(ws=ws, warehouse_id="wh", catalog="cat", schema="sch")
        count = runner.run_all()

        assert count == 0

    def test_run_all_applies_all_when_none_applied(self, ws: WorkspaceClient) -> None:
        """run_all should apply every migration when none are recorded yet."""
        # schema + meta table + applied_at_map query + 2 calls per migration (DDL + INSERT)
        total_responses = 3 + 2 * len(MIGRATIONS)
        ws.statement_execution.execute_statement.side_effect = [_ok_response()] * total_responses  # type: ignore[attr-defined]

        runner = MigrationRunner(ws=ws, warehouse_id="wh", catalog="cat", schema="sch")
        count = runner.run_all()

        assert count == len(MIGRATIONS)

    def test_run_all_applies_only_pending_migrations(self, ws: WorkspaceClient) -> None:
        """run_all should only apply migrations that are not yet recorded."""
        n_already_applied = 3
        applied_rows = [[str(m.version), "2025-01-01T00:00:00"] for m in MIGRATIONS[:n_already_applied]]
        pending = len(MIGRATIONS) - n_already_applied
        # schema + meta table + applied_at query + 2 calls per pending migration (DDL + INSERT)
        ws.statement_execution.execute_statement.side_effect = (  # type: ignore[attr-defined]
            [_ok_response(), _ok_response(), _ok_response(applied_rows)] + [_ok_response()] * (2 * pending)
        )

        runner = MigrationRunner(ws=ws, warehouse_id="wh", catalog="cat", schema="sch")
        count = runner.run_all()

        assert count == pending

    def test_run_all_raises_on_sql_failure(self, ws: WorkspaceClient) -> None:
        """run_all should propagate RuntimeError on the first SQL failure."""
        ws.statement_execution.execute_statement.return_value = _failed_response("permission denied")  # type: ignore[attr-defined]

        runner = MigrationRunner(ws=ws, warehouse_id="wh", catalog="cat", schema="sch")
        with pytest.raises(RuntimeError, match="Migration SQL failed"):
            runner.run_all()

    def test_status_returns_all_migrations_with_applied_flag(self, ws: WorkspaceClient) -> None:
        """status should list every migration with correct applied/pending flags."""
        n_applied = 2
        applied_rows = [[str(m.version), "2025-01-01T00:00:00"] for m in MIGRATIONS[:n_applied]]
        ws.statement_execution.execute_statement.side_effect = [  # type: ignore[attr-defined]
            _ok_response(),  # _ensure_schema
            _ok_response(),  # _ensure_meta_table
            _ok_response(applied_rows),  # _applied_at_map
        ]

        runner = MigrationRunner(ws=ws, warehouse_id="wh", catalog="cat", schema="sch")
        statuses = runner.status()

        assert len(statuses) == len(MIGRATIONS)
        applied = [s for s in statuses if s["applied"]]
        pending = [s for s in statuses if not s["applied"]]
        assert len(applied) == n_applied
        assert len(pending) == len(MIGRATIONS) - n_applied
        assert statuses[0]["version"] == 1


# ============================================================================
# Tests for profiler routes
# ============================================================================


class TestProfilerRoutes:
    """Unit tests for profiler route handlers."""

    @pytest.fixture
    def mock_job_svc(self) -> JobService:
        svc = create_autospec(JobService)
        return svc

    @pytest.fixture
    def mock_view_svc(self) -> ViewService:
        svc = create_autospec(ViewService)
        return svc

    @pytest.fixture
    def mock_obo_ws(self) -> WorkspaceClient:
        ws = create_autospec(WorkspaceClient)
        mock_user = create_autospec(User)
        mock_user.user_name = "alice@example.com"
        ws.current_user.me.return_value = mock_user
        return ws

    def test_list_profile_runs_returns_summaries(self, mock_job_svc: JobService) -> None:
        """list_profile_runs should return ProfileRunSummaryOut entries from job service rows."""
        mock_job_svc.list_run_rows.return_value = [  # type: ignore[attr-defined]
            {
                "run_id": "abc123",
                "source_table_fqn": "cat.sch.tbl",
                "status": "SUCCEEDED",
                "rows_profiled": "1000",
                "columns_profiled": "5",
                "duration_seconds": "3.14",
                "requesting_user": "alice@example.com",
                "created_at": "2025-01-01T00:00:00",
            }
        ]

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        result = list_profile_runs(job_svc=mock_job_svc, app_conf=app_conf)

        assert len(result) == 1
        assert result[0].run_id == "abc123"
        assert result[0].source_table_fqn == "cat.sch.tbl"
        assert result[0].rows_profiled == 1000
        assert result[0].columns_profiled == 5

    def test_list_profile_runs_returns_empty_list(self, mock_job_svc: JobService) -> None:
        """list_profile_runs should return [] when no runs exist."""
        mock_job_svc.list_run_rows.return_value = []  # type: ignore[attr-defined]

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        result = list_profile_runs(job_svc=mock_job_svc, app_conf=app_conf)

        assert result == []

    def test_list_profile_runs_raises_500_on_error(self, mock_job_svc: JobService) -> None:
        """list_profile_runs should raise HTTP 500 when an unexpected error occurs."""
        mock_job_svc.list_run_rows.side_effect = RuntimeError("db error")  # type: ignore[attr-defined]

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        with pytest.raises(HTTPException) as exc:
            list_profile_runs(job_svc=mock_job_svc, app_conf=app_conf)

        assert exc.value.status_code == 500

    def test_submit_profile_run_creates_view_and_submits_job(
        self,
        mock_job_svc: JobService,
        mock_view_svc: ViewService,
        mock_obo_ws: WorkspaceClient,
    ) -> None:
        """submit_profile_run should create a view and submit a job, returning run ids."""
        mock_view_svc.create_view.return_value = "cat.sch.tmp_view_xyz"  # type: ignore[attr-defined]
        mock_job_svc.submit_run.return_value = 77777  # type: ignore[attr-defined]
        body = ProfileRunIn(table_fqn="cat.sch.src_table", sample_limit=5000)

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        result = submit_profile_run(
            body=body,
            obo_ws=mock_obo_ws,
            view_svc=mock_view_svc,
            job_svc=mock_job_svc,
            app_conf=app_conf,
        )

        assert isinstance(result, ProfileRunOut)
        assert result.job_run_id == 77777
        assert len(result.run_id) > 0
        mock_view_svc.create_view.assert_called_once_with("cat.sch.src_table", sample_limit=5000)  # type: ignore[attr-defined]

    def test_submit_profile_run_raises_500_on_error(
        self,
        mock_job_svc: JobService,
        mock_view_svc: ViewService,
        mock_obo_ws: WorkspaceClient,
    ) -> None:
        """submit_profile_run should raise HTTP 500 when view creation fails."""
        mock_view_svc.create_view.side_effect = RuntimeError("warehouse down")  # type: ignore[attr-defined]
        body = ProfileRunIn(table_fqn="cat.sch.src_table")

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        with pytest.raises(HTTPException) as exc:
            submit_profile_run(
                body=body,
                obo_ws=mock_obo_ws,
                view_svc=mock_view_svc,
                job_svc=mock_job_svc,
                app_conf=app_conf,
            )

        assert exc.value.status_code == 500

    def test_get_profile_run_status_returns_status_out(
        self, mock_job_svc: JobService, mock_view_svc: ViewService
    ) -> None:
        """get_profile_run_status should map JobService.get_run_status to RunStatusOut."""
        mock_job_svc.get_run_status.return_value = RunStatus(  # type: ignore[attr-defined]
            state="TERMINATED", result_state="SUCCESS", message=None
        )

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        result = get_profile_run_status(
            run_id="run-001",
            job_run_id=99999,
            job_svc=mock_job_svc,
            view_svc=mock_view_svc,
            app_conf=app_conf,
        )

        assert isinstance(result, RunStatusOut)
        assert result.run_id == "run-001"
        assert result.state == "TERMINATED"
        assert result.result_state == "SUCCESS"

    def test_get_profile_run_status_raises_500_on_error(
        self, mock_job_svc: JobService, mock_view_svc: ViewService
    ) -> None:
        """get_profile_run_status should raise HTTP 500 when the job service errors."""
        mock_job_svc.get_run_status.side_effect = RuntimeError("jobs api error")  # type: ignore[attr-defined]

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        with pytest.raises(HTTPException) as exc:
            get_profile_run_status(
                run_id="run-001",
                job_run_id=99999,
                job_svc=mock_job_svc,
                view_svc=mock_view_svc,
                app_conf=app_conf,
            )

        assert exc.value.status_code == 500

    def test_get_profile_run_results_returns_results(self, mock_job_svc: JobService) -> None:
        """get_profile_run_results should parse result row from the Delta table."""
        mock_job_svc.get_run_result_row.return_value = {  # type: ignore[attr-defined]
            "run_id": "run-001",
            "source_table_fqn": "cat.sch.tbl",
            "rows_profiled": "1000",
            "columns_profiled": "5",
            "duration_seconds": "3.14",
            "summary_json": json.dumps({"col": "stats"}),
            "generated_rules_json": json.dumps([{"check": {"function": "is_not_null"}}]),
            "status": "SUCCEEDED",
        }

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        result = get_profile_run_results(run_id="run-001", job_svc=mock_job_svc, app_conf=app_conf)

        assert isinstance(result, ProfileResultsOut)
        assert result.rows_profiled == 1000
        assert len(result.generated_rules) == 1

    def test_get_profile_run_results_raises_404_when_not_found(self, mock_job_svc: JobService) -> None:
        """get_profile_run_results should raise HTTP 404 when no result row exists."""
        mock_job_svc.get_run_result_row.return_value = None  # type: ignore[attr-defined]

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        with pytest.raises(HTTPException) as exc:
            get_profile_run_results(run_id="run-missing", job_svc=mock_job_svc, app_conf=app_conf)

        assert exc.value.status_code == 404

    def test_get_profile_run_results_raises_500_on_failed_status(self, mock_job_svc: JobService) -> None:
        """get_profile_run_results should raise HTTP 500 when the run status is FAILED."""
        mock_job_svc.get_run_result_row.return_value = {  # type: ignore[attr-defined]
            "run_id": "run-001",
            "source_table_fqn": "cat.sch.tbl",
            "status": "FAILED",
            "error_message": "OOM error",
        }

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        with pytest.raises(HTTPException) as exc:
            get_profile_run_results(run_id="run-001", job_svc=mock_job_svc, app_conf=app_conf)

        assert exc.value.status_code == 500


# ============================================================================
# Tests for dry-run routes (job-based)
# ============================================================================


class TestDryRunRoutes:
    """Unit tests for the job-based dry-run route handlers."""

    @pytest.fixture
    def mock_job_svc(self) -> JobService:
        svc = create_autospec(JobService)
        return svc

    @pytest.fixture
    def mock_view_svc(self) -> ViewService:
        svc = create_autospec(ViewService)
        return svc

    @pytest.fixture
    def mock_obo_ws(self) -> WorkspaceClient:
        ws = create_autospec(WorkspaceClient)
        mock_user = create_autospec(User)
        mock_user.user_name = "alice@example.com"
        ws.current_user.me.return_value = mock_user
        return ws

    def test_submit_dry_run_success(
        self,
        mock_job_svc: JobService,
        mock_view_svc: ViewService,
        mock_obo_ws: WorkspaceClient,
    ) -> None:
        """submit_dry_run should validate checks, create view, submit job and return run ids."""
        validation = ChecksValidationStatus()
        mock_view_svc.create_view.return_value = "cat.sch.tmp_view_abc"  # type: ignore[attr-defined]
        mock_job_svc.submit_run.return_value = 88888  # type: ignore[attr-defined]
        body = DryRunIn(table_fqn="cat.sch.tbl", checks=_SAMPLE_CHECKS)

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        result = submit_dry_run(
            body=body,
            obo_ws=mock_obo_ws,
            view_svc=mock_view_svc,
            job_svc=mock_job_svc,
            validate_checks_fn=lambda checks: validation,
            app_conf=app_conf,
        )

        assert isinstance(result, DryRunSubmitOut)
        assert result.job_run_id == 88888
        assert len(result.run_id) > 0
        mock_view_svc.create_view.assert_called_once_with("cat.sch.tbl")  # type: ignore[attr-defined]

    def test_submit_dry_run_raises_400_on_invalid_checks(
        self,
        mock_job_svc: JobService,
        mock_view_svc: ViewService,
        mock_obo_ws: WorkspaceClient,
    ) -> None:
        """submit_dry_run should raise HTTP 400 when check validation reports errors."""
        validation = ChecksValidationStatus(_errors=["Unknown function: bad_func"])
        body = DryRunIn(table_fqn="cat.sch.tbl", checks=_SAMPLE_CHECKS)

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        with pytest.raises(HTTPException) as exc:
            submit_dry_run(
                body=body,
                obo_ws=mock_obo_ws,
                view_svc=mock_view_svc,
                job_svc=mock_job_svc,
                validate_checks_fn=lambda checks: validation,
                app_conf=app_conf,
            )

        assert exc.value.status_code == 400

    def test_submit_dry_run_raises_500_on_view_error(
        self,
        mock_job_svc: JobService,
        mock_view_svc: ViewService,
        mock_obo_ws: WorkspaceClient,
    ) -> None:
        """submit_dry_run should raise HTTP 500 when view creation fails."""
        validation = ChecksValidationStatus()
        mock_view_svc.create_view.side_effect = RuntimeError("warehouse unreachable")  # type: ignore[attr-defined]
        body = DryRunIn(table_fqn="cat.sch.tbl", checks=_SAMPLE_CHECKS)

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        with pytest.raises(HTTPException) as exc:
            submit_dry_run(
                body=body,
                obo_ws=mock_obo_ws,
                view_svc=mock_view_svc,
                job_svc=mock_job_svc,
                validate_checks_fn=lambda checks: validation,
                app_conf=app_conf,
            )

        assert exc.value.status_code == 500

    def test_get_dry_run_status_returns_status_out(self, mock_job_svc: JobService, mock_view_svc: ViewService) -> None:
        """get_dry_run_status should map JobService status to RunStatusOut."""
        mock_job_svc.get_run_status.return_value = RunStatus(  # type: ignore[attr-defined]
            state="RUNNING", result_state=None, message="Executing"
        )

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        result = get_dry_run_status(
            run_id="run-001",
            job_run_id=88888,
            job_svc=mock_job_svc,
            view_svc=mock_view_svc,
            app_conf=app_conf,
        )

        assert isinstance(result, RunStatusOut)
        assert result.state == "RUNNING"
        assert result.message == "Executing"

    def test_get_dry_run_status_raises_500_on_error(self, mock_job_svc: JobService, mock_view_svc: ViewService) -> None:
        """get_dry_run_status should raise HTTP 500 when the job service errors."""
        mock_job_svc.get_run_status.side_effect = RuntimeError("api error")  # type: ignore[attr-defined]

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        with pytest.raises(HTTPException) as exc:
            get_dry_run_status(
                run_id="run-001",
                job_run_id=88888,
                job_svc=mock_job_svc,
                view_svc=mock_view_svc,
                app_conf=app_conf,
            )

        assert exc.value.status_code == 500

    def test_get_dry_run_results_returns_results(self, mock_job_svc: JobService) -> None:
        """get_dry_run_results should parse result row from the Delta table."""
        mock_job_svc.get_run_result_row.return_value = {  # type: ignore[attr-defined]
            "run_id": "run-001",
            "source_table_fqn": "cat.sch.tbl",
            "total_rows": "500",
            "valid_rows": "480",
            "invalid_rows": "20",
            "error_summary_json": json.dumps([{"error": "null", "count": 20}]),
            "sample_invalid_json": json.dumps([{"id": None}]),
            "status": "SUCCEEDED",
        }

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        result = get_dry_run_results(run_id="run-001", job_svc=mock_job_svc, app_conf=app_conf)

        assert isinstance(result, DryRunResultsOut)
        assert result.total_rows == 500
        assert result.valid_rows == 480
        assert result.invalid_rows == 20
        assert len(result.error_summary) == 1

    def test_get_dry_run_results_raises_404_when_not_found(self, mock_job_svc: JobService) -> None:
        """get_dry_run_results should raise HTTP 404 when no result row exists."""
        mock_job_svc.get_run_result_row.return_value = None  # type: ignore[attr-defined]

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        with pytest.raises(HTTPException) as exc:
            get_dry_run_results(run_id="run-missing", job_svc=mock_job_svc, app_conf=app_conf)

        assert exc.value.status_code == 404

    def test_get_dry_run_results_raises_500_on_failed_status(self, mock_job_svc: JobService) -> None:
        """get_dry_run_results should raise HTTP 500 when the run status is FAILED."""
        mock_job_svc.get_run_result_row.return_value = {  # type: ignore[attr-defined]
            "run_id": "run-001",
            "source_table_fqn": "cat.sch.tbl",
            "status": "FAILED",
            "error_message": "Spark OOM",
        }

        app_conf = AppConfig(catalog="cat", schema_name="sch", job_id="")
        with pytest.raises(HTTPException) as exc:
            get_dry_run_results(run_id="run-001", job_svc=mock_job_svc, app_conf=app_conf)

        assert exc.value.status_code == 500

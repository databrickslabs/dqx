"""Unit tests for ``QuarantineSampleService`` — the two live per-request
permission checks that gate the row-level failing-sample endpoint.

Both checks run with the *requesting user's* OBO credentials (never the
service principal) and both fail closed: any error reads as "no access" /
"fine-grained controls present" respectively.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, ColumnMask, TableInfo, TableRowFilter

from databricks_labs_dqx_app.backend.services.quarantine_sample_service import (
    QuarantineSampleService,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

FQN = "cat.schema.tbl"


def make_obo_ws() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


class TestHasFineGrainedAccessControl:
    def test_true_when_row_filter_present(self):
        ws = make_obo_ws()
        ws.tables.get.return_value = TableInfo(
            row_filter=TableRowFilter(function_name="cat.schema.filter_fn", input_column_names=["region"])
        )
        assert QuarantineSampleService.has_fine_grained_access_control(ws, FQN) is True

    def test_true_when_column_mask_present(self):
        ws = make_obo_ws()
        ws.tables.get.return_value = TableInfo(
            row_filter=None,
            columns=[
                ColumnInfo(name="id"),
                ColumnInfo(name="ssn", mask=ColumnMask(function_name="cat.schema.mask_fn")),
            ],
        )
        assert QuarantineSampleService.has_fine_grained_access_control(ws, FQN) is True

    def test_false_when_neither_present(self):
        ws = make_obo_ws()
        ws.tables.get.return_value = TableInfo(row_filter=None, columns=[ColumnInfo(name="id")])
        assert QuarantineSampleService.has_fine_grained_access_control(ws, FQN) is False

    def test_false_when_columns_missing(self):
        # tables.get can omit the columns list entirely.
        ws = make_obo_ws()
        ws.tables.get.return_value = TableInfo(row_filter=None, columns=None)
        assert QuarantineSampleService.has_fine_grained_access_control(ws, FQN) is False

    def test_fails_closed_when_metadata_read_fails(self):
        # If we cannot verify the absence of fine-grained controls we must
        # report them as present, so the caller suppresses the sample.
        ws = make_obo_ws()
        ws.tables.get.side_effect = RuntimeError("transient UC failure")
        assert QuarantineSampleService.has_fine_grained_access_control(ws, FQN) is True

    def test_queries_the_requested_table(self):
        ws = make_obo_ws()
        ws.tables.get.return_value = TableInfo(row_filter=None)
        QuarantineSampleService.has_fine_grained_access_control(ws, FQN)
        ws.tables.get.assert_called_once_with(FQN)

    def test_rejects_malformed_fqn_before_any_call(self):
        ws = make_obo_ws()
        with pytest.raises(ValueError):
            QuarantineSampleService.has_fine_grained_access_control(ws, "not-a-three-part-name")
        ws.tables.get.assert_not_called()


class TestUserCanSelect:
    def test_true_when_obo_probe_succeeds(self):
        obo_sql = create_autospec(SqlExecutor, instance=True)
        obo_sql.query.return_value = []
        assert QuarantineSampleService.user_can_select(obo_sql, FQN) is True

    def test_false_when_obo_probe_raises(self):
        obo_sql = create_autospec(SqlExecutor, instance=True)
        obo_sql.query.side_effect = RuntimeError("PERMISSION_DENIED: user lacks SELECT")
        assert QuarantineSampleService.user_can_select(obo_sql, FQN) is False

    def test_probe_is_zero_row_and_targets_quoted_table(self):
        # The self-check must be cheap (no data returned) and must embed the
        # identifier backtick-quoted so exotic names can't break out.
        obo_sql = create_autospec(SqlExecutor, instance=True)
        obo_sql.query.return_value = []
        QuarantineSampleService.user_can_select(obo_sql, FQN)
        stmt = obo_sql.query.call_args[0][0]
        assert "`cat`.`schema`.`tbl`" in stmt
        assert "LIMIT 0" in stmt

    def test_rejects_malformed_fqn_before_any_query(self):
        obo_sql = create_autospec(SqlExecutor, instance=True)
        with pytest.raises(ValueError):
            QuarantineSampleService.user_can_select(obo_sql, "nope")
        obo_sql.query.assert_not_called()

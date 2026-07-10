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

from databricks_labs_dqx_app.backend.models import FailedRowFailureOut, FailingRecordFailureOut
from databricks_labs_dqx_app.backend.services.dq_results_service import CheckAttribution
from databricks_labs_dqx_app.backend.services.quarantine_sample_service import (
    QuarantineSampleService,
    enrich_failures,
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


# ---------------------------------------------------------------------------
# Failure enrichment + server-side filters (dq-results failed-rows path)
# ---------------------------------------------------------------------------


def failure(
    rule_name: str | None = "c1",
    quality_dimension: str | None = None,
    severity: str | None = None,
    columns: list[str] | None = None,
) -> FailedRowFailureOut:
    return FailedRowFailureOut(
        rule_name=rule_name,
        quality_dimension=quality_dimension,
        severity=severity,
        message="m",
        columns=columns or [],
    )


class TestEnrichFailures:
    def test_joins_rule_metadata_by_check_name(self):
        raw = [FailingRecordFailureOut(rule_name="c1", message="boom", columns=["id"])]
        attribution = {
            "c1": CheckAttribution(
                rule_id="rule-1",
                rule_name="c1",
                severity="High",
                dimension="Completeness",
                columns=("id",),
            )
        }
        out = enrich_failures(raw, attribution)
        assert out == [
            FailedRowFailureOut(
                rule_id="rule-1",
                rule_name="c1",
                quality_dimension="Completeness",
                severity="High",
                message="boom",
                columns=["id"],
            )
        ]

    def test_unattributed_failures_keep_null_metadata(self):
        raw = [FailingRecordFailureOut(rule_name="adhoc", message="boom", columns=[])]
        out = enrich_failures(raw, {})
        assert out[0].rule_id is None
        assert out[0].quality_dimension is None
        assert out[0].severity is None
        assert out[0].rule_name == "adhoc"

    def test_nameless_failures_are_not_looked_up(self):
        raw = [FailingRecordFailureOut(rule_name=None, message="boom", columns=[])]
        out = enrich_failures(raw, {"c1": CheckAttribution(rule_id="rule-1", rule_name="c1")})
        assert out[0].rule_id is None


class TestRowMatchesFilters:
    """dqlake predicate parity: exists() per facet, OR within a facet,
    AND across facets; column facet is a membership test over
    failed_columns."""

    def test_no_filters_matches_everything(self):
        assert QuarantineSampleService.row_matches_filters([failure()], []) is True

    def test_rule_filter_matches_any_failure(self):
        failures = [failure(rule_name="c1"), failure(rule_name="c2")]
        assert QuarantineSampleService.row_matches_filters(failures, [], rules=("c2",)) is True
        assert QuarantineSampleService.row_matches_filters(failures, [], rules=("c3",)) is False

    def test_severity_filter(self):
        failures = [failure(severity="High")]
        assert QuarantineSampleService.row_matches_filters(failures, [], severities=("High",)) is True
        assert QuarantineSampleService.row_matches_filters(failures, [], severities=("Low",)) is False

    def test_dimension_filter(self):
        failures = [failure(quality_dimension="Validity")]
        assert QuarantineSampleService.row_matches_filters(failures, [], dimensions=("Validity",)) is True
        assert (
            QuarantineSampleService.row_matches_filters(failures, [], dimensions=("Completeness",)) is False
        )

    def test_column_filter_is_membership_over_failed_columns(self):
        assert QuarantineSampleService.row_matches_filters([failure()], ["id"], columns=("id",)) is True
        assert QuarantineSampleService.row_matches_filters([failure()], ["id"], columns=("amount",)) is False

    def test_or_within_a_facet(self):
        failures = [failure(rule_name="c1")]
        assert QuarantineSampleService.row_matches_filters(failures, [], rules=("c1", "c9")) is True

    def test_and_across_facets(self):
        # Facets may be satisfied by DIFFERENT failures on the same row
        # (dqlake parity: one exists() predicate per facet).
        failures = [failure(rule_name="c1", severity="High"), failure(rule_name="c2", severity="Low")]
        assert (
            QuarantineSampleService.row_matches_filters(failures, [], rules=("c1",), severities=("Low",))
            is True
        )
        assert (
            QuarantineSampleService.row_matches_filters(failures, [], rules=("c1",), severities=("Med",))
            is False
        )

    def test_untagged_failures_never_match_active_metadata_facets(self):
        failures = [failure(severity=None, quality_dimension=None)]
        assert QuarantineSampleService.row_matches_filters(failures, [], severities=("High",)) is False
        assert QuarantineSampleService.row_matches_filters(failures, [], dimensions=("Validity",)) is False

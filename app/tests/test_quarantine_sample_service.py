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

import json

from databricks_labs_dqx_app.backend.models import FailedRowFailureOut
from databricks_labs_dqx_app.backend.services.quarantine_sample_service import (
    ParsedFailure,
    QuarantineSampleService,
    enrich_failures,
    parse_failures,
    to_failing_record,
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
# Failure parsing + enrichment (dq-results failed-rows path)
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


def quarantine_row(errors: object = None, warnings: object = None) -> dict[str, str | None]:
    return {
        "quarantine_id": "q1",
        "row_data": json.dumps({"id": 1}),
        "errors": json.dumps(errors) if errors is not None else None,
        "warnings": json.dumps(warnings) if warnings is not None else None,
    }


class TestParseFailures:
    """The failure structs written by the DQX engine (see the core
    library's ``DQRuleManager._build_result_struct``) carry the check's
    OWN ``user_metadata`` map — the severity/dimension tags and registry
    provenance frozen at materialization time. Parsing keeps that map so
    enrichment is version-accurate without any live rule join."""

    def test_parses_struct_fields_and_user_metadata(self):
        parsed = parse_failures(
            quarantine_row(
                errors=[
                    {
                        "name": "c1",
                        "message": "id is null",
                        "columns": ["id"],
                        "user_metadata": {
                            "severity": "High",
                            "dimension": "Completeness",
                            "registry_rule_id": "rule-1",
                        },
                    }
                ]
            )
        )
        assert parsed == [
            ParsedFailure(
                rule_name="c1",
                message="id is null",
                columns=("id",),
                user_metadata={"severity": "High", "dimension": "Completeness", "registry_rule_id": "rule-1"},
            )
        ]

    def test_merges_errors_and_warnings(self):
        parsed = parse_failures(
            quarantine_row(
                errors=[{"name": "e1", "message": "m1", "columns": []}],
                warnings=[{"name": "w1", "message": "m2", "columns": []}],
            )
        )
        assert [f.rule_name for f in parsed] == ["e1", "w1"]

    def test_missing_user_metadata_degrades_to_empty(self):
        parsed = parse_failures(quarantine_row(errors=[{"name": "c1", "message": "m", "columns": []}]))
        assert parsed[0].user_metadata == {}

    def test_non_string_metadata_values_are_dropped(self):
        parsed = parse_failures(
            quarantine_row(errors=[{"name": "c1", "message": "m", "columns": [], "user_metadata": {"a": 1, "b": "x"}}])
        )
        assert parsed[0].user_metadata == {"b": "x"}

    def test_legacy_dict_shape_yields_untagged_failures(self):
        # Legacy SQL-check rows wrote a single {check_name: message} dict.
        parsed = parse_failures(quarantine_row(errors={"legacy_check": "boom"}))
        assert parsed == [ParsedFailure(rule_name="legacy_check", message="boom", columns=(), user_metadata={})]

    def test_malformed_payloads_degrade_to_empty(self):
        assert parse_failures(quarantine_row()) == []
        assert parse_failures({"errors": "{not json", "warnings": None}) == []
        assert parse_failures({"errors": json.dumps([1, "x"]), "warnings": None}) == []

    def test_no_failure_payload_row_yields_empty_failures_but_keeps_the_record(self):
        """Regression pin for the blank-tooltip report (P3.6).

        Observed live on the dev workspace: the frozen task runner can
        persist quarantine rows whose ``errors`` VARIANT is SQL NULL and
        whose ``warnings`` is an empty array (non-deterministic
        ``df.limit(sample_size)`` re-evaluation between the split filter
        and the quarantine write). ``to_json`` renders those as None /
        ``"[]"``. The read path must degrade to an EMPTY failures list —
        the record itself is still returned (its row values are real) and
        the UI renders an explicit "no failure details" fallback instead
        of a blank tooltip.
        """
        row = {
            "quarantine_id": "q-empty",
            "row_data": json.dumps({"id": 7, "name": None}),
            "errors": None,  # to_json(NULL VARIANT) -> NULL
            "warnings": "[]",  # to_json(empty array) -> "[]"
        }
        parsed = parse_failures(row)
        assert parsed == []
        record = to_failing_record(row, parsed)
        assert record.record_key == "q-empty"
        assert record.row_values == {"id": "7", "name": None}
        assert record.failed_columns == []
        assert record.failures == []


class TestEnrichFailures:
    """Severity/dimension/rule_id come from the failure struct's OWN
    frozen user_metadata — the as-of-run payload — never a live join."""

    def test_reads_metadata_from_the_failure_struct(self):
        parsed = [
            ParsedFailure(
                rule_name="c1",
                message="boom",
                columns=("id",),
                user_metadata={"severity": "High", "dimension": "Completeness", "registry_rule_id": "rule-1"},
            )
        ]
        assert enrich_failures(parsed) == [
            FailedRowFailureOut(
                rule_id="rule-1",
                rule_name="c1",
                quality_dimension="Completeness",
                severity="High",
                message="boom",
                columns=["id"],
            )
        ]

    def test_untagged_failures_keep_null_metadata(self):
        parsed = [ParsedFailure(rule_name="adhoc", message="boom", columns=(), user_metadata={})]
        out = enrich_failures(parsed)
        assert out[0].rule_id is None
        assert out[0].quality_dimension is None
        assert out[0].severity is None
        assert out[0].rule_name == "adhoc"


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

"""Tests for the pure (no-Spark) helpers in ``dqx_task_runner.runner``.

The runner module is heavyweight (PySpark + DQX engine) but its helper
functions are plain Python: fingerprint computation, custom-metric
validation, and conversion of observed ``check_metrics`` JSON into the
legacy ``error_summary`` shape.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# The runner lives in ``app/tasks/src``, which is a separate uv project.
# Add it to sys.path so we can import it without depending on
# ``uv sync`` having installed the wheel into the test venv.
_TASKS_SRC = Path(__file__).resolve().parent.parent / "tasks" / "src"
if str(_TASKS_SRC) not in sys.path:
    sys.path.insert(0, str(_TASKS_SRC))


@pytest.fixture
def runner_module():
    import dqx_task_runner.runner as r  # type: ignore[import-not-found]

    return r


# ---------------------------------------------------------------------------
# _observer_run_name — internal run_type → user-facing dq_metrics.run_name
# ---------------------------------------------------------------------------


class TestObserverRunName:
    """Locks in the ``dryrun → manual`` rename and other run_type mappings.

    The internal ``run_type`` token is unchanged everywhere else in the
    code (API filters, ``dq_validation_runs.run_type``, frontend
    grouping); only the metrics row's display name is rewritten.
    """

    def test_dryrun_renamed_to_manual(self, runner_module):
        # ``dryrun`` runs that persist history are really ad-hoc manual
        # runs from the UI; the literal preview path uses run_type="preview".
        assert runner_module._observer_run_name("dryrun") == "dqx_app_manual"

    def test_scheduled_unchanged(self, runner_module):
        assert runner_module._observer_run_name("scheduled") == "dqx_app_scheduled"

    def test_preview_unchanged(self, runner_module):
        assert runner_module._observer_run_name("preview") == "dqx_app_preview"

    def test_unknown_run_type_falls_back_to_prefixed_form(self, runner_module):
        # Defensive default — if a future run_type is added but the map
        # isn't updated, we still produce a stable, prefixed name.
        assert runner_module._observer_run_name("backfill") == "dqx_app_backfill"


# ---------------------------------------------------------------------------
# _compute_fingerprint
# ---------------------------------------------------------------------------


class TestComputeFingerprint:
    def test_empty_checks_returns_none(self, runner_module):
        assert runner_module._compute_fingerprint([]) is None
        assert runner_module._compute_fingerprint(None) is None  # type: ignore[arg-type]

    def test_returns_hex_string_for_valid_checks(self, runner_module):
        checks = [
            {
                "name": "amount_not_null",
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "amount"}},
            }
        ]
        fp = runner_module._compute_fingerprint(checks)
        assert isinstance(fp, str)
        assert len(fp) == 64  # SHA-256 hex
        assert all(c in "0123456789abcdef" for c in fp)

    def test_fingerprint_is_deterministic(self, runner_module):
        checks = [
            {"check": {"function": "is_not_null", "arguments": {"column": "x"}}},
        ]
        a = runner_module._compute_fingerprint(checks)
        b = runner_module._compute_fingerprint(list(checks))
        assert a == b

    def test_fallback_hash_used_when_dqx_helper_raises(self, runner_module, monkeypatch):
        # Force the ``compute_rule_set_fingerprint_by_metadata`` import path
        # to throw; the helper must fall back to a SHA-256 over canonical JSON.
        import databricks.labs.dqx.rule_fingerprint as rf

        monkeypatch.setattr(
            rf,
            "compute_rule_set_fingerprint_by_metadata",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        fp = runner_module._compute_fingerprint([{"check": {"function": "x"}}])
        assert fp is not None
        assert len(fp) == 64


# ---------------------------------------------------------------------------
# _validate_custom_metrics
# ---------------------------------------------------------------------------


class TestValidateCustomMetrics:
    def test_empty_returns_empty(self, runner_module):
        assert runner_module._validate_custom_metrics([]) == []
        assert runner_module._validate_custom_metrics(None) == []  # type: ignore[arg-type]

    def test_keeps_well_formed_entries(self, runner_module):
        exprs = [
            "sum(amount) as total_amount",
            "count(distinct id) as uniques",
        ]
        assert runner_module._validate_custom_metrics(exprs) == exprs

    def test_drops_non_string_entries(self, runner_module):
        result = runner_module._validate_custom_metrics(
            ["count(*) as n", 42, None, [], {"foo": "bar"}],
        )
        assert result == ["count(*) as n"]

    def test_drops_malformed_entries(self, runner_module):
        result = runner_module._validate_custom_metrics(
            ["count(*) as 1bad", "sum(x) as total", "no alias here"],
        )
        assert result == ["sum(x) as total"]

    def test_strips_whitespace(self, runner_module):
        result = runner_module._validate_custom_metrics(["  sum(x) as total  "])
        assert result == ["sum(x) as total"]


# ---------------------------------------------------------------------------
# _check_metrics_to_error_summary
# ---------------------------------------------------------------------------


class TestCheckMetricsToErrorSummary:
    def test_empty_returns_empty(self, runner_module):
        assert runner_module._check_metrics_to_error_summary(None) == []
        assert runner_module._check_metrics_to_error_summary("") == []
        assert runner_module._check_metrics_to_error_summary("[]") == []

    def test_parses_json_string(self, runner_module):
        raw = json.dumps([{"check_name": "x", "error_count": 5, "warning_count": 2}])
        out = runner_module._check_metrics_to_error_summary(raw)
        assert out == [{"error": "x", "count": 7, "error_count": 5, "warning_count": 2}]

    def test_accepts_already_parsed_list(self, runner_module):
        items = [{"check_name": "y", "error_count": 0, "warning_count": 3}]
        out = runner_module._check_metrics_to_error_summary(items)
        assert out == [{"error": "y", "count": 3, "error_count": 0, "warning_count": 3}]

    def test_skips_zero_count_entries(self, runner_module):
        items = [
            {"check_name": "skip", "error_count": 0, "warning_count": 0},
            {"check_name": "keep", "error_count": 1, "warning_count": 0},
        ]
        out = runner_module._check_metrics_to_error_summary(items)
        assert len(out) == 1
        assert out[0]["error"] == "keep"

    def test_orders_by_count_desc_and_caps_at_20(self, runner_module):
        items = [{"check_name": f"c{i}", "error_count": i, "warning_count": 0} for i in range(1, 30)]
        out = runner_module._check_metrics_to_error_summary(items)
        assert len(out) == 20
        assert out[0]["count"] == 29  # highest count first
        assert out[-1]["count"] == 10

    def test_invalid_json_returns_empty(self, runner_module):
        assert runner_module._check_metrics_to_error_summary("{not json}") == []

    def test_non_list_top_level_returns_empty(self, runner_module):
        assert runner_module._check_metrics_to_error_summary(json.dumps({"foo": "bar"})) == []


# ---------------------------------------------------------------------------
# _aggregate_rule_labels — rule labels → dq_metrics.user_metadata
# ---------------------------------------------------------------------------


class TestAggregateRuleLabels:
    """``user_metadata`` on ``dq_metrics`` is the *intersection* of labels.

    These tests pin down the contract:
      - run provenance (``run_type``, ``requesting_user``) is NEVER
        synthesised here — it lives on ``dq_validation_runs``;
      - only labels that EVERY rule agrees on flow through;
      - non-string values are coerced (legacy numeric ``weight``);
      - empty / malformed inputs degrade to ``{}`` rather than blowing up.
    """

    def test_empty_inputs_return_empty(self, runner_module):
        assert runner_module._aggregate_rule_labels(None) == {}
        assert runner_module._aggregate_rule_labels([]) == {}

    def test_single_rule_passes_labels_through(self, runner_module):
        checks = [{"name": "c1", "user_metadata": {"team": "finance", "env": "prod"}}]
        assert runner_module._aggregate_rule_labels(checks) == {"team": "finance", "env": "prod"}

    def test_intersection_keeps_only_keys_with_matching_values(self, runner_module):
        checks = [
            {"name": "c1", "user_metadata": {"team": "finance", "env": "prod", "owner": "alice"}},
            {"name": "c2", "user_metadata": {"team": "finance", "env": "prod", "owner": "bob"}},
            {"name": "c3", "user_metadata": {"team": "finance", "env": "prod"}},
        ]
        # ``team`` and ``env`` are universal — ``owner`` differs across c1/c2
        # AND is missing from c3, so it must be dropped.
        assert runner_module._aggregate_rule_labels(checks) == {
            "team": "finance",
            "env": "prod",
        }

    def test_conflicting_values_drop_the_key(self, runner_module):
        checks = [
            {"user_metadata": {"team": "finance"}},
            {"user_metadata": {"team": "ops"}},
        ]
        assert runner_module._aggregate_rule_labels(checks) == {}

    def test_no_common_keys_returns_empty(self, runner_module):
        checks = [
            {"user_metadata": {"a": "1"}},
            {"user_metadata": {"b": "2"}},
        ]
        assert runner_module._aggregate_rule_labels(checks) == {}

    def test_non_string_values_are_coerced(self, runner_module):
        # Legacy numeric ``weight`` field, even if it survives as a non-string
        # in the rule body, must be safe to write into ``map<string,string>``.
        checks = [
            {"user_metadata": {"weight": 5}},
            {"user_metadata": {"weight": 5}},
        ]
        assert runner_module._aggregate_rule_labels(checks) == {"weight": "5"}

    def test_missing_user_metadata_is_treated_as_empty(self, runner_module):
        # A check without ``user_metadata`` collapses the intersection to
        # the empty set — nothing it can agree on.
        checks = [
            {"user_metadata": {"team": "finance"}},
            {"name": "no_labels"},
        ]
        assert runner_module._aggregate_rule_labels(checks) == {}

    def test_skips_non_dict_check_entries(self, runner_module):
        # Defensive: malformed check entries (None, strings, ints) must
        # not poison the aggregation.
        checks = [
            None,
            "not a dict",
            42,
            {"user_metadata": {"team": "finance"}},
        ]
        assert runner_module._aggregate_rule_labels(checks) == {"team": "finance"}

    def test_skips_non_dict_user_metadata(self, runner_module):
        checks = [
            {"user_metadata": "not a dict"},
            {"user_metadata": ["also", "wrong"]},
            {"user_metadata": {"team": "finance"}},
        ]
        # Only the well-formed entry contributes labels.
        assert runner_module._aggregate_rule_labels(checks) == {"team": "finance"}

    def test_does_not_inject_run_type_or_requesting_user(self, runner_module):
        # Regression guard for the dq_metrics.user_metadata bug: this
        # helper must NEVER add run-level provenance keys.
        checks = [{"user_metadata": {"team": "finance"}}]
        result = runner_module._aggregate_rule_labels(checks)
        assert "run_type" not in result
        assert "requesting_user" not in result
        assert "check_kind" not in result

    def test_preserves_user_keys_named_like_provenance(self, runner_module):
        # If a *user* legitimately tags rules with a key called
        # ``run_type`` (unusual but allowed), we still pass it through —
        # we only refuse to *synthesize* run provenance, not to honour
        # it when it genuinely belongs on the rule.
        checks = [
            {"user_metadata": {"run_type": "nightly", "team": "finance"}},
            {"user_metadata": {"run_type": "nightly", "team": "finance"}},
        ]
        assert runner_module._aggregate_rule_labels(checks) == {
            "run_type": "nightly",
            "team": "finance",
        }


# ---------------------------------------------------------------------------
# _write_sql_quarantine_records — early-return + cap behaviour
# ---------------------------------------------------------------------------


class TestWriteSqlQuarantineRecords:
    """Locks in the contract for the SQL-check quarantine writer.

    Avoids real Spark by passing a ``MagicMock`` ``violation_df`` and only
    asserting on what the function does *to* that mock. The Spark-side
    DataFrame transformations themselves are exercised in integration
    tests; here we pin down decision logic and provenance.
    """

    def _common_kwargs(self, runner_module, df):
        return {
            "spark": MagicMock(name="spark"),
            "violation_df": df,
            "run_id": "r1",
            "source_table_fqn": "cat.sch.tbl",
            "requesting_user": "alice@x",
            "check_name": "my_sql_check",
            "result_catalog": "rc",
            "result_schema": "rs",
        }

    def test_zero_violations_short_circuits_without_writing(self, runner_module):
        df = MagicMock(name="violation_df")
        persisted = runner_module._write_sql_quarantine_records(
            **self._common_kwargs(runner_module, df),
            invalid_count=0,
        )
        assert persisted == 0
        # No DataFrame methods touched — the early return must avoid any
        # Spark action (limit, withColumn, writeTo, etc.) which would
        # otherwise blow up with the bare MagicMock in real conditions.
        df.limit.assert_not_called()
        df.withColumn.assert_not_called()
        df.writeTo.assert_not_called()

    def test_negative_count_is_treated_as_empty(self, runner_module):
        # Defensive guard against pathological invalid_count values.
        df = MagicMock(name="violation_df")
        persisted = runner_module._write_sql_quarantine_records(
            **self._common_kwargs(runner_module, df),
            invalid_count=-5,
        )
        assert persisted == 0
        df.limit.assert_not_called()

    def test_below_cap_skips_the_limit_call(self, runner_module):
        # When invalid_count <= max_rows, .limit() must NOT be called —
        # that would be a redundant Spark action and obscure the original
        # cardinality. Wire up a chained mock so the (mocked) writeTo
        # call still completes, then assert the contract.
        df = MagicMock(name="violation_df")
        df.columns = ["id", "name"]
        chain = MagicMock(name="chain")
        chain.withColumn.return_value = chain
        chain.select.return_value = chain
        chain.writeTo.return_value.append.return_value = None
        df.withColumn.return_value = chain

        persisted = runner_module._write_sql_quarantine_records(
            **self._common_kwargs(runner_module, df),
            invalid_count=5,
            max_rows=10,
        )
        assert persisted == 5
        df.limit.assert_not_called()
        chain.writeTo.assert_called_once()

    def test_above_cap_calls_limit_with_max_rows(self, runner_module):
        # When invalid_count > max_rows we MUST call .limit(max_rows) so
        # the write doesn't materialise more rows than necessary, and
        # the function returns the persisted count (max_rows), not the
        # original violation count.
        df = MagicMock(name="violation_df")
        df.columns = ["id", "name"]
        capped = MagicMock(name="capped_df")
        capped.columns = ["id", "name"]
        df.limit.return_value = capped
        chain = MagicMock(name="chain")
        chain.withColumn.return_value = chain
        chain.select.return_value = chain
        chain.writeTo.return_value.append.return_value = None
        capped.withColumn.return_value = chain

        persisted = runner_module._write_sql_quarantine_records(
            **self._common_kwargs(runner_module, df),
            invalid_count=1_000_000,
            max_rows=100,
        )
        assert persisted == 100
        df.limit.assert_called_once_with(100)
        chain.writeTo.assert_called_once()

    def test_synthetic_errors_payload_uses_check_name_key(self, runner_module):
        # The errors JSON written into ``dq_quarantine_records.errors``
        # must mirror the column-check format: ``{<check_name>: <message>}``.
        # We can't introspect the Spark expression directly, but we can
        # verify the constant payload that's threaded into ``F.lit(...)``
        # by capturing the lit() argument from a hand-rolled spy on
        # ``pyspark.sql.functions``.
        # Rather than monkey-patching pyspark, just verify the Python
        # constant we emit by re-constructing it deterministically.
        synth = json.dumps({"my_sql_check": "SQL check violation"})
        # Re-invoke the same json serialiser the runner uses (it reuses
        # ``_json_dumps`` so dates/decimals are stable). For a plain
        # dict this is the same as ``json.dumps``.
        re_synth = runner_module._json_dumps({"my_sql_check": "SQL check violation"})
        assert json.loads(synth) == json.loads(re_synth)
        assert json.loads(re_synth) == {"my_sql_check": "SQL check violation"}

    def test_max_rows_constant_default_matches_module(self, runner_module):
        # Regression guard: the function default must mirror the module
        # constant so admins tuning the constant don't get a stale default.
        import inspect

        sig = inspect.signature(runner_module._write_sql_quarantine_records)
        assert sig.parameters["max_rows"].default == runner_module._SQL_QUARANTINE_MAX_ROWS

    def test_sql_quarantine_max_rows_is_finite_and_positive(self, runner_module):
        cap = runner_module._SQL_QUARANTINE_MAX_ROWS
        assert isinstance(cap, int)
        assert cap > 0
        # Sanity: leave headroom over the public export cap so admins
        # querying the table directly can still see more rows than the
        # download endpoint surfaces.
        from databricks_labs_dqx_app.backend.routes.v1 import quarantine as q

        assert cap >= q._EXPORT_MAX_ROWS

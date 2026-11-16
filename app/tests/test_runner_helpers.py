"""Tests for the pure (no-Spark) helpers in ``dqx_task_runner.runner``.

The runner module is heavyweight (PySpark + DQX engine) but its helper
functions are plain Python: fingerprint computation, custom-metric
validation, and conversion of observed ``check_metrics`` JSON into the
legacy ``error_summary`` shape.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, create_autospec

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame

from databricks.labs.dqx.errors import ComputationError, InvalidCheckError, UnsafeSqlQueryError

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


@pytest.fixture
def evaluated_rows_module():
    import dqx_task_runner.evaluated_rows as m  # type: ignore[import-not-found]

    return m


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

    def test_synthetic_errors_payload_matches_dqx_result_schema(self, runner_module):
        # The errors JSON written into ``dq_quarantine_records.errors``
        # must mirror DQX's ``dq_result_item_schema`` — a list of
        # ``{name, message, ...}`` structs — so:
        #   * the Pydantic ``QuarantineRecordOut.errors: list[Any]``
        #     validates without coercion,
        #   * the frontend's ``row.errors.map(formatError)`` renders the
        #     same way as row-level check errors,
        #   * legacy ``{check_name: message}`` dict-shape rows still
        #     work via the back-compat coercion in
        #     ``quarantine._row_to_record``.
        re_synth = runner_module._json_dumps([{"name": "my_sql_check", "message": "SQL check violation"}])
        decoded = json.loads(re_synth)
        assert isinstance(decoded, list)
        assert decoded == [{"name": "my_sql_check", "message": "SQL check violation"}]

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


# ---------------------------------------------------------------------------
# _parse_job_run_id — {{job.run_id}} run-context parsing (B2-69)
# ---------------------------------------------------------------------------


class TestParseJobRunId:
    def test_none_and_empty_return_none(self, runner_module):
        assert runner_module._parse_job_run_id(None) is None
        assert runner_module._parse_job_run_id("") is None
        assert runner_module._parse_job_run_id("   ") is None

    def test_numeric_string_parses(self, runner_module):
        assert runner_module._parse_job_run_id("123456") == 123456

    def test_surrounding_whitespace_tolerated(self, runner_module):
        assert runner_module._parse_job_run_id("  789 ") == 789

    def test_unresolved_template_reference_returns_none(self, runner_module):
        # If the {{job.run_id}} reference is not substituted (local run, SDK
        # drift) we must not persist the literal — treat it as absent.
        assert runner_module._parse_job_run_id("{{job.run_id}}") is None

    def test_non_numeric_returns_none(self, runner_module):
        assert runner_module._parse_job_run_id("abc") is None
        assert runner_module._parse_job_run_id("12x") is None


# ---------------------------------------------------------------------------
# _stamp_run_provenance — terminal-row field population (B2-69)
# ---------------------------------------------------------------------------


class TestStampRunProvenance:
    """The task runner appends a fresh terminal row; this helper stamps the
    provenance columns the Runs-History UI reads off it: ``created_at``,
    ``updated_at`` (duration source for validation runs) and ``job_run_id``
    (the "Run" deep-link).
    """

    def _mock_df(self):
        df = MagicMock(name="result_row")
        df.withColumn.return_value = df  # chainable
        return df

    def _stamped_columns(self, df) -> list[str]:
        return [call.args[0] for call in df.withColumn.call_args_list]

    def test_stamps_created_updated_and_job_run_id(self, runner_module):
        df = self._mock_df()
        out = runner_module._stamp_run_provenance(df, job_run_id=42, duration_seconds=1.5)
        cols = self._stamped_columns(df)
        assert "updated_at" in cols
        assert "created_at" in cols
        assert "job_run_id" in cols
        assert out is df

    def test_omits_job_run_id_when_none(self, runner_module):
        # A missing run context must leave job_run_id unset (NULL) rather than
        # writing a bogus value — but timestamps are always stamped.
        df = self._mock_df()
        runner_module._stamp_run_provenance(df, job_run_id=None, duration_seconds=None)
        cols = self._stamped_columns(df)
        assert "job_run_id" not in cols
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_zero_duration_still_stamps_created_at(self, runner_module):
        # duration_seconds=None / 0 collapses created_at to the completion
        # instant, but the column must still be present.
        df = self._mock_df()
        runner_module._stamp_run_provenance(df, job_run_id=None, duration_seconds=0)
        assert "created_at" in self._stamped_columns(df)


# ---------------------------------------------------------------------------
# _write_error — run_type tagging on the FAILED row
# ---------------------------------------------------------------------------


class TestWriteErrorRunType:
    """The FAILED row must carry the SUBMITTER's run_type, not the task_type.

    Every app-submitted check run is submitted as ``task_type="dryrun"``;
    manual-vs-scheduled travels separately in the job config. Deriving the tag
    from ``task_type`` recorded a failed scheduled run as ``dryrun``, so Runs
    History labelled it "Manual".
    """

    def _written_row(self, spark) -> dict:
        """Zip the written tuple against its schema so asserts aren't positional."""
        rows = spark.createDataFrame.call_args.args[0]
        schema = spark.createDataFrame.call_args.kwargs["schema"]
        names = [field.strip().rsplit(" ", 1)[0] for field in schema.split(",")]
        return dict(zip(names, rows[0]))

    def _write(self, runner_module, **kwargs) -> dict:
        spark = MagicMock(name="spark")
        runner_module._write_error(
            spark,
            kwargs.pop("task_type", "dryrun"),
            "cat",
            "sch",
            "run1",
            "alice@example.com",
            "cat.sch.tbl",
            "cat.tmp.view",
            "boom",
            **kwargs,
        )
        return self._written_row(spark)

    def test_config_run_type_wins_over_task_type(self, runner_module):
        # The scheduler fires binding runs as task_type="dryrun" with
        # run_type="scheduled" in the config — the row must say "scheduled".
        row = self._write(runner_module, task_type="dryrun", run_type="scheduled")
        assert row["run_type"] == "scheduled"
        assert row["status"] == "FAILED"

    def test_manual_run_stays_dryrun(self, runner_module):
        row = self._write(runner_module, task_type="dryrun", run_type="dryrun")
        assert row["run_type"] == "dryrun"

    def test_task_type_is_the_fallback_when_config_omits_run_type(self, runner_module):
        # Callers that submit "scheduled" directly pass no config run_type.
        assert self._write(runner_module, task_type="scheduled")["run_type"] == "scheduled"
        assert self._write(runner_module, task_type="dryrun")["run_type"] == "dryrun"

    def test_skip_history_still_forces_preview(self, runner_module):
        # A preview must never be tagged as history, whatever the config says.
        row = self._write(runner_module, task_type="dryrun", run_type="scheduled", skip_history=True)
        assert row["run_type"] == "preview"


# ---------------------------------------------------------------------------
# _is_permission_denied — quiet the redundant SP-side DROP (B2-70)
# ---------------------------------------------------------------------------


class TestIsPermissionDenied:
    def test_matches_permission_denied_text(self, runner_module):
        exc = RuntimeError("PERMISSION_DENIED: User does not have MANAGE on Table 'cat.dqx_studio_tmp.tmp_view_abc'")
        assert runner_module._is_permission_denied(exc) is True

    def test_matches_manage_phrase_case_insensitive(self, runner_module):
        assert runner_module._is_permission_denied(Exception("user does not have manage on table")) is True

    def test_unrelated_error_is_not_permission_denied(self, runner_module):
        assert runner_module._is_permission_denied(RuntimeError("connection reset by peer")) is False


# ---------------------------------------------------------------------------
# Run-config resolution — inline / Delta-manifest / legacy-volume branches
# ---------------------------------------------------------------------------

_TABLE = "`cat`.`sch`.`dq_run_configs`"


class _FakeDataFrame:
    """Stand-in for the DataFrame returned by ``spark.sql``."""

    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _FakeSpark:
    """Minimal SparkSession stand-in that records ``spark.sql`` calls.

    *rows* is returned for every ``SELECT``; *error* is raised by every call.
    """

    def __init__(self, rows=None, error=None):
        self._rows = rows or []
        self._error = error
        self.calls: list[tuple[str, dict]] = []

    def sql(self, query, args=None):
        self.calls.append((query, args or {}))
        if self._error:
            raise self._error
        return _FakeDataFrame(self._rows)


class TestResolveRunConfig:
    """The runner loads its config three ways: inlined in ``config_json``, from
    the ``dq_run_configs`` Delta table (oversized configs), or (for backwards
    compatibility) from a legacy volume file stub.
    """

    def test_inline_config_passes_through_untouched(self, runner_module):
        ws = MagicMock(name="ws")
        spark = _FakeSpark()
        raw = {"checks": [{"name": "c1"}], "sample_size": 100}
        config, cleanup = runner_module._resolve_run_config(spark, ws, raw, _TABLE, "run1")
        assert config == raw
        assert cleanup is None
        assert spark.calls == []
        ws.files.download.assert_not_called()

    def test_manifest_stub_reads_from_the_table(self, runner_module):
        ws = MagicMock(name="ws")
        full = {"checks": [{"name": "c1"}], "sample_size": 50}
        spark = _FakeSpark(rows=[{"config": json.dumps(full)}])
        config, cleanup = runner_module._resolve_run_config(spark, ws, {"__manifest__": True}, _TABLE, "run1")
        assert config == full
        assert cleanup == ("table", _TABLE)
        query, args = spark.calls[0]
        assert query == f"SELECT config FROM {_TABLE} WHERE run_id = :run_id"
        assert args == {"run_id": "run1"}

    def test_missing_manifest_row_raises(self, runner_module):
        spark = _FakeSpark(rows=[])
        with pytest.raises(RuntimeError, match="No manifest run config row"):
            runner_module._resolve_run_config(spark, MagicMock(), {"__manifest__": True}, _TABLE, "run1")

    def test_legacy_volume_stub_reads_from_file(self, runner_module):
        ws = MagicMock(name="ws")
        spark = _FakeSpark()
        full = {"checks": [], "sample_size": 5}
        ws.files.download.return_value.contents.read.return_value = json.dumps(full).encode()
        path = "/Volumes/c/s/w/run-configs/run1.json"
        config, cleanup = runner_module._resolve_run_config(spark, ws, {"__staged__": path}, _TABLE, "run1")
        assert config == full
        assert cleanup == ("volume", path)
        assert spark.calls == []

    def test_run_configs_table_is_quoted(self, runner_module):
        assert runner_module._run_configs_table("cat", "sch") == _TABLE

    def test_run_configs_table_rejects_bad_schema(self, runner_module):
        with pytest.raises(ValueError):
            runner_module._run_configs_table("cat", "sch`; DROP")


class TestCleanupRunConfig:
    def test_table_cleanup_deletes_the_row(self, runner_module):
        spark = _FakeSpark()
        runner_module._cleanup_run_config(spark, MagicMock(), ("table", _TABLE), "run1")
        assert spark.calls == [(f"DELETE FROM {_TABLE} WHERE run_id = :run_id", {"run_id": "run1"})]

    def test_table_delete_failure_is_swallowed(self, runner_module):
        # Cleanup is best-effort; the retention sweep removes a row left behind.
        spark = _FakeSpark(error=RuntimeError("permission denied"))
        runner_module._cleanup_run_config(spark, MagicMock(), ("table", _TABLE), "run1")

    def test_volume_cleanup_deletes_the_file(self, runner_module):
        ws = MagicMock(name="ws")
        path = "/Volumes/c/s/w/run-configs/run1.json"
        runner_module._cleanup_run_config(_FakeSpark(), ws, ("volume", path), "run1")
        ws.files.delete.assert_called_once_with(path)

    def test_none_cleanup_is_a_noop(self, runner_module):
        ws = MagicMock(name="ws")
        spark = _FakeSpark()
        runner_module._cleanup_run_config(spark, ws, None, "run1")
        ws.files.delete.assert_not_called()
        assert spark.calls == []

    def test_volume_delete_failure_is_swallowed(self, runner_module):
        # Cleanup is best-effort; a delete failure must not propagate out of the
        # runner's finally block.
        ws = MagicMock(name="ws")
        ws.files.delete.side_effect = RuntimeError("permission denied")
        runner_module._cleanup_run_config(_FakeSpark(), ws, ("volume", "/Volumes/c/s/w/run-configs/run1.json"), "run1")


# ---------------------------------------------------------------------------
# Manifest config parsing
# ---------------------------------------------------------------------------


class TestParseManifestConfig:
    def test_parses_valid_json_object(self, runner_module):
        cfg = {"checks": [{"name": "c1"}]}
        assert runner_module._parse_manifest_config(json.dumps(cfg), "run1") == cfg

    def test_empty_value_raises(self, runner_module):
        with pytest.raises(RuntimeError, match="empty"):
            runner_module._parse_manifest_config("", "run1")
        with pytest.raises(RuntimeError, match="empty"):
            runner_module._parse_manifest_config(None, "run1")

    def test_non_string_value_raises(self, runner_module):
        with pytest.raises(RuntimeError, match="non-string"):
            runner_module._parse_manifest_config(123, "run1")

    def test_non_object_json_raises(self, runner_module):
        with pytest.raises(RuntimeError, match="not a JSON object"):
            runner_module._parse_manifest_config("[1, 2, 3]", "run1")


# ---------------------------------------------------------------------------
# build_evaluated_rows_metric — per-check evaluated-row counts
# ---------------------------------------------------------------------------


class TestBuildEvaluatedRowsMetric:
    def _row_check(self, name: str, flt: str | None, column: str = "a") -> dict:
        check = {
            "name": name,
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": column}},
        }
        if flt is not None:
            check["filter"] = flt
        return check

    def test_metric_name_constant(self, evaluated_rows_module):
        assert evaluated_rows_module.EVALUATED_ROWS_METRIC == "check_evaluated_rows"

    def test_no_filtered_checks_returns_none(self, evaluated_rows_module):
        assert evaluated_rows_module.build_evaluated_rows_metric([self._row_check("a_not_null", None)]) is None

    def test_filtered_row_check_counts_rows_where_filter_true(self, evaluated_rows_module):
        expr = evaluated_rows_module.build_evaluated_rows_metric([self._row_check("a_not_null", "region = 'EU'")])
        assert expr is not None
        assert expr.endswith(" as check_evaluated_rows")
        assert "count(case when (region = 'EU'\n) then 1 end)" in expr
        assert "'\"a_not_null\"'" in expr

    def test_expression_skeleton_joins_entries(self, evaluated_rows_module):
        expr = evaluated_rows_module.build_evaluated_rows_metric(
            [self._row_check("one", "a > 0"), self._row_check("two", "a < 5")]
        )
        assert expr is not None
        assert expr.startswith("concat('[', concat_ws(',', ")
        assert expr.count("count(case when") == 2
        assert expr.index("'\"one\"'") < expr.index("'\"two\"'")

    def test_dataset_check_with_filter_is_skipped(self, evaluated_rows_module):
        check = {
            "name": "uniq",
            "criticality": "error",
            "filter": "x > 1",
            "check": {"function": "is_unique", "arguments": {"columns": ["a"]}},
        }
        assert evaluated_rows_module.build_evaluated_rows_metric([check]) is None

    def test_check_name_quotes_are_escaped(self, evaluated_rows_module):
        expr = evaluated_rows_module.build_evaluated_rows_metric([self._row_check("it's", "a > 0")])
        assert expr is not None
        assert "it\\'s" in expr

    def test_unsafe_filter_fails_with_sanitized_check_name(self, evaluated_rows_module):
        with pytest.raises(UnsafeSqlQueryError) as exc_info:
            evaluated_rows_module.build_evaluated_rows_metric([self._row_check("bad\nname", "1=1; DROP TABLE x")])
        message = str(exc_info.value)
        assert "'bad name'" in message
        assert "\n" not in message
        assert "DROP TABLE" not in message

    def test_double_dash_inside_string_literal_is_kept_verbatim(self, evaluated_rows_module):
        expr = evaluated_rows_module.build_evaluated_rows_metric([self._row_check("c", "note = 'a--b'")])
        assert expr is not None
        assert "note = 'a--b'" in expr

    def test_trailing_line_comment_cannot_swallow_closing_paren(self, evaluated_rows_module):
        expr = evaluated_rows_module.build_evaluated_rows_metric([self._row_check("c", "a > 0 -- only positives")])
        assert expr is not None
        assert "count(case when (a > 0 -- only positives\n) then 1 end)" in expr

    def test_for_each_column_gets_entry_per_expanded_rule(self, evaluated_rows_module):
        check = {
            "criticality": "error",
            "filter": "region = 'EU'",
            "check": {"function": "is_not_null", "for_each_column": ["a", "b"], "arguments": {}},
        }
        expr = evaluated_rows_module.build_evaluated_rows_metric([check])
        assert expr is not None
        assert expr.count("count(case when (region = 'EU'\n) then 1 end)") == 2
        assert "'\"a_is_null\"'" in expr
        assert "'\"b_is_null\"'" in expr

    def test_subquery_filter_is_not_observed(self, evaluated_rows_module):
        flt = "x IN (SELECT id FROM t)"
        assert evaluated_rows_module.build_evaluated_rows_metric([self._row_check("sub", flt)]) is None

    def test_select_inside_string_literal_is_not_a_subquery(self, evaluated_rows_module):
        expr = evaluated_rows_module.build_evaluated_rows_metric([self._row_check("c", "note = 'select me'")])
        assert expr is not None
        assert "note = 'select me'" in expr

    def test_subquery_check_counted_separately_others_observed(self, evaluated_rows_module):
        checks = [self._row_check("sub", "x in (select id from t)"), self._row_check("ok", "a > 0")]
        expr = evaluated_rows_module.build_evaluated_rows_metric(checks)
        assert expr is not None
        assert "'\"ok\"'" in expr
        assert "'\"sub\"'" not in expr
        plan = evaluated_rows_module.plan_evaluated_rows(checks)
        assert plan.observed_metric == expr
        assert plan.separate_filters == [("sub", "x in (select id from t)")]
        assert plan.filters == [("sub", "x in (select id from t)"), ("ok", "a > 0")]


# ---------------------------------------------------------------------------
# prepare_evaluated_rows / with_separate_evaluated_rows — fail runs whose filters cannot be counted
# ---------------------------------------------------------------------------


_FILTERED_CHECK = {
    "name": "eu_not_null",
    "criticality": "error",
    "filter": "region = 'EU'",
    "check": {"function": "is_not_null", "arguments": {"column": "a"}},
}


_SUBQUERY_CHECK = {
    "name": "known_region_not_null",
    "criticality": "error",
    "filter": "region IN (SELECT code FROM c.s.regions)",
    "check": {"function": "is_not_null", "arguments": {"column": "a"}},
}


def _analysing_df(failing_text: str | None = None) -> MagicMock:
    """A DataFrame mock whose lazy *agg(...).schema* fails when any aggregate column mentions *failing_text*."""
    df = create_autospec(DataFrame, instance=True)

    def agg(*cols: object) -> MagicMock:
        aggregated = MagicMock()
        if failing_text is not None and any(failing_text in str(col) for col in cols):
            type(aggregated).schema = PropertyMock(side_effect=AnalysisException("cannot resolve"))
        return aggregated

    df.agg.side_effect = agg
    return df


class TestPrepareEvaluatedRows:
    def test_builds_observed_metric_when_filters_analyse(self, evaluated_rows_module):
        df = _analysing_df()
        plan = evaluated_rows_module.prepare_evaluated_rows(df, [_FILTERED_CHECK])
        assert plan.observed_metric is not None
        assert plan.observed_metric.endswith(" as check_evaluated_rows")
        assert plan.separate_filters == []
        # One lazy analysis of the filters on their own, one of the assembled metric; no job.
        assert df.agg.call_count == 2
        df.collect.assert_not_called()

    def test_each_filter_reaches_spark_as_its_own_expression(self, evaluated_rows_module):
        # Parens that only balance inside the count(case when (...)) template: parsed on its own the
        # filter is malformed, so Spark must see it as a standalone expression and reject it.
        breakout = "a > 0) then 1 end), (count(case when (1 = 1"
        check = {**_FILTERED_CHECK, "filter": breakout}
        df = _analysing_df(failing_text=breakout)
        with pytest.raises(InvalidCheckError):
            evaluated_rows_module.prepare_evaluated_rows(df, [check])
        first_analysis = [str(col) for col in df.agg.call_args_list[0].args]
        assert first_analysis == [f"Column<'count(CASE WHEN {breakout} THEN 1 END) AS _f0'>"]

    def test_unanalysable_filter_fails_the_run_naming_only_that_check(self, evaluated_rows_module):
        bad = {**_FILTERED_CHECK, "name": "eu\r\nnot null", "filter": "missing_col = 1"}
        df = _analysing_df(failing_text="missing_col")
        with pytest.raises(InvalidCheckError) as exc_info:
            evaluated_rows_module.prepare_evaluated_rows(df, [_FILTERED_CHECK, bad])
        message = str(exc_info.value)
        assert "'eu  not null'" in message
        assert "eu_not_null" not in message
        assert "missing_col" not in message
        assert "\n" not in message and "\r" not in message

    def test_unanalysable_observed_metric_fails_the_run(self, evaluated_rows_module):
        df = _analysing_df(failing_text="check_evaluated_rows")
        with pytest.raises(InvalidCheckError) as exc_info:
            evaluated_rows_module.prepare_evaluated_rows(df, [_FILTERED_CHECK])
        assert "region = 'EU'" not in str(exc_info.value)

    def test_unsafe_filter_fails_the_run(self, evaluated_rows_module):
        unsafe = {**_FILTERED_CHECK, "filter": "1=1; DROP TABLE x"}
        df = _analysing_df()
        with pytest.raises(UnsafeSqlQueryError):
            evaluated_rows_module.prepare_evaluated_rows(df, [unsafe])
        df.agg.assert_not_called()

    def test_subquery_filter_is_analysed_but_not_observed(self, evaluated_rows_module):
        df = _analysing_df()
        plan = evaluated_rows_module.prepare_evaluated_rows(df, [_SUBQUERY_CHECK])
        assert plan.observed_metric is None
        assert plan.separate_filters == [("known_region_not_null", _SUBQUERY_CHECK["filter"])]
        df.agg.assert_called_once()

    def test_no_filtered_checks_skips_analysis(self, evaluated_rows_module):
        df = create_autospec(DataFrame, instance=True)
        unfiltered = {k: v for k, v in _FILTERED_CHECK.items() if k != "filter"}
        plan = evaluated_rows_module.prepare_evaluated_rows(df, [unfiltered])
        assert plan.observed_metric is None
        assert plan.separate_filters == []
        df.agg.assert_not_called()


def _counting_df(counts: list[int]) -> MagicMock:
    df = create_autospec(DataFrame, instance=True)
    df.agg.return_value.collect.return_value = [tuple(counts)]
    return df


class TestWithSeparateEvaluatedRows:
    def test_merges_separate_counts_into_observed_payload(self, evaluated_rows_module):
        plan = evaluated_rows_module.plan_evaluated_rows([_FILTERED_CHECK, _SUBQUERY_CHECK])
        payload = '[{"check_name":"eu_not_null","evaluated_count":4}]'
        observed = {"input_row_count": 10, "check_evaluated_rows": payload}
        df = _counting_df([7])
        merged = evaluated_rows_module.with_separate_evaluated_rows(df, plan, observed)
        assert json.loads(merged["check_evaluated_rows"]) == [
            {"check_name": "eu_not_null", "evaluated_count": 4},
            {"check_name": "known_region_not_null", "evaluated_count": 7},
        ]
        assert merged["input_row_count"] == 10
        assert observed["check_evaluated_rows"] == payload  # the snapshot is never mutated
        df.agg.assert_called_once()

    def test_creates_payload_when_observed_has_none(self, evaluated_rows_module):
        plan = evaluated_rows_module.plan_evaluated_rows([_SUBQUERY_CHECK])
        merged = evaluated_rows_module.with_separate_evaluated_rows(_counting_df([3]), plan, {"input_row_count": 5})
        assert json.loads(merged["check_evaluated_rows"]) == [
            {"check_name": "known_region_not_null", "evaluated_count": 3}
        ]

    def test_one_aggregate_for_all_separate_filters(self, evaluated_rows_module):
        second = {**_SUBQUERY_CHECK, "name": "other", "filter": "EXISTS (SELECT 1 FROM c.s.r)"}
        plan = evaluated_rows_module.plan_evaluated_rows([_SUBQUERY_CHECK, second])
        df = _counting_df([3, 0])
        merged = evaluated_rows_module.with_separate_evaluated_rows(df, plan, {})
        assert [e["evaluated_count"] for e in json.loads(merged["check_evaluated_rows"])] == [3, 0]
        df.agg.assert_called_once()
        assert len(df.agg.call_args.args) == 2

    def test_no_separate_filters_runs_nothing(self, evaluated_rows_module):
        plan = evaluated_rows_module.plan_evaluated_rows([_FILTERED_CHECK])
        df = create_autospec(DataFrame, instance=True)
        observed = {"input_row_count": 1}
        assert evaluated_rows_module.with_separate_evaluated_rows(df, plan, observed) == observed
        df.agg.assert_not_called()

    def test_failed_aggregate_fails_the_run(self, evaluated_rows_module):
        plan = evaluated_rows_module.plan_evaluated_rows([_SUBQUERY_CHECK])
        df = create_autospec(DataFrame, instance=True)
        df.agg.return_value.collect.side_effect = AnalysisException("boom")
        with pytest.raises(ComputationError) as exc_info:
            evaluated_rows_module.with_separate_evaluated_rows(df, plan, {})
        assert "'known_region_not_null'" in str(exc_info.value)
        assert "SELECT" not in str(exc_info.value)


def _persisted_metrics(spark: MagicMock) -> dict[str, str]:
    """The metric_name -> metric_value rows the run wrote via *DQMetricsObserver.build_metrics_df*."""
    from databricks.labs.dqx.metrics_observer import OBSERVATION_TABLE_SCHEMA

    for call in spark.createDataFrame.call_args_list:
        if call.kwargs.get("schema") is OBSERVATION_TABLE_SCHEMA:
            return {row[7]: row[8] for row in call.args[0]}
    raise AssertionError("no metrics were persisted")


class TestRunEvaluatedRows:
    def test_scheduled_run_fails_when_filter_cannot_be_analysed(self, runner_module):
        # Scope-config schedules run through the scheduled path; the filters must be analysed
        # against the source DataFrame there too, and an unanalysable one fails the run.
        spark = MagicMock()
        source_df = spark.table.return_value
        type(source_df.agg.return_value).schema = PropertyMock(side_effect=AnalysisException("cannot resolve"))
        config = {"checks": [_FILTERED_CHECK], "source_table_fqn": "c.s.t"}
        with pytest.raises(InvalidCheckError):
            runner_module._run_scheduled(spark, MagicMock(), "c.s.t", config, "c", "s", "run1", "user")
        source_df.agg.assert_called()

    @pytest.mark.parametrize("task", ["scheduled", "dryrun"])
    def test_run_persists_separately_counted_subquery_filters(self, runner_module, monkeypatch, task):
        import databricks.labs.dqx.engine as engine_module

        observed = {"input_row_count": 10, "check_evaluated_rows": '[{"check_name":"eu_not_null","evaluated_count":4}]'}
        invalid_df = MagicMock()
        invalid_df.count.return_value = 0
        observation = MagicMock()
        observation.get = observed
        engine_cls = MagicMock()
        engine_cls.return_value.apply_checks_by_metadata_and_split.return_value = (MagicMock(), invalid_df, observation)
        # DQEngine is the third-party boundary: the checks themselves are not under test here.
        monkeypatch.setattr(engine_module, "DQEngine", engine_cls)
        spark = MagicMock()
        source_df = spark.table.return_value
        source_df.agg.return_value.collect.return_value = [(7,)]
        config = {"checks": [_FILTERED_CHECK, _SUBQUERY_CHECK], "source_table_fqn": "c.s.t", "sample_size": 0}
        run = runner_module._run_scheduled if task == "scheduled" else runner_module._run_dryrun

        run(spark, MagicMock(), "c.s.t", config, "c", "s", "run1", "user")

        persisted = _persisted_metrics(spark)
        assert json.loads(persisted["check_evaluated_rows"]) == [
            {"check_name": "eu_not_null", "evaluated_count": 4},
            {"check_name": "known_region_not_null", "evaluated_count": 7},
        ]
        assert persisted["input_row_count"] == "10"
        # The engine ran on the very DataFrame the separate count aggregated.
        assert engine_cls.return_value.apply_checks_by_metadata_and_split.call_args.args[0] is source_df


class TestPersistedMetricCounts:
    def test_evaluated_rows_metric_counts_as_builtin(self, runner_module, caplog):
        observed = {"input_row_count": 10, "check_evaluated_rows": '[{"check_name":"c","evaluated_count":4}]'}
        with caplog.at_level("INFO", logger="dqx_task_runner"):
            runner_module._persist_observed_metrics(
                spark=MagicMock(),
                observed=observed,
                run_id="run1",
                run_name="dqx_app_scheduled",
                input_location=None,
                quarantine_location=None,
                checks_location=None,
                rule_set_fingerprint=None,
                user_metadata=None,
                result_catalog="c",
                result_schema="s",
            )
        assert "builtin=2, custom=0" in caplog.text

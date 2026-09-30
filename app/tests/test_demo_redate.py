from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.demo import redate as r

M = "dqx.dqx_studio.dq_metrics"
RUNS = "dqx.dqx_studio.dq_validation_runs"
H = "dqx.dqx_studio.dq_score_history"
V = "dqx.dqx_studio.dq_monitored_table_versions"


def _marker(name: str) -> str:
    return f":{name}"


def test_redate_metrics_binds_run_id_and_timestamp_with_quote_and_backslash():
    run_id = "a\\' OR TRUE --"
    timestamp = "2026-05-01 09:30:00\\'"
    sql, parameters = r.build_redate_metrics_sql(M, run_id, timestamp, marker=_marker)
    assert sql == f"UPDATE {M} SET run_time = CAST(:target_iso AS TIMESTAMP) WHERE run_id = :run_id"
    assert parameters == {"run_id": run_id, "target_iso": timestamp}
    assert run_id not in sql and timestamp not in sql


def test_redate_history_binds_scope_values_and_reuses_markers():
    scope_key = "a\\' OR TRUE --"
    sql, parameters = r.build_redate_latest_history_sql(H, "table", scope_key, "2026-05-01", marker=_marker)
    assert sql.count(":scope_key") == 2
    assert sql.count(":scope_type") == 2
    assert parameters == {"scope_type": "table", "scope_key": scope_key, "target_iso": "2026-05-01"}
    assert scope_key not in sql


def test_iso_formats_utc():
    assert r.iso(datetime(2026, 5, 1, 9, 30, 0, tzinfo=timezone.utc)) == "2026-05-01 09:30:00"


def test_redate_metrics_targets_run_id_and_casts_timestamp():
    sql, params = r.build_redate_metrics_sql(M, "abc123", "2026-05-01 09:30:00", marker=_marker)
    assert sql.startswith("UPDATE")
    assert M in sql and "run_time" in sql
    assert "CAST(:target_iso AS TIMESTAMP)" in sql
    assert "run_id = :run_id" in sql
    assert params == {"run_id": "abc123", "target_iso": "2026-05-01 09:30:00"}


def test_redate_runs_preserves_positive_duration_span():
    # FIX I: the Runs History "Time" column is derived as
    # timestampdiff(SECOND, MIN(created_at), MAX(updated_at)); collapsing both to
    # one instant makes it zero -> blank "–". The re-date must set updated_at to
    # created_at + a positive duration so the span (and displayed Time) is real.
    sql, params = r.build_redate_runs_sql(RUNS, "abc123", "2026-05-01 09:30:00", duration_seconds=45, marker=_marker)
    assert sql.startswith("UPDATE")
    assert RUNS in sql
    assert "created_at = CAST(:target_iso AS TIMESTAMP)" in sql
    # end = start + duration, so run_ended_at > run_started_at (positive span)
    assert "updated_at = CAST(:target_iso AS TIMESTAMP) + INTERVAL 45 SECONDS" in sql
    assert "run_id = :run_id" in sql
    assert params == {"run_id": "abc123", "target_iso": "2026-05-01 09:30:00"}


def test_redate_runs_default_duration_is_positive():
    sql, _ = r.build_redate_runs_sql(RUNS, "abc123", "2026-05-01 09:30:00", marker=_marker)
    # default duration keeps a believable, positive span rather than a zero one
    assert "+ INTERVAL 45 SECONDS" in sql


def test_redate_runs_run_id_is_escaped_against_injection():
    sql, params = r.build_redate_runs_sql(RUNS, "a'b", "2026-05-01 09:30:00", marker=_marker)
    assert "a'b" not in sql
    assert params["run_id"] == "a'b"


def test_redate_versions_targets_binding_and_version_and_casts_timestamp():
    # Item 2: freeze created_at is written at seed-time "now"; it must be
    # re-dated into the trend window (keyed on binding_id + version) so
    # annotate_trend_versions resolves increasing versions mid-timeline and the
    # results-over-time version markers appear.
    sql, params = r.build_redate_versions_sql(V, "b-abc", 2, "2026-05-01 09:30:00", marker=_marker)
    assert sql.startswith("UPDATE")
    assert V in sql and "created_at" in sql
    assert "created_at = CAST(:target_iso AS TIMESTAMP)" in sql
    assert "binding_id = :binding_id" in sql
    assert "version = :version" in sql
    assert params == {"binding_id": "b-abc", "version": 2, "target_iso": "2026-05-01 09:30:00"}


def test_redate_versions_binding_id_is_escaped_against_injection():
    sql, params = r.build_redate_versions_sql(V, "a'b", 1, "2026-05-01 09:30:00", marker=_marker)
    assert "a'b" not in sql
    assert params["binding_id"] == "a'b"


def test_redate_versions_version_is_coerced_to_int():
    # the version is interpolated verbatim after an int() cast, so a non-int
    # string can never smuggle SQL through the version slot
    sql, params = r.build_redate_versions_sql(V, "b1", 3, "2026-05-01 09:30:00", marker=_marker)
    assert "version = :version" in sql
    assert params["version"] == 3


def test_delete_metrics_targets_run_id():
    sql, params = r.build_delete_metrics_sql(M, "abc123", marker=_marker)
    assert sql.startswith("DELETE FROM")
    assert M in sql
    assert "run_id = :run_id" in sql
    assert params == {"run_id": "abc123"}


def test_delete_runs_targets_run_id():
    sql, params = r.build_delete_runs_sql(RUNS, "abc123", marker=_marker)
    assert sql.startswith("DELETE FROM")
    assert RUNS in sql
    assert "run_id = :run_id" in sql
    assert params == {"run_id": "abc123"}


def test_delete_run_id_is_escaped_against_injection():
    sql, params = r.build_delete_metrics_sql(M, "a'b", marker=_marker)
    assert "a'b" not in sql
    assert params["run_id"] == "a'b"


def test_run_id_is_escaped_against_injection():
    sql, params = r.build_redate_metrics_sql(M, "a'b", "2026-05-01 09:30:00", marker=_marker)
    assert "a'b" not in sql
    assert params["run_id"] == "a'b"


def test_delete_history_after_targets_computed_at_cutoff():
    sql, params = r.build_delete_history_after_sql(H, "2026-05-01 09:30:00", marker=_marker)
    assert sql.startswith("DELETE FROM")
    assert H in sql
    # deletes rows appended AFTER the cutoff (the un-re-dated real-now appends)
    assert "computed_at > CAST(:target_iso AS TIMESTAMP)" in sql
    assert params == {"target_iso": "2026-05-01 09:30:00"}
    # no run/scope filter — a plain computed_at cutoff over the whole table
    assert "scope_type" not in sql
    assert "run_id" not in sql


def test_delete_history_after_cutoff_is_escaped_against_injection():
    sql, params = r.build_delete_history_after_sql(H, "a'b", marker=_marker)
    assert "a'b" not in sql
    assert params["target_iso"] == "a'b"


def test_delete_orphan_metrics_is_anti_join_on_validation_runs():
    # A deleted gate run's late metric batch survives as a run_id present in
    # dq_metrics but absent from dq_validation_runs. The sweep must delete
    # exactly those via an anti-join, keying only on run_id (no timestamp, no
    # scope filter) so every legit weekly run — which keeps its re-dated
    # validation-run row — is preserved.
    sql = r.build_delete_orphan_metrics_sql(M, RUNS)
    assert sql.startswith("DELETE FROM")
    assert M in sql and RUNS in sql
    assert "run_id NOT IN" in sql
    # subquery pulls the surviving run_ids from the runs table
    assert "SELECT run_id FROM" in sql
    # guards against a NULL run_id in the runs table making NOT IN drop everything
    assert "run_id IS NOT NULL" in sql
    # no time/scope predicate — the anti-join alone defines "orphan"
    assert "computed_at" not in sql
    assert "run_time" not in sql

from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.demo import redate as r

M = "dqx.dqx_studio.dq_metrics"
RUNS = "dqx.dqx_studio.dq_validation_runs"
H = "dqx.dqx_studio.dq_score_history"


def test_iso_formats_utc():
    assert r.iso(datetime(2026, 5, 1, 9, 30, 0, tzinfo=timezone.utc)) == "2026-05-01 09:30:00"


def test_redate_metrics_targets_run_id_and_casts_timestamp():
    sql = r.build_redate_metrics_sql(M, "abc123", "2026-05-01 09:30:00")
    assert sql.startswith("UPDATE")
    assert M in sql and "run_time" in sql
    assert "CAST('2026-05-01 09:30:00' AS TIMESTAMP)" in sql
    assert "run_id = 'abc123'" in sql


def test_delete_metrics_targets_run_id():
    sql = r.build_delete_metrics_sql(M, "abc123")
    assert sql.startswith("DELETE FROM")
    assert M in sql
    assert "run_id = 'abc123'" in sql


def test_delete_runs_targets_run_id():
    sql = r.build_delete_runs_sql(RUNS, "abc123")
    assert sql.startswith("DELETE FROM")
    assert RUNS in sql
    assert "run_id = 'abc123'" in sql


def test_delete_run_id_is_escaped_against_injection():
    sql = r.build_delete_metrics_sql(M, "a'b")
    assert "'a''b'" in sql  # ANSI doubled-quote escaping


def test_run_id_is_escaped_against_injection():
    sql = r.build_redate_metrics_sql(M, "a'b", "2026-05-01 09:30:00")
    assert "'a''b'" in sql  # ANSI doubled-quote escaping


def test_delete_history_after_targets_computed_at_cutoff():
    sql = r.build_delete_history_after_sql(H, "2026-05-01 09:30:00")
    assert sql.startswith("DELETE FROM")
    assert H in sql
    # deletes rows appended AFTER the cutoff (the un-re-dated real-now appends)
    assert "computed_at > CAST('2026-05-01 09:30:00' AS TIMESTAMP)" in sql
    # no run/scope filter — a plain computed_at cutoff over the whole table
    assert "scope_type" not in sql
    assert "run_id" not in sql


def test_delete_history_after_cutoff_is_escaped_against_injection():
    sql = r.build_delete_history_after_sql(H, "a'b")
    assert "'a''b'" in sql  # ANSI doubled-quote escaping

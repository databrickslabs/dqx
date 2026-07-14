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


def test_insert_history_backdates_both_timestamps():
    sql = r.build_insert_history_sql(
        H, "global", "global", score=0.91, failed_tests=12, total_tests=1000, target_iso="2026-05-01 09:30:00"
    )
    assert sql.startswith("INSERT INTO")
    assert "0.91" in sql and "12" in sql and "1000" in sql
    assert sql.count("CAST('2026-05-01 09:30:00' AS TIMESTAMP)") == 2  # run_time + computed_at


def test_insert_history_null_counts_render_null():
    sql = r.build_insert_history_sql(
        H, "table", "dqx.dqx_studio_demo.orders", score=0.5, failed_tests=None, total_tests=None,
        target_iso="2026-05-01 09:30:00",
    )
    assert "NULL" in sql


def test_run_id_is_escaped_against_injection():
    sql = r.build_redate_metrics_sql(M, "a'b", "2026-05-01 09:30:00")
    assert "'a''b'" in sql  # ANSI doubled-quote escaping

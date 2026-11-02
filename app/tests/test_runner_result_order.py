"""The runner must write a run's terminal result row only after its last read of the view.

The app drops a run's ``tmp_view_*`` as soon as a non-RUNNING row for the run
appears in ``dq_validation_runs``. The quarantine write re-scans the lazy
``invalid_df`` (and so the view), so a result row written before it lets the
app drop the view mid-run and the run fails with TABLE_OR_VIEW_NOT_FOUND.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_TASKS_SRC = Path(__file__).resolve().parent.parent / "tasks" / "src"
if str(_TASKS_SRC) not in sys.path:
    sys.path.insert(0, str(_TASKS_SRC))


@pytest.fixture
def runner_module():
    import dqx_task_runner.runner as r  # type: ignore[import-not-found]

    return r


@pytest.mark.parametrize("entrypoint", ["_run_dryrun", "_run_scheduled"])
def test_result_row_is_written_after_quarantine_and_metrics(
    runner_module, monkeypatch: pytest.MonkeyPatch, entrypoint: str
) -> None:
    import databricks.labs.dqx.engine as dqx_engine
    import databricks.labs.dqx.metrics_observer as dqx_observer

    order: list[str] = []
    invalid_df = MagicMock()
    invalid_df.count.return_value = 0
    observation = MagicMock()
    observation.get = {"input_row_count": "5", "valid_row_count": "5"}
    engine = MagicMock()
    engine.apply_checks_by_metadata_and_split.return_value = (MagicMock(), invalid_df, observation)
    monkeypatch.setattr(dqx_engine, "DQEngine", MagicMock(return_value=engine))
    monkeypatch.setattr(dqx_observer, "DQMetricsObserver", MagicMock())
    monkeypatch.setattr(runner_module, "_read_view_with_retry", MagicMock())
    monkeypatch.setattr(runner_module, "_stamp_run_provenance", lambda row, **_: row)
    monkeypatch.setattr(runner_module, "_write_quarantine_records", lambda *a, **k: order.append("quarantine"))
    monkeypatch.setattr(runner_module, "_persist_observed_metrics", lambda *a, **k: order.append("metrics"))

    spark = MagicMock()
    spark.createDataFrame.return_value.writeTo.return_value.append.side_effect = lambda: order.append("result")

    getattr(runner_module, entrypoint)(
        spark,
        MagicMock(),
        "main.dqx_studio_tmp.tmp_view_a0dff78afd11",
        {"checks": [], "source_table_fqn": "main.sales.orders", "sample_size": 0},
        "main",
        "dqx_studio",
        "run-1",
        "user@example.com",
    )

    assert order == ["quarantine", "metrics", "result"]
    schema = spark.createDataFrame.call_args.kwargs["schema"]
    for column in ("total_rows", "valid_rows", "invalid_rows", "error_rows", "warning_rows"):
        assert f"{column} BIGINT" in schema, "row counts of tables over 2^31 rows overflow INT"

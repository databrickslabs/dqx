"""Tests for oversized run-config staging (Databricks 10k job-parameter limit)."""

import json
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.run_config_store import (
    JOB_PARAMETERS_CHAR_LIMIT,
    MANIFEST_CONFIG_KEY,
    RunConfigError,
    RunConfigStagingError,
    RunConfigTooLargeError,
    job_parameters_size,
    prepare_config_json,
    stage_config_to_table,
)
from databricks_labs_dqx_app.backend.sql_executor import RawSql, SqlExecutor


def _base_params() -> dict[str, str]:
    return {
        "task_type": "dryrun",
        "view_fqn": "cat.sch.tmp_view_abc",
        "result_catalog": "cat",
        "result_schema": "sch",
        "run_id": "run123",
        "requesting_user": "user@example.com",
        "warehouse_id": "wh-1",
    }


def _oltp_mock():
    """OLTP executor mock — run_config_store only calls ``fqn`` and ``upsert``."""
    sql = create_autospec(SqlExecutor, instance=True)
    sql.fqn.side_effect = lambda t: f"dqx_studio.{t}"
    return sql


def _big_config() -> dict:
    checks = [
        {"name": f"rule_{i}", "check": {"function": "is_not_null", "arguments": {"col": "x"}}} for i in range(200)
    ]
    return {"checks": checks, "sample_size": 1000}


class TestJobParametersSize:
    def test_counts_json_representation(self) -> None:
        params = {**_base_params(), "config_json": '{"checks":[]}'}
        assert job_parameters_size(params) == len(json.dumps(params, separators=(",", ":")))


class TestPrepareConfigJson:
    def test_inline_when_under_limit(self) -> None:
        sql = _oltp_mock()
        config = {"checks": [{"name": "c1"}]}
        result = prepare_config_json(
            sql,
            run_id="run123",
            config=config,
            job_parameters_without_config=_base_params(),
        )
        assert MANIFEST_CONFIG_KEY not in json.loads(result)
        assert json.loads(result) == config
        sql.upsert.assert_not_called()

    def test_stages_when_over_limit(self) -> None:
        sql = _oltp_mock()
        config = _big_config()
        base = _base_params()
        inline = json.dumps(config, separators=(",", ":"))
        assert job_parameters_size({**base, "config_json": inline}) > JOB_PARAMETERS_CHAR_LIMIT

        result = prepare_config_json(
            sql,
            run_id="run123",
            config=config,
            job_parameters_without_config=base,
        )
        # The job carries only the tiny stub, not the checks.
        assert json.loads(result) == {MANIFEST_CONFIG_KEY: True}
        # The full config was upserted keyed by run_id.
        sql.upsert.assert_called_once()
        kwargs = sql.upsert.call_args.kwargs
        assert kwargs["key_cols"] == {"run_id": "run123"}
        assert json.loads(kwargs["value_cols"]["config"]) == config

    def test_stub_stays_within_the_job_parameter_limit(self) -> None:
        # Regression guard: the stub plus base params must always fit, so a
        # staged config never re-trips the limit it was meant to dodge.
        sql = _oltp_mock()
        config = {"checks": [{"name": f"rule_{i}"} for i in range(500)]}
        result = prepare_config_json(
            sql,
            run_id="run123",
            config=config,
            job_parameters_without_config=_base_params(),
        )
        assert job_parameters_size({**_base_params(), "config_json": result}) <= JOB_PARAMETERS_CHAR_LIMIT

    def test_staging_failure_raises_actionable_error(self) -> None:
        # A missing table / unreachable Lakebase surfaces as an actionable
        # RunConfigStagingError, not a raw SQL exception.
        sql = _oltp_mock()
        sql.upsert.side_effect = RuntimeError('relation "dq_run_configs" does not exist')
        with pytest.raises(RunConfigStagingError) as excinfo:
            prepare_config_json(
                sql,
                run_id="run123",
                config=_big_config(),
                job_parameters_without_config=_base_params(),
            )
        msg = str(excinfo.value)
        assert "run123" in msg
        assert "migrations" in msg  # tells the operator what to check
        assert isinstance(excinfo.value, RunConfigError)


class TestStageConfigToTable:
    def test_upserts_config_payload_verbatim(self) -> None:
        # A regex check body carries backslashes and quotes; the portable upsert
        # binds the JSON as a literal, so it must round-trip without hand-escaping.
        sql = _oltp_mock()
        config = {"checks": [{"name": "it's", "pattern": "\\d+"}]}
        stage_config_to_table(sql, "run123", config)
        kwargs = sql.upsert.call_args.kwargs
        assert kwargs["key_cols"] == {"run_id": "run123"}
        assert json.loads(kwargs["value_cols"]["config"]) == config
        # created_at is a portable RawSql the executor rewrites per dialect.
        assert isinstance(kwargs["value_cols"]["created_at"], RawSql)

    def test_rejects_malformed_run_id(self) -> None:
        # run_id is validated before use, so a value that isn't an app-minted id
        # is rejected rather than reaching the executor.
        sql = _oltp_mock()
        with pytest.raises(ValueError):
            stage_config_to_table(sql, "run\\", {"checks": []})
        sql.upsert.assert_not_called()


class TestRunConfigTooLargeError:
    def test_message_reports_size_and_limit(self) -> None:
        err = RunConfigTooLargeError(12345)
        assert "12345" in str(err)
        assert str(JOB_PARAMETERS_CHAR_LIMIT) in str(err)
        assert isinstance(err, RunConfigError)

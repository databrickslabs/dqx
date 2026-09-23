"""Tests for oversized run-config staging (Databricks 10k job-parameter limit)."""

import json
from unittest.mock import create_autospec

from databricks_labs_dqx_app.backend.run_config_store import (
    JOB_PARAMETERS_CHAR_LIMIT,
    MANIFEST_CONFIG_KEY,
    RunConfigTooLargeError,
    job_parameters_size,
    prepare_config_json,
    stage_config_to_table,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


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


def _sql_mock():
    sql = create_autospec(SqlExecutor, instance=True)
    sql.fqn.return_value = "`cat`.`sch`.dq_run_configs"
    return sql


class TestJobParametersSize:
    def test_counts_json_representation(self) -> None:
        params = {**_base_params(), "config_json": '{"checks":[]}'}
        assert job_parameters_size(params) == len(json.dumps(params, separators=(",", ":")))


class TestPrepareConfigJson:
    def test_inline_when_under_limit(self) -> None:
        sql = _sql_mock()
        config = {"checks": [{"name": "c1"}]}
        result = prepare_config_json(
            sql,
            run_id="run123",
            config=config,
            job_parameters_without_config=_base_params(),
        )
        assert MANIFEST_CONFIG_KEY not in json.loads(result)
        assert json.loads(result) == config
        sql.execute.assert_not_called()

    def test_stages_when_over_limit(self) -> None:
        sql = _sql_mock()
        big_checks = [
            {"name": f"rule_{i}", "check": {"function": "is_not_null", "arguments": {"col": "x"}}} for i in range(200)
        ]
        config = {"checks": big_checks, "sample_size": 1000}
        base = _base_params()
        inline = json.dumps(config, separators=(",", ":"))
        assert job_parameters_size({**base, "config_json": inline}) > JOB_PARAMETERS_CHAR_LIMIT

        result = prepare_config_json(
            sql,
            run_id="run123",
            config=config,
            job_parameters_without_config=base,
        )
        stub = json.loads(result)
        assert stub == {MANIFEST_CONFIG_KEY: True}
        sql.execute.assert_called_once()

        stmt = sql.execute.call_args.args[0]
        assert stmt.startswith("INSERT INTO `cat`.`sch`.dq_run_configs")
        assert "run123" in stmt
        assert "rule_0" in stmt

    def test_stub_stays_within_the_job_parameter_limit(self) -> None:
        sql = _sql_mock()
        config = {"checks": [{"name": f"rule_{i}"} for i in range(500)]}
        result = prepare_config_json(
            sql,
            run_id="run123",
            config=config,
            job_parameters_without_config=_base_params(),
        )
        assert job_parameters_size({**_base_params(), "config_json": result}) <= JOB_PARAMETERS_CHAR_LIMIT


class TestStageConfigToTable:
    def test_writes_escaped_json_payload(self) -> None:
        sql = _sql_mock()
        config = {"checks": [{"name": "it's", "pattern": "\\d+"}]}
        stage_config_to_table(sql, "run123", config)
        stmt = sql.execute.call_args.args[0]
        assert "\\\\d+" in stmt  # backslash doubled
        assert "it''s" in stmt  # single-quote doubled


class TestRunConfigTooLargeError:
    def test_message_reports_size_and_limit(self) -> None:
        err = RunConfigTooLargeError(12345)
        assert "12345" in str(err)
        assert str(JOB_PARAMETERS_CHAR_LIMIT) in str(err)

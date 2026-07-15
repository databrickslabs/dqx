"""Tests for oversized run-config staging (Databricks 10k job-parameter limit)."""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock

import pytest

from databricks_labs_dqx_app.backend.run_config_store import (
    JOB_PARAMETERS_CHAR_LIMIT,
    STAGED_CONFIG_KEY,
    RunConfigTooLargeError,
    job_parameters_size,
    prepare_config_json,
    resolve_config,
    staged_config_path,
)


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


class TestJobParametersSize:
    def test_counts_json_representation(self) -> None:
        params = {**_base_params(), "config_json": '{"checks":[]}'}
        assert job_parameters_size(params) == len(json.dumps(params, separators=(",", ":")))


class TestPrepareConfigJson:
    def test_inline_when_under_limit(self) -> None:
        ws = MagicMock()
        config = {"checks": [{"name": "c1"}]}
        result = prepare_config_json(
            ws,
            wheels_volume="/Volumes/cat/sch/wheels",
            run_id="run123",
            config=config,
            job_parameters_without_config=_base_params(),
        )
        assert STAGED_CONFIG_KEY not in json.loads(result)
        ws.files.upload.assert_not_called()

    def test_stages_when_over_limit(self) -> None:
        ws = MagicMock()
        big_checks = [{"name": f"rule_{i}", "check": {"function": "is_not_null", "arguments": {"col": "x"}}} for i in range(200)]
        config = {"checks": big_checks, "sample_size": 1000}
        base = _base_params()
        inline = json.dumps(config, separators=(",", ":"))
        assert job_parameters_size({**base, "config_json": inline}) > JOB_PARAMETERS_CHAR_LIMIT

        result = prepare_config_json(
            ws,
            wheels_volume="/Volumes/cat/sch/wheels",
            run_id="run123",
            config=config,
            job_parameters_without_config=base,
        )
        stub = json.loads(result)
        assert STAGED_CONFIG_KEY in stub
        assert stub[STAGED_CONFIG_KEY] == staged_config_path("/Volumes/cat/sch/wheels", "run123")
        ws.files.upload.assert_called_once()
        upload_path = ws.files.upload.call_args.args[0]
        assert upload_path.endswith("/run-configs/run123.json")
        uploaded = ws.files.upload.call_args.args[1].read().decode()
        assert json.loads(uploaded)["checks"][0]["name"] == "rule_0"

    def test_raises_when_over_limit_and_no_volume(self) -> None:
        ws = MagicMock()
        big_checks = [{"name": f"rule_{i}", "payload": "x" * 80} for i in range(200)]
        config = {"checks": big_checks}
        with pytest.raises(RunConfigTooLargeError):
            prepare_config_json(
                ws,
                wheels_volume="",
                run_id="run123",
                config=config,
                job_parameters_without_config=_base_params(),
            )


class TestResolveConfig:
    def test_passthrough_inline_config(self) -> None:
        ws = MagicMock()
        config = {"checks": [], "sample_size": 100}
        resolved, path = resolve_config(ws, config)
        assert resolved == config
        assert path is None

    def test_loads_staged_config(self) -> None:
        ws = MagicMock()
        full = {"checks": [{"name": "c1"}], "sample_size": 50}
        ws.files.download.return_value.contents = io.BytesIO(json.dumps(full).encode())
        resolved, path = resolve_config(ws, {STAGED_CONFIG_KEY: "/Volumes/c/s/w/run-configs/r1.json"})
        assert resolved == full
        assert path == "/Volumes/c/s/w/run-configs/r1.json"

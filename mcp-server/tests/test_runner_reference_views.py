"""Tests for the runner's reference-view handling and temp-view cleanup.

Two layers:

* ``dqx_mcp_runner.temp_views`` is dependency-free, so its tests always run (including in the
  mcp-server test environment, which has no pyspark).
* The runner operations and ``main`` import pyspark and DQX, which are not installed in that
  environment, so those tests skip there and run wherever both are available (e.g. the app venv).
  They never start Spark or touch a workspace: Spark and the workspace client are autospec mocks
  injected through the public ``main`` seams, and ``DQEngine`` — the third-party boundary that would
  otherwise need a real session — is replaced with an autospec.
"""

import json
from collections.abc import Mapping
from types import ModuleType
from typing import NoReturn
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from dqx_mcp_runner.temp_views import (
    collect_temp_views,
    drop_temp_views,
    ensure_views_in_namespace,
    parse_ref_views,
    trusted_view_namespace,
)

NAMESPACE = ("main", "dqx_mcp_tmp")
RESULTS_VOLUME = "/Volumes/main/dqx_mcp_tmp/mcp_results"
MONITORED = "main.dqx_mcp_tmp.v_1_aaa"
REF_A = "main.dqx_mcp_tmp.v_2_bbb"
REF_B = "main.dqx_mcp_tmp.v_3_ccc"


class RecordingSql:
    """Minimal SqlExecutor: records statements and can fail on chosen ones."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.statements: list[str] = []
        self._fail_on = fail_on

    def sql(self, query: str, /) -> object:
        self.statements.append(query)
        if self._fail_on and self._fail_on in query:
            raise RuntimeError("boom")
        return object()


class TestTrustedViewNamespace:
    def test_derived_from_results_volume(self) -> None:
        assert trusted_view_namespace(RESULTS_VOLUME) == NAMESPACE
        assert trusted_view_namespace("/Volumes/Main/Tmp/vol/") == ("main", "tmp")

    @pytest.mark.parametrize("path", ["", "/tmp/x/y/z", "/Volumes/main/schema", "/Volumes/ma-in/s/v", "Volumes/a/b"])
    def test_untrusted_paths_yield_none(self, path: str) -> None:
        assert trusted_view_namespace(path) is None


class TestCollectTempViews:
    def test_monitored_and_reference_views_deduplicated_in_order(self) -> None:
        params = {"view_name": MONITORED, "ref_views": {"self": MONITORED, "a": REF_A, "a_again": REF_A, "b": REF_B}}
        assert collect_temp_views(params) == [MONITORED, REF_A, REF_B]

    def test_absent_and_malformed_values_are_ignored(self) -> None:
        assert collect_temp_views({}) == []
        assert collect_temp_views({"view_name": MONITORED, "ref_views": "nope"}) == [MONITORED]
        assert collect_temp_views({"ref_views": {"a": REF_A, "b": 5, "c": ""}}) == [REF_A]

    def test_parse_ref_views_rejects_bad_shape(self) -> None:
        assert parse_ref_views({}) == {}
        assert parse_ref_views({"ref_views": {"a": REF_A}}) == {"a": REF_A}
        for bad in ("x", ["a"], {"a": 1}):
            with pytest.raises(ValueError, match="ref_views"):
                parse_ref_views({"ref_views": bad})


class TestDropTempViews:
    def test_drops_each_view_once_quoted(self) -> None:
        spark = RecordingSql()
        drop_temp_views(spark, [MONITORED, REF_A, MONITORED], NAMESPACE)
        assert spark.statements == [
            "DROP VIEW IF EXISTS `main`.`dqx_mcp_tmp`.`v_1_aaa`",
            "DROP VIEW IF EXISTS `main`.`dqx_mcp_tmp`.`v_2_bbb`",
        ]

    def test_one_failed_drop_does_not_stop_the_others(self) -> None:
        spark = RecordingSql(fail_on="v_1_aaa")
        drop_temp_views(spark, [MONITORED, REF_A, REF_B], NAMESPACE)
        assert len(spark.statements) == 3

    def test_views_outside_namespace_or_malformed_are_never_dropped(self) -> None:
        spark = RecordingSql()
        drop_temp_views(
            spark,
            ["other.schema.v", "main.dqx_mcp_tmp.v;DROP", "just_a_name", "main.dqx_mcp_tmp.regular_view", REF_A],
            NAMESPACE,
        )
        assert spark.statements == ["DROP VIEW IF EXISTS `main`.`dqx_mcp_tmp`.`v_2_bbb`"]

    def test_nothing_dropped_without_a_trusted_namespace(self) -> None:
        spark = RecordingSql()
        drop_temp_views(spark, [MONITORED], None)
        assert not spark.statements

    @pytest.mark.parametrize("name", ["regular_view", "v_bad_abc", "v_123_xyz", "v_123_abc_extra"])
    def test_rejects_non_generated_names_even_in_the_right_schema(self, name: str) -> None:
        view = f"main.dqx_mcp_tmp.{name}"
        with pytest.raises(ValueError, match="generated view"):
            ensure_views_in_namespace([view], NAMESPACE)
        spark = RecordingSql()
        drop_temp_views(spark, [view], NAMESPACE)
        assert spark.statements == []

    def test_ensure_views_in_namespace(self) -> None:
        ensure_views_in_namespace([MONITORED, REF_A], NAMESPACE)
        with pytest.raises(ValueError, match="namespace"):
            ensure_views_in_namespace(["other.schema.v"], NAMESPACE)
        with pytest.raises(ValueError, match="namespace"):
            ensure_views_in_namespace([MONITORED], None)


# --- Runner operations and entry point (need pyspark + DQX; skipped where they are absent). ---


@pytest.fixture(name="runner")
def runner_module() -> ModuleType:
    return pytest.importorskip("dqx_mcp_runner.runner")


@pytest.fixture(name="engine")
def fake_engine(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace DQEngine (third-party boundary that needs a live session) with an autospec instance."""
    engine_module = pytest.importorskip("databricks.labs.dqx.engine")
    instance = create_autospec(engine_module.DQEngine, instance=True)
    valid_df, invalid_df = MagicMock(name="valid_df"), MagicMock(name="invalid_df")
    for frame in (valid_df, invalid_df):
        frame.count.return_value = 0
        frame.limit.return_value.collect.return_value = []
    instance.apply_checks_by_metadata_and_split.return_value = (valid_df, invalid_df)
    monkeypatch.setattr(engine_module, "DQEngine", lambda *args, **kwargs: instance)
    return instance


@pytest.fixture(name="spark")
def spark_mock(runner: ModuleType) -> MagicMock:
    spark = create_autospec(runner.SparkSession, instance=True)
    frames: dict[str, MagicMock] = {}

    def table(name: str) -> MagicMock:
        if name not in frames:
            frames[name] = MagicMock(name=f"df:{name}")
            frames[name].count.return_value = 0
        return frames[name]

    spark.table.side_effect = table
    spark.frames = frames
    return spark


@pytest.fixture(name="ws")
def workspace_mock() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


def _argv(operation: str, params: Mapping[str, object]) -> list[str]:
    return [
        "--operation",
        operation,
        "--params",
        json.dumps(params),
        "--run-id",
        "42",
        "--results-volume",
        RESULTS_VOLUME,
    ]


def _dropped(spark: MagicMock) -> list[str]:
    return [c.args[0] for c in spark.sql.call_args_list if c.args[0].startswith("DROP VIEW")]


def _run_main(runner: ModuleType, argv: list[str], spark: MagicMock, ws: MagicMock) -> None:
    runner.main(argv, spark_factory=lambda: spark, workspace_client_factory=lambda: ws)


class TestReferenceDataFrameResolution:
    def test_run_checks_passes_resolved_ref_dfs(
        self, runner: ModuleType, engine: MagicMock, spark: MagicMock, ws: MagicMock
    ) -> None:
        params = {"view_name": MONITORED, "checks": [{"check": {}}], "ref_views": {"orders": REF_A, "users": REF_B}}

        runner.run_checks(spark, ws, params)

        args, kwargs = engine.apply_checks_by_metadata_and_split.call_args
        assert args[0] is spark.frames[MONITORED]
        assert kwargs["ref_dfs"] == {"orders": spark.frames[REF_A], "users": spark.frames[REF_B]}

    def test_run_checks_without_ref_views_passes_no_ref_dfs(
        self, runner: ModuleType, engine: MagicMock, spark: MagicMock, ws: MagicMock
    ) -> None:
        runner.run_checks(spark, ws, {"view_name": MONITORED, "checks": []})

        assert engine.apply_checks_by_metadata_and_split.call_args.kwargs["ref_dfs"] is None

    def test_apply_checks_and_save_passes_resolved_ref_dfs(
        self, runner: ModuleType, engine: MagicMock, spark: MagicMock, ws: MagicMock
    ) -> None:
        params = {
            "view_name": MONITORED,
            "checks": [],
            "output_name": "out",
            "catalog": "main",
            "grant_to": "alice@example.com",
            "ref_views": {"orders": REF_A},
        }

        runner.apply_checks_and_save_to_table(spark, ws, params)

        kwargs = engine.apply_checks_by_metadata_and_save_in_table.call_args.kwargs
        assert kwargs["ref_dfs"] == {"orders": spark.frames[REF_A]}
        assert kwargs["input_config"].location == MONITORED

    def test_malformed_ref_views_is_rejected(
        self, runner: ModuleType, engine: MagicMock, spark: MagicMock, ws: MagicMock
    ) -> None:
        with pytest.raises(ValueError, match="ref_views"):
            runner.run_checks(spark, ws, {"view_name": MONITORED, "checks": [], "ref_views": ["x"]})


class TestEntrypointCleanup:
    PARAMS = {
        "view_name": MONITORED,
        "checks": [],
        "ref_views": {"self": MONITORED, "orders": REF_A, "users": REF_B},
    }

    def test_success_writes_result_and_drops_every_view_once(
        self, runner: ModuleType, engine: MagicMock, spark: MagicMock, ws: MagicMock
    ) -> None:
        _run_main(runner, _argv("run_checks", self.PARAMS), spark, ws)

        assert sorted(_dropped(spark)) == [
            f"DROP VIEW IF EXISTS `main`.`dqx_mcp_tmp`.`{view}`" for view in ("v_1_aaa", "v_2_bbb", "v_3_ccc")
        ]
        assert ws.files.upload.call_args.args[0] == f"{RESULTS_VOLUME}/42.json"

    def test_operation_failure_drops_every_view_and_writes_no_result(
        self, runner: ModuleType, engine: MagicMock, spark: MagicMock, ws: MagicMock
    ) -> None:
        engine.apply_checks_by_metadata_and_split.side_effect = RuntimeError("engine failed")

        with pytest.raises(RuntimeError, match="engine failed"):
            _run_main(runner, _argv("run_checks", self.PARAMS), spark, ws)

        assert len(_dropped(spark)) == 3
        ws.files.upload.assert_not_called()

    def test_failed_drop_does_not_stop_remaining_drops(
        self, runner: ModuleType, engine: MagicMock, spark: MagicMock, ws: MagicMock
    ) -> None:
        def sql(statement: str) -> None:
            if "v_1_aaa" in statement:
                raise RuntimeError("drop failed")

        spark.sql.side_effect = sql

        _run_main(runner, _argv("run_checks", self.PARAMS), spark, ws)

        assert len(_dropped(spark)) == 3

    def test_workspace_client_failure_still_drops_views(self, runner: ModuleType, spark: MagicMock) -> None:
        def broken_client() -> NoReturn:
            raise RuntimeError("no workspace auth")

        with pytest.raises(RuntimeError, match="no workspace auth"):
            runner.main(
                _argv("run_checks", self.PARAMS), spark_factory=lambda: spark, workspace_client_factory=broken_client
            )

        assert len(_dropped(spark)) == 3

    def test_result_upload_failure_still_drops_every_view(
        self, runner: ModuleType, engine: MagicMock, spark: MagicMock, ws: MagicMock
    ) -> None:
        ws.files.upload.side_effect = RuntimeError("result upload failed")
        with pytest.raises(RuntimeError, match="result upload failed"):
            _run_main(runner, _argv("run_checks", self.PARAMS), spark, ws)
        assert len(_dropped(spark)) == 3

    def test_malformed_reference_map_does_not_prevent_valid_view_cleanup(
        self, runner: ModuleType, engine: MagicMock, spark: MagicMock, ws: MagicMock
    ) -> None:
        params = {"view_name": MONITORED, "checks": [], "ref_views": {"valid": REF_A, "invalid": 123}}
        with pytest.raises(ValueError, match="ref_views"):
            _run_main(runner, _argv("run_checks", params), spark, ws)
        assert len(_dropped(spark)) == 2
        ws.files.upload.assert_not_called()

    def test_unknown_operation_still_drops_views(self, runner: ModuleType, spark: MagicMock, ws: MagicMock) -> None:
        with pytest.raises(ValueError, match="Unknown operation"):
            _run_main(runner, _argv("nope", self.PARAMS), spark, ws)

        assert len(_dropped(spark)) == 3

    def test_view_outside_results_namespace_is_neither_used_nor_dropped(
        self, runner: ModuleType, engine: MagicMock, spark: MagicMock, ws: MagicMock
    ) -> None:
        params = {"view_name": MONITORED, "checks": [], "ref_views": {"evil": "other_catalog.other_schema.t"}}

        with pytest.raises(ValueError, match="namespace"):
            _run_main(runner, _argv("run_checks", params), spark, ws)

        engine.apply_checks_by_metadata_and_split.assert_not_called()
        assert _dropped(spark) == ["DROP VIEW IF EXISTS `main`.`dqx_mcp_tmp`.`v_1_aaa`"]

    def test_spark_initialisation_failure_is_not_recovered(self, runner: ModuleType, ws: MagicMock) -> None:
        workspace_factory = MagicMock(return_value=ws)

        def broken_spark() -> NoReturn:
            raise RuntimeError("no spark")

        with pytest.raises(RuntimeError, match="no spark"):
            runner.main(
                _argv("run_checks", self.PARAMS), spark_factory=broken_spark, workspace_client_factory=workspace_factory
            )

        workspace_factory.assert_not_called()

    def test_invalid_params_json_is_not_recovered(self, runner: ModuleType, ws: MagicMock) -> None:
        spark_factory = MagicMock()
        argv = _argv("run_checks", {})
        argv[argv.index("--params") + 1] = "{not json"

        with pytest.raises(json.JSONDecodeError):
            runner.main(argv, spark_factory=spark_factory, workspace_client_factory=lambda: ws)

        spark_factory.assert_not_called()

import logging
import os
from datetime import datetime, timezone
from io import BytesIO
import random
import string
from typing import Any
from unittest.mock import patch
from collections.abc import Generator, Callable

from chispa import assert_df_equality  # type: ignore
from pyspark.sql import DataFrame
import pytest
from databricks.sdk.service.workspace import ImportFormat
from databricks.labs.blueprint.installation import Installation
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.labs.dqx.checks_serializer import (
    generate_rule_set_fingerprint_from_dict,
    serialize_checks,
    deserialize_checks,
)
from databricks.labs.dqx.checks_storage import InstallationChecksStorageHandler
from databricks.labs.dqx.config import InputConfig, OutputConfig, InstallationChecksStorageConfig, ExtraParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.installer.mixins import InstallationMixin
from databricks.labs.dqx.rule import DQRule
from databricks.labs.dqx.schema import dq_result_schema
from databricks.sdk.service.catalog import SchemaInfo
from databricks.sdk.service.compute import DataSecurityMode, Kind

from tests.conftest import TEST_CATALOG


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


REPORTING_COLUMNS = f", _errors: {dq_result_schema.simpleString()}, _warnings: {dq_result_schema.simpleString()}"
RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
RUN_ID = "2f9120cf-e9f2-446a-8278-12d508b00639"
EXTRA_PARAMS = ExtraParams(run_time_overwrite=RUN_TIME.isoformat(), run_id_overwrite=RUN_ID)


def build_quality_violation(
    name: str,
    message: str,
    columns: list[str] | None,
    rules: list[dict] | None = None,
    *,
    function: str = "is_not_null_and_not_empty",
) -> dict[str, Any]:
    """Helper for constructing expected violation entries with shared metadata."""

    return {
        "name": name,
        "message": message,
        "columns": columns,
        "filter": None,
        "function": function,
        "run_time": RUN_TIME,
        "run_id": RUN_ID,
        "rule_fingerprint": get_rule_fingerprint_from_checks(rules, name, function),
        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(rules, name, function),
        "user_metadata": {},
    }


@pytest.fixture
def webbrowser_open():
    with patch("webbrowser.open") as mock_open:
        yield mock_open


@pytest.fixture
def setup_workflows(ws, spark, installation_ctx, make_schema, make_table, make_random):
    """
    Set up the workflows with serverless cluster for the tests in the workspace.
    """

    if os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID"):
        pytest.skip()

    def create(_spark, **kwargs):
        installation_ctx.installation_service.run()

        quarantine = False
        if "quarantine" in kwargs and kwargs["quarantine"]:
            quarantine = True

        checks_location = None
        if "checks" in kwargs and kwargs["checks"]:
            checks_location = _setup_quality_checks(installation_ctx, _spark, ws)

        run_config = _setup_workflows_deps(
            installation_ctx, make_schema, make_table, make_random, checks_location, quarantine
        )
        return installation_ctx, run_config

    def delete(resource) -> None:
        ctx, run_config = resource
        checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
        ws.workspace.delete(checks_location)

    yield from factory("workflows", lambda **kw: create(spark, **kw), delete)


@pytest.fixture
def setup_serverless_workflows(ws, spark, serverless_installation_ctx, make_schema, make_table, make_random):
    """
    Set up the workflows with serverless cluster for the tests in the workspace.
    """

    if not os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID"):
        pytest.skip()

    def create(_spark, **kwargs):
        serverless_installation_ctx.installation_service.run()

        quarantine = False
        if "quarantine" in kwargs and kwargs["quarantine"]:
            quarantine = True

        checks_location = None
        if "checks" in kwargs and kwargs["checks"]:
            checks_location = _setup_quality_checks(serverless_installation_ctx, _spark, ws)

        run_config = _setup_workflows_deps(
            serverless_installation_ctx,
            make_schema,
            make_table,
            make_random,
            checks_location,
            quarantine,
            is_streaming=kwargs.get("is_streaming", False),
        )
        return serverless_installation_ctx, run_config

    def delete(resource) -> None:
        ctx, run_config = resource
        checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
        ws.workspace.delete(checks_location)

    yield from factory("workflows", lambda **kw: create(spark, **kw), delete)


@pytest.fixture
def setup_workflows_with_metrics(ws, spark, installation_ctx, make_schema, make_table, make_cluster, make_random):
    """Set up workflows with metrics configuration for testing."""

    if os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID"):
        pytest.skip()

    def create(_spark, **kwargs):
        cluster = make_cluster(
            single_node=True,
            kind=Kind.CLASSIC_PREVIEW,
            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
        )
        cluster_id = cluster.cluster_id
        installation_ctx.config.serverless_clusters = False
        installation_ctx.config.profiler_override_clusters["default"] = cluster_id
        installation_ctx.config.quality_checker_override_clusters["default"] = cluster_id
        installation_ctx.config.e2e_override_clusters["default"] = cluster_id
        installation_ctx.installation_service.run()

        quarantine = False
        if "quarantine" in kwargs and kwargs["quarantine"]:
            quarantine = True

        checks_location = _setup_quality_checks(installation_ctx, _spark, ws)

        run_config = _setup_workflows_deps(
            installation_ctx,
            make_schema,
            make_table,
            make_random,
            checks_location,
            quarantine,
            is_streaming=kwargs.get("is_streaming", False),
            is_continuous_streaming=kwargs.get("is_continuous_streaming", False),
        )

        config = installation_ctx.config
        run_config = config.get_run_config()

        catalog_name = TEST_CATALOG
        schema_name = run_config.output_config.location.split(".")[1]
        metrics_table_name = f"{catalog_name}.{schema_name}.metrics_{make_random(6).lower()}"
        run_config.metrics_config = OutputConfig(location=metrics_table_name)

        custom_metrics = kwargs.get("custom_metrics")
        if custom_metrics:
            config.custom_metrics = custom_metrics

        installation_ctx.installation.save(config)

        return installation_ctx, run_config

    def delete(resource):
        ctx, run_config = resource
        checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
        try:
            ws.workspace.delete(checks_location)
        except Exception:
            pass

    yield from factory("workflows_with_metrics", lambda **kw: create(spark, **kw), delete)


@pytest.fixture
def setup_workflows_with_custom_folder(
    ws, spark, installation_ctx_custom_install_folder, make_schema, make_table, make_random
):
    """
    Set up the workflows with installation in the custom install folder.
    """

    if os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID"):
        pytest.skip()

    def create(_spark, **kwargs):
        installation_ctx_custom_install_folder.installation_service.run()

        quarantine = False
        if "quarantine" in kwargs and kwargs["quarantine"]:
            quarantine = True

        checks_location = None
        if "checks" in kwargs and kwargs["checks"]:
            checks_location = _setup_quality_checks(installation_ctx_custom_install_folder, _spark, ws)

        run_config = _setup_workflows_deps(
            installation_ctx_custom_install_folder, make_schema, make_table, make_random, checks_location, quarantine
        )
        return installation_ctx_custom_install_folder, run_config

    def delete(resource) -> None:
        ctx, run_config = resource
        checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
        ws.workspace.delete(checks_location)

    yield from factory("workflows", lambda **kw: create(spark, **kw), delete)


class TestInstallationMixin(InstallationMixin):
    def get_my_username(self):
        return self._my_username

    def get_me(self):
        return self._me

    def get_installation(
        self, product_name: str, assume_user: bool = True, install_folder: str | None = None
    ) -> Installation:
        return self._get_installation(product_name, assume_user, install_folder)


def _setup_workflows_deps(
    ctx,
    make_schema,
    make_table,
    make_random,
    checks_location: str | None = None,
    quarantine: bool = False,
    is_streaming: bool = False,
    is_continuous_streaming: bool = False,
):
    # prepare test data
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    input_table = make_table(
        catalog_name=catalog_name,
        schema_name=schema.name,
        # sample data
        ctas="SELECT * FROM VALUES "
        "(1, 'a'), (2, 'b'), (3, NULL), (NULL, 'c'), (3, NULL), (1, 'a'), (6, 'a'), (2, 'c'), (4, 'a'), (5, 'd') "
        "AS data(id, name)",
    )

    # update input and output locations
    config = ctx.config
    config.extra_params = EXTRA_PARAMS

    run_config = config.get_run_config()
    run_config.input_config = InputConfig(
        location=input_table.full_name,
        options={"versionAsOf": "0"} if not is_streaming else {},
        is_streaming=is_streaming,
    )

    trigger: dict[str, Any] = {}
    if is_streaming:
        if is_continuous_streaming:
            trigger = {"processingTime": "60 seconds"}
        else:
            trigger = {"availableNow": True}

    output_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"
    run_config.output_config = OutputConfig(
        location=output_table,
        trigger=trigger,
        options=({"checkpointLocation": f"/tmp/dqx_tests/{make_random(10)}_out_ckpt"} if is_streaming else {}),
    )

    if checks_location:
        run_config.checks_location = checks_location

    if quarantine:
        quarantine_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}_quarantine"
        run_config.quarantine_config = OutputConfig(
            location=quarantine_table,
            trigger=trigger,
            options=({"checkpointLocation": f"/tmp/dqx_tests/{make_random(10)}_qr_ckpt"} if is_streaming else {}),
        )

    # ensure tests are deterministic
    run_config.profiler_config.sample_fraction = 1.0
    run_config.profiler_config.sample_seed = 100

    ctx.installation.save(ctx.config)

    return run_config


@pytest.fixture
def expected_quality_checking_output(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            [1, "a", None, None],
            [2, "b", None, None],
            [
                3,
                None,
                [
                    build_quality_violation(
                        "name_is_not_null_and_not_empty", "Column 'name' value is null or empty", ["name"]
                    )
                ],
                None,
            ],
            [
                None,
                "c",
                [
                    build_quality_violation(
                        "id_is_not_null", "Column 'id' value is null", ["id"], function="is_not_null"
                    )
                ],
                None,
            ],
            [
                3,
                None,
                [
                    build_quality_violation(
                        "name_is_not_null_and_not_empty",
                        "Column 'name' value is null or empty",
                        ["name"],
                    )
                ],
                None,
            ],
            [1, "a", None, None],
            [6, "a", None, None],
            [2, "c", None, None],
            [4, "a", None, None],
            [5, "d", None, None],
        ],
        f"id int, name string {REPORTING_COLUMNS}",
    )


def _setup_quality_checks(ctx, spark, ws):
    config = ctx.config
    checks_location = config.get_run_config().checks_location
    checks = [
        {
            "name": "id_is_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
        {
            "name": "name_is_not_null_and_not_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "name"}},
        },
    ]
    config = InstallationChecksStorageConfig(
        location=checks_location,
        product_name=ctx.installation.product(),
        install_folder=ctx.installation.install_folder(),
    )

    InstallationChecksStorageHandler(ws, spark).save(checks=checks, config=config)
    return checks_location


def setup_custom_check_func(ws, installation_ctx, custom_checks_funcs_location):
    content = '''from databricks.labs.dqx.check_funcs import make_condition, register_rule
from pyspark.sql import functions as F

@register_rule("row")
def not_ends_with_suffix(column: str, suffix: str):
    """
    Example of custom python row-level check function.
    """
    return make_condition(
        F.col(column).endswith(suffix), f"Column '{column}' ends with '{suffix}'", f"{column}_ends_with_{suffix}"
    )
'''
    if custom_checks_funcs_location.startswith("/Workspace/"):
        ws.workspace.upload(
            path=custom_checks_funcs_location, format=ImportFormat.AUTO, content=content.encode(), overwrite=True
        )
    elif custom_checks_funcs_location.startswith("/Volumes/"):
        binary_data = BytesIO(content.encode("utf-8"))
        ws.files.upload(custom_checks_funcs_location, binary_data, overwrite=True)
    else:  # relative workspace path
        installation_dir = installation_ctx.installation.install_folder()
        ws.workspace.upload(
            path=f"{installation_dir}/{custom_checks_funcs_location}",
            format=ImportFormat.AUTO,
            content=content.encode(),
            overwrite=True,
        )

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.custom_check_functions = {"not_ends_with_suffix": custom_checks_funcs_location}
    installation_ctx.installation.save(config)


def contains_expected_workflows(workflows, state):
    for workflow in workflows:
        if all(item in workflow.items() for item in state.items()):
            return True
    return False


def assert_quarantine_and_output_dfs(ws, spark, expected_output, output_config, quarantine_config):
    dq_engine = DQEngine(ws, spark)
    expected_output_df = dq_engine.get_valid(expected_output)
    expected_quarantine_df = dq_engine.get_invalid(expected_output)

    output_df = spark.table(output_config.location)
    assert_df_equality(output_df, expected_output_df, ignore_nullable=True)

    quarantine_df = spark.table(quarantine_config.location)
    assert_df_equality(quarantine_df, expected_quarantine_df, ignore_nullable=True)


def assert_output_df(spark, expected_output, output_config):
    checked_df = spark.table(output_config.location)
    assert_df_equality(checked_df, expected_output, ignore_nullable=True)


@pytest.fixture
def make_schema_seed(
    sql_backend,
    log_workspace_link,
    watchdog_remove_after,
) -> Generator[Callable[..., SchemaInfo], None, None]:
    """
    Create a schema and return its info. Remove it after the test. Returns instance of `databricks.sdk.service.catalog.SchemaInfo`.

    Keyword Arguments:
    * `catalog_name` (str): The name of the catalog where the schema will be created. Default is `hive_metastore`.
    * `name` (str): The name of the schema. Default is a random string.
    * `location` (str): The path to the location if it should be a managed schema.

    Usage:
    ```python
    def test_catalog_fixture(make_catalog, make_schema, make_table):
        from_catalog = make_catalog()
        from_schema = make_schema(catalog_name=from_catalog.name)
        from_table_1 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
        logger.info(f"Created new schema: {from_table_1}")
    ```
    """

    def create(
        *, catalog_name: str = "hive_metastore", name: str | None = None, location: str | None = None
    ) -> SchemaInfo:
        name = name or f"dummy_s{generate_random_name_seed(8,32)}".lower()
        full_name = f"{catalog_name}.{name}".lower()
        schema_ddl = f"CREATE SCHEMA {full_name}"
        if location:
            schema_ddl = f"{schema_ddl} LOCATION '{location}'"
        schema_ddl = f"{schema_ddl} WITH DBPROPERTIES (RemoveAfter={watchdog_remove_after})"
        sql_backend.execute(schema_ddl)
        schema_info = SchemaInfo(catalog_name=catalog_name, name=name, full_name=full_name, storage_location=location)
        path = f'explore/data/{schema_info.catalog_name}/{schema_info.name}'
        log_workspace_link(f'{schema_info.full_name} schema', path)
        return schema_info

    def remove(schema_info: SchemaInfo):
        sql_backend.execute(f"DROP SCHEMA IF EXISTS {schema_info.full_name} CASCADE")

    yield from factory("schema", create, remove)


def generate_random_name_seed(length: int, seed: int) -> str:
    rng = random.Random(seed)
    charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
    return ''.join(rng.choice(charset) for _ in range(length))


def generate_rule_and_set_fingerprint_from_rules(rules: list, custom_checks: dict | None = None) -> list[dict]:
    if all(isinstance(rule, DQRule) for rule in rules):
        checks_dict = serialize_checks(rules)
    else:
        # serialize and deserialize to get check name to link it in the result dataframe.
        checks_rules = deserialize_checks(rules, custom_checks=custom_checks)
        checks_dict = serialize_checks(checks_rules)
    rule_fingerprint_checks = generate_rule_set_fingerprint_from_dict(checks_dict)
    return rule_fingerprint_checks


def get_rule_fingerprint_from_checks(
    versioning_rules_checks: list[dict] | None,
    check_name: str,
    function: str,
    criticality: str | None = None,
    columns: str | None = None,
) -> str | None:
    """
    Helper function to extract the rule_fingerprint from the versioning rules checks
    based on the check name, function, criticality and column (if applicable).
    versioning_rules_checks: list of versioning rules checks
    check_name: name of the check
    function: function of the check
    criticality: criticality of the check
    columns: column of the foreach check (Needed only for foreach checks with same name and function but different columns)
    """
    rule_dict = {}
    if not versioning_rules_checks:
        return None
    for check in versioning_rules_checks:
        if (
            check.get("name") == check_name
            and check["check"]["function"] == function
            and (criticality is None or check["criticality"] == criticality)
            and (
                columns is None
                or (
                    ("column" in check["check"]["arguments"] and check["check"]["arguments"]["column"] == columns[0])
                    or ("columns" in check["check"]["arguments"] and check["check"]["arguments"]["columns"] == columns)
                )
            )
        ):
            rule_dict = check

    return rule_dict.get("rule_fingerprint", None)


def get_rule_set_fingerprint_from_checks(
    versioning_rules_checks: list[dict] | None,
    check_name: str,
    function: str,
    criticality: str | None = None,
) -> str | None:
    rule_dict = {}
    if not versioning_rules_checks:
        return None
    for check in versioning_rules_checks:
        if (
            check.get("name") == check_name
            and check["check"]["function"] == function
            and (criticality is None or check["criticality"] == criticality)
        ):
            rule_dict = check

    return rule_dict.get("rule_set_fingerprint", None)

import sys
from databricks.labs.dqx.config import (
    InputConfig,
    ProfilerConfig,
    InstallationChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.profiler_runner import ProfilerRunner
from databricks.labs.dqx.profiler.profiler_workflow import ProfilerWorkflow


def test_profiler_runner_raise_error_when_profile_summary_stats_file_missing(ws, spark, installation_ctx):
    profiler = DQProfiler(ws)
    generator = DQGenerator(ws)
    dq_engine = DQEngine(ws, spark)
    runner = ProfilerRunner(ws, spark, dq_engine, installation_ctx.installation, profiler, generator)

    checks = [
        {
            "name": "col_a_is_null_or_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "a"}},
        },
    ]
    summary_stats = {
        'a': {
            'count': 3,
            'mean': 2.0,
            'stddev': 1.0,
            'min': 1,
            '25%': 1,
            '50%': 2,
            '75%': 3,
            'max': 3,
            'count_non_null': 3,
            'count_null': 0,
        }
    }
    install_folder = installation_ctx.installation.install_folder()
    checks_location = f"{install_folder}/checks.yml"
    profile_summary_stats_file = "profile_summary_stats.yml"
    storage_config = WorkspaceFileChecksStorageConfig(location=checks_location)

    runner.save(checks, summary_stats, storage_config, profile_summary_stats_file)

    checks_location_status = ws.workspace.get_status(f"{checks_location}")
    assert checks_location_status, f"Checks not uploaded to {checks_location}."

    summary_stats_file_status = ws.workspace.get_status(f"{install_folder}/{profile_summary_stats_file}")
    assert (
        summary_stats_file_status
    ), f"Profile summary stats not uploaded to {install_folder}/{profile_summary_stats_file}."


def test_profiler_runner(ws, spark, installation_ctx, make_schema, make_table):
    profiler = DQProfiler(ws)
    generator = DQGenerator(ws)
    dq_engine = DQEngine(ws, spark)
    runner = ProfilerRunner(ws, spark, dq_engine, installation_ctx.installation, profiler, generator)

    # prepare test data
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    table = make_table(
        catalog_name=catalog_name,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES (0, 'a'),(1, 'a'), (2, 'b'), (3, NULL)  AS data(id, name)",
    )
    input_config = InputConfig(location=table.full_name)
    profiler_config = ProfilerConfig(sample_fraction=1.0, filter="id > 0")

    checks, summary_stats = runner.run(input_config=input_config, profiler_config=profiler_config)

    assert checks, "Checks were not generated correctly"
    assert summary_stats, "Profile summary stats were not generated correctly"


def test_profiler_workflow_class(ws, spark, setup_workflows):
    installation_ctx, run_config = setup_workflows()

    sys.modules["pyspark.sql.session"] = spark
    ctx = installation_ctx.replace(run_config=run_config)

    ProfilerWorkflow().profile(ctx)  # type: ignore

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = DQEngine(ws).load_checks(config=config)

    assert checks, "Checks were not loaded correctly"


def test_profiler_workflow_class_serverless(ws, spark, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows()

    sys.modules["pyspark.sql.session"] = spark
    ctx = installation_ctx.replace(run_config=run_config)

    ProfilerWorkflow().profile(ctx)  # type: ignore

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = DQEngine(ws).load_checks(config=config)

    assert checks, "Checks were not loaded correctly"

import sys
import pytest

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.runner import ProfilerRunner
from databricks.labs.dqx.profiler.workflow import ProfilerWorkflow


def test_profiler_runner_save_raise_error_when_check_file_missing(ws, spark, installation_ctx):
    profiler = DQProfiler(ws)
    generator = DQGenerator(ws)
    runner = ProfilerRunner(ws, spark, installation_ctx.installation, profiler, generator)

    checks = []
    summary_stats = {}
    checks_file = None
    profile_summary_stats_file = "profile_summary_stats.yml"

    with pytest.raises(ValueError, match="Check file not configured"):
        runner.save(checks, summary_stats, checks_file, profile_summary_stats_file)


def test_profiler_runner_save_raise_error_when_profile_summary_stats_file_missing(ws, spark, installation_ctx):
    profiler = DQProfiler(ws)
    generator = DQGenerator(ws)
    runner = ProfilerRunner(ws, spark, installation_ctx.installation, profiler, generator)

    checks = []
    summary_stats = {}
    checks_file = "checks.yml"
    profile_summary_stats_file = None

    with pytest.raises(ValueError, match="Profile summary stats file not configured"):
        runner.save(checks, summary_stats, checks_file, profile_summary_stats_file)


def test_profiler_runner_raise_error_when_profile_summary_stats_file_missing(ws, spark, installation_ctx):
    profiler = DQProfiler(ws)
    generator = DQGenerator(ws)
    runner = ProfilerRunner(ws, spark, installation_ctx.installation, profiler, generator)

    checks = [
        {
            "name": "col_a_is_null_or_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "a"}},
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
    checks_file = "checks.yml"
    profile_summary_stats_file = "profile_summary_stats.yml"

    runner.save(checks, summary_stats, checks_file, profile_summary_stats_file)
    installation_ctx.installation.install_folder()

    install_folder = installation_ctx.installation.install_folder()
    checks_file_status = ws.workspace.get_status(f"{install_folder}/{checks_file}")
    assert checks_file_status, f"Checks not uploaded to {install_folder}/{checks_file}."

    summary_stats_file_status = ws.workspace.get_status(f"{install_folder}/{profile_summary_stats_file}")
    assert (
        summary_stats_file_status
    ), f"Profile summary stats not uploaded to {install_folder}/{profile_summary_stats_file}."


def test_profiler_runner(ws, spark, installation_ctx, make_schema, make_table, make_random):
    profiler = DQProfiler(ws)
    generator = DQGenerator(ws)
    runner = ProfilerRunner(ws, spark, installation_ctx.installation, profiler, generator)

    # prepare test data
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    table = make_table(
        catalog_name=catalog_name,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, NULL)  AS data(id, name)",
    )

    checks, summary_stats = runner.run(input_location=table.full_name)

    assert checks, "Checks were not generated correctly"
    assert summary_stats, "Profile summary stats were not generated correctly"


def test_profiler_workflow(ws, spark, setup_workflows):
    installation_ctx, run_config = setup_workflows

    sys.modules["pyspark.sql.session"] = spark
    ctx = installation_ctx.replace(run_config=run_config)

    ProfilerWorkflow().profile(ctx)  # type: ignore

    checks = DQEngine(ws).load_checks(
        run_config_name=run_config.name, assume_user=True, product_name=installation_ctx.installation.product()
    )
    assert checks, "Checks were not loaded correctly"

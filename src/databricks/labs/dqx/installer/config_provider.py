import os
import json
import logging
from databricks.labs.blueprint.tui import Prompts

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.installer.warehouse_installer import WarehouseInstaller
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig, InputConfig, OutputConfig, ProfilerConfig


class ConfigProvider:
    """
    Collects configuration from the user interactively.
    """

    def __init__(self, prompts: Prompts, warehouse_configurator: WarehouseInstaller, logger: logging.Logger):
        self._prompts = prompts
        self._warehouse_configurator = warehouse_configurator
        self.logger = logger

    def prompt_new_installation(self, install_folder: str | None = None) -> WorkspaceConfig:
        self.logger.info(
            "Please answer a couple of questions to provide default DQX run configuration. "
            "The configuration can also be updated manually after the installation."
        )

        # Show installation folder information
        if install_folder:
            self.logger.info(f"DQX will be installed in folder '{install_folder}'")
        else:
            install_path = (
                "/Applications/dqx" if os.getenv("DQX_FORCE_INSTALL") == "global" else "/Users/<your_user>/.dqx"
            )
            self.logger.info(f"DQX will be installed in the default location: '{install_path}'")

        log_level = self._prompts.question("Log level", default="INFO").upper()
        is_streaming = self._prompts.confirm("Should the input data be read using streaming?")
        input_config = self._prompt_input_config(is_streaming)
        output_config = self._prompt_output_config(is_streaming)
        quarantine_config = self._prompt_quarantine_config(is_streaming)

        metrics_config, custom_metrics = self._prompt_metrics()

        checks_location = self._prompts.question(
            "Provide location of the quality checks definitions, either:\n"
            "- a filename for storing data quality rules (e.g. checks.yml),\n"
            "- or a table for storing checks in the format `catalog.schema.table` or `schema.table`,\n"
            "- or a full volume path in the format /Volumes/catalog/schema/volume/<folder_path>/<file_name_with_extension>,\n",
            default="checks.yml",
            valid_regex=(
                r"^(?![^.]*\.[^.]*\.[^.]*\.)"  # Negative lookahead: Prevents more than three dot-separated segments
                r"(?:(?:[\w.-]+(?:/[\w.-]+)*/[\w.-]+\.[\w]+)"  # Relative file paths ending in a file with an extension
                r"|(?:\w+\.\w+\.\w+|\w+\.\w+)"  # Table names: either schema.table or catalog.schema.table
                r"|(?:/Volumes(?:/[\w.-]+)*/[\w.-]+\.[\w]+))$"  # Full volume path: must begin with /Volumes/
            ),
        )

        profiler_config = self._prompt_profiler_config()

        serverless_clusters = not self._prompts.confirm(
            "Do you want to use standard job clusters for the workflows execution (not Serverless)?"
        )

        (
            quality_checker_override_clusters,
            quality_checker_spark_conf,
            profiler_override_clusters,
            profiler_spark_conf,
            e2e_override_clusters,
            e2e_spark_conf,
        ) = (
            ({}, {}, {}, {}, {}, {}) if serverless_clusters else self._prompt_clusters_configs()
        )

        reference_tables_raw: dict[str, dict] = json.loads(
            self._prompts.question(
                "Provide reference tables to use for checks as a dictionary "
                "that maps reference table name to reference data location. "
                "The specification can contain fields from InputConfig such as: "
                "location, format, schema, options and is_streaming fields "
                "(e.g. {\"reference_vendor\":{\"location\": \"catalog.schema.table\", \"format\": \"delta\"}})",
                default="{}",
                valid_regex=r"^.*$",
            )
        )
        reference_tables: dict[str, InputConfig] = {
            key: InputConfig(**value) for key, value in reference_tables_raw.items()
        }

        custom_check_functions: dict[str, str] = json.loads(
            self._prompts.question(
                "Provide custom check functions as a dictionary "
                "that maps function name to a python module located in the workspace file "
                "(relative or absolute workspace path) or volume "
                "(e.g. {\"my_func\": \"/Workspace/Shared/my_module.py\"}), ",
                default="{}",
                valid_regex=r"^.*$",
            )
        )

        warehouse_id = self._warehouse_configurator.create()

        # Ask if the workspace blocks Internet access to determine if dependencies should be uploaded
        upload_dependencies = self._prompts.confirm("Does the given workspace block Internet access?")

        return WorkspaceConfig(
            log_level=log_level,
            run_configs=[
                RunConfig(
                    input_config=input_config,
                    output_config=output_config,
                    quarantine_config=quarantine_config,
                    metrics_config=metrics_config,
                    checks_location=checks_location,
                    warehouse_id=warehouse_id,
                    profiler_config=profiler_config,
                    custom_check_functions=custom_check_functions,
                    reference_tables=reference_tables,
                )
            ],
            serverless_clusters=serverless_clusters,
            upload_dependencies=upload_dependencies,
            profiler_spark_conf=profiler_spark_conf,
            profiler_override_clusters=profiler_override_clusters,
            quality_checker_spark_conf=quality_checker_spark_conf,
            quality_checker_override_clusters=quality_checker_override_clusters,
            e2e_spark_conf=e2e_spark_conf,
            e2e_override_clusters=e2e_override_clusters,
            custom_metrics=custom_metrics,
        )

    def _prompt_clusters_configs(self):
        profiler_spark_conf = json.loads(
            self._prompts.question(
                "Optional spark conf to use with the profiler workflow (e.g. {\"spark.sql.ansi.enabled\": \"true\"})",
                default="{}",
                valid_regex=r"^.*$",
            )
        )

        profiler_override_clusters = json.loads(
            self._prompts.question(
                "Optional Cluster ID to use for the profiler workflow (e.g. {\"default\": \"<existing-cluster-id>\"}). "
                "If not provided, a job cluster will be created automatically when the job runs",
                default="{}",
                valid_regex=r"^.*$",
            )
        )

        quality_checker_spark_conf = json.loads(
            self._prompts.question(
                "Optional spark conf to use with the data quality job (e.g. {\"spark.sql.ansi.enabled\": \"true\"})",
                default="{}",
                valid_regex=r"^.*$",
            )
        )

        quality_checker_override_clusters = json.loads(
            self._prompts.question(
                "Optional Cluster ID to use for the data quality job (e.g. {\"default\": \"<existing-cluster-id>\"}). "
                "If not provided, a job cluster will be created automatically when the job runs",
                default="{}",
                valid_regex=r"^.*$",
            )
        )
        e2e_spark_conf = json.loads(
            self._prompts.question(
                "Optional spark conf to use with the end-to-end workflow (e.g. {\"spark.sql.ansi.enabled\": \"true\"})",
                default="{}",
                valid_regex=r"^.*$",
            )
        )

        e2e_override_clusters = json.loads(
            self._prompts.question(
                "Optional Cluster ID to use for the end-to-end workflow (e.g. {\"default\": \"<existing-cluster-id>\"}). "
                "If not provided, a job cluster will be created automatically when the job runs",
                default="{}",
                valid_regex=r"^.*$",
            )
        )
        return (
            quality_checker_override_clusters,
            quality_checker_spark_conf,
            profiler_override_clusters,
            profiler_spark_conf,
            e2e_override_clusters,
            e2e_spark_conf,
        )

    def _prompt_profiler_config(self) -> ProfilerConfig:
        profile_summary_stats_file = self._prompts.question(
            "Provide filename for storing profile summary statistics",
            default="profile_summary_stats.yml",
            valid_regex=r"^\w.+$",
        )

        return ProfilerConfig(
            summary_stats_file=profile_summary_stats_file,
        )

    def _prompt_input_config(self, is_streaming: bool) -> InputConfig | None:
        input_location = self._prompts.question(
            "Provide location for the input data "
            "as a path or table in the fully qualified format `catalog.schema.table` or `schema.table`",
            default="skipped",
            valid_regex=(
                # Cloud URI (e.g., s3://bucket/key, gs://path/to/file)
                r"^(?:[A-Za-z0-9]+://[A-Za-z0-9_\-./]+"
                # Absolute path (e.g., /path/to/file.csv)
                r"|/[A-Za-z0-9_\-./]+"
                # One or two dot-separated identifiers (schema.table OR catalog.schema.table)
                r"|[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){1,2})$"
            ),
        )

        if input_location != "skipped":
            input_format = self._prompts.question(
                "Provide format for the input data (e.g. delta, parquet, csv, json)",
                default="delta",
                valid_regex=r"^\w.+$",
            )

            input_schema = self._prompts.question(
                "Provide schema for the input data (e.g. col1 int, col2 string)",
                default="skipped",
                valid_regex=r"^\w.+$",
            )

            input_read_options = json.loads(
                self._prompts.question(
                    "Provide additional options for reading the input data (e.g. {\"versionAsOf\": \"0\"})",
                    default="{}",
                    valid_regex=r"^.*$",
                )
            )

            return InputConfig(
                location=input_location,
                format=input_format,
                schema=None if input_schema == "skipped" else input_schema,
                options=input_read_options,
                is_streaming=is_streaming,
            )
        return None

    def _prompt_output_config(self, is_streaming: bool) -> OutputConfig:
        output_table = self._prompts.question(
            "Provide output table in the fully qualified format `catalog.schema.table` or `schema.table`",
            valid_regex=r"^([\w]+(?:\.[\w]+){1,2})$",
        )

        output_write_mode = self._prompts.question(
            "Provide write mode for output table (e.g. 'append' or 'overwrite')",
            default="append",
            valid_regex=r"^(append|overwrite)$",
        )

        output_format = self._prompts.question(
            "Provide format for the output data (e.g. delta, parquet)",
            default="delta",
            valid_regex=r"^\w.+$",
        )

        output_write_options = json.loads(
            self._prompts.question(
                "Provide additional options for writing the output data (e.g. {\"mergeSchema\": \"true\"})",
                default="{}",
                valid_regex=r"^.*$",
            )
        )

        output_trigger_options = {}
        if is_streaming:
            output_trigger_options = json.loads(
                self._prompts.question(
                    "Provide additional options for writing the output data using streaming "
                    "(e.g. {\"availableNow\": true})",
                    default="{}",
                    valid_regex=r"^.*$",
                )
            )

        return OutputConfig(
            location=output_table,
            mode=output_write_mode,
            format=output_format,
            options=output_write_options,
            trigger=output_trigger_options,
        )

    def _prompt_quarantine_config(self, is_streaming: bool) -> OutputConfig | None:
        quarantine_table = self._prompts.question(
            "Provide quarantined table in the fully qualified format `catalog.schema.table` or `schema.table` "
            "(use output table if skipped)",
            default="skipped",
            valid_regex=r"^([\w]+(?:\.[\w]+){1,2})$",
        )

        if quarantine_table != "skipped":
            quarantine_write_mode = self._prompts.question(
                "Provide write mode for quarantine table (e.g. 'append' or 'overwrite')",
                default="append",
                valid_regex=r"^(append|overwrite)$",
            )

            quarantine_format = self._prompts.question(
                "Provide format for the quarantine data (e.g. delta, parquet)",
                default="delta",
                valid_regex=r"^\w.+$",
            )

            quarantine_write_options = json.loads(
                self._prompts.question(
                    "Provide additional options for writing the quarantine data (e.g. {\"mergeSchema\": \"true\"})",
                    default="{}",
                    valid_regex=r"^.*$",
                )
            )

            quarantine_trigger_options = {}
            if is_streaming:
                quarantine_trigger_options = json.loads(
                    self._prompts.question(
                        "Provide additional options for writing the quarantine data using streaming "
                        "(e.g. {\"availableNow\": true})",
                        default="{}",
                        valid_regex=r"^.*$",
                    )
                )

            return OutputConfig(
                location=quarantine_table,
                mode=quarantine_write_mode,
                format=quarantine_format,
                options=quarantine_write_options,
                trigger=quarantine_trigger_options,
            )
        return None

    def _prompt_metrics(self) -> tuple[OutputConfig | None, list[str] | None]:
        store_summary_metrics = self._prompts.confirm(
            "Do you want to store summary metrics from data quality checking in a table?"
        )
        metrics_config = None
        custom_metrics = None

        if store_summary_metrics:
            metrics_config = self._prompt_metrics_config()
            custom_metrics = self._prompt_custom_metrics()

        return metrics_config, custom_metrics

    def _prompt_metrics_config(self) -> OutputConfig:
        """Prompt user for metrics configuration."""
        metrics_table = self._prompts.question(
            "Provide table for storing summary metrics in the fully qualified format `catalog.schema.table` or `schema.table`",
            valid_regex=r"^([\w]+(?:\.[\w]+){1,2})$",
        )

        metrics_write_mode = self._prompts.question(
            "Provide write mode for metrics table (e.g. 'append' or 'overwrite')",
            default="append",
            valid_regex=r"^(append|overwrite)$",
        )

        metrics_format = self._prompts.question(
            "Provide format for the metrics data (e.g. delta, parquet)",
            default="delta",
            valid_regex=r"^\w.+$",
        )

        metrics_write_options = json.loads(
            self._prompts.question(
                "Provide additional options for writing the metrics data (e.g. {\"mergeSchema\": \"true\"})",
                default="{}",
                valid_regex=r"^.*$",
            )
        )

        return OutputConfig(
            location=metrics_table,
            mode=metrics_write_mode,
            format=metrics_format,
            options=metrics_write_options,
        )

    def _prompt_custom_metrics(self) -> list[str]:
        """Prompt user for custom metrics as Spark SQL expressions."""
        custom_metrics_input = self._prompts.question(
            "Provide custom metrics as a list of Spark SQL expressions "
            "(e.g. [\"count(case when age > 65 then 1 end) as senior_count\", \"avg(salary) as avg_salary\"]). "
            "Leave blank to track the default data quality metrics.",
            default="[]",
            valid_regex=r"^.*$",
        )

        if custom_metrics_input.strip():
            custom_metrics = json.loads(custom_metrics_input)
            if not isinstance(custom_metrics, list):
                raise InvalidParameterError(
                    "Custom metrics must be provided as a list of Spark SQL expressions (e.g. ['count(case when age > 65 then 1 end) as senior_count']"
                )
            return custom_metrics
        return []

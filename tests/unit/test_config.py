import pytest
from databricks.labs.dqx.config import (
    WorkspaceConfig,
    RunConfig,
    InstallationChecksStorageConfig,
    FileChecksStorageConfig,
    LakebaseChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
    VolumeFileChecksStorageConfig,
    InputConfig,
    OutputConfig,
    ProfilerConfig,
    LLMModelConfig,
    LLMConfig,
    ExtraParams,
)
from databricks.labs.dqx.errors import InvalidConfigError, InvalidParameterError

DEFAULT_RUN_CONFIG_NAME = "default"
DEFAULT_RUN_CONFIG = RunConfig(
    name=DEFAULT_RUN_CONFIG_NAME,
)

CONFIG = WorkspaceConfig(
    run_configs=[
        DEFAULT_RUN_CONFIG,
        RunConfig(
            name="another_run_config",
        ),
    ]
)


@pytest.mark.parametrize(
    "run_config_name, expected_result",
    [
        ("default", CONFIG.run_configs[0]),
        (None, CONFIG.run_configs[0]),
        ("another_run_config", CONFIG.run_configs[1]),
    ],
)
def test_get_run_config_valid_names(run_config_name, expected_result):
    assert CONFIG.get_run_config(run_config_name) == expected_result


def test_get_run_config_when_name_not_found():
    with pytest.raises(InvalidConfigError, match="No run configurations available"):
        CONFIG.get_run_config("not_found")


def test_get_run_config_when_no_run_configs():
    config = WorkspaceConfig(run_configs=[])
    with pytest.raises(InvalidConfigError, match="No run configurations available"):
        config.get_run_config(None)


@pytest.mark.parametrize(
    "config_class, location, expected_exception, expected_message",
    [
        (
            FileChecksStorageConfig,
            None,
            InvalidConfigError,
            "The file path \\('location' field\\) must not be empty or None",
        ),
        (
            FileChecksStorageConfig,
            "",
            InvalidConfigError,
            "The file path \\('location' field\\) must not be empty or None",
        ),
        (
            WorkspaceFileChecksStorageConfig,
            None,
            InvalidConfigError,
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (
            WorkspaceFileChecksStorageConfig,
            "",
            InvalidConfigError,
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (
            TableChecksStorageConfig,
            None,
            InvalidConfigError,
            "The table name \\('location' field\\) must not be empty or None",
        ),
        (
            TableChecksStorageConfig,
            "",
            InvalidConfigError,
            "The table name \\('location' field\\) must not be empty or None",
        ),
        (
            InstallationChecksStorageConfig,
            None,
            InvalidConfigError,
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (
            InstallationChecksStorageConfig,
            "",
            InvalidConfigError,
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (LakebaseChecksStorageConfig, "", InvalidParameterError, "Location must not be empty or None."),
        (
            VolumeFileChecksStorageConfig,
            "",
            InvalidParameterError,
            "The Unity Catalog volume file path \\('location' field\\) must not be empty or None.",
        ),
        (
            VolumeFileChecksStorageConfig,
            "invalid_volume_path/files",
            InvalidParameterError,
            "The volume path must start with '/Volumes/'.",
        ),
        (
            VolumeFileChecksStorageConfig,
            "/Volumes",
            InvalidParameterError,
            "The volume path must start with '/Volumes/'.",
        ),
        (
            VolumeFileChecksStorageConfig,
            "/Volumes/main",
            InvalidParameterError,
            "Invalid path: Path is missing a schema name",
        ),
        (
            VolumeFileChecksStorageConfig,
            "/Volumes/main/demo",
            InvalidParameterError,
            "Invalid path: Path is missing a volume name",
        ),
        (
            VolumeFileChecksStorageConfig,
            "/Volumes/main/demo/files/my_file.txt",
            InvalidParameterError,
            "Invalid path: Path must include a file name after the volume",
        ),
    ],
)
def test_post_init_validation(config_class, location, expected_exception, expected_message):
    with pytest.raises(expected_exception, match=expected_message):
        config_class(location=location)


# Test InputConfig
def test_input_config_defaults():
    config = InputConfig(location="table_name")
    assert config.location == "table_name"
    assert config.format == "delta"
    assert config.is_streaming is False
    assert config.schema is None
    assert not config.options


def test_input_config_with_options():
    config = InputConfig(
        location="s3://bucket/path",
        format="parquet",
        is_streaming=True,
        schema="my_schema",
        options={"header": "true", "delimiter": ","},
    )
    assert config.location == "s3://bucket/path"
    assert config.format == "parquet"
    assert config.is_streaming is True
    assert config.schema == "my_schema"
    assert config.options == {"header": "true", "delimiter": ","}


# Test OutputConfig
def test_output_config_defaults():
    config = OutputConfig(location="output_table")
    assert config.location == "output_table"
    assert config.format == "delta"
    assert config.mode == "append"
    assert not config.options
    assert not config.trigger


def test_output_config_trigger_string_to_bool_conversion():
    config = OutputConfig(
        location="output_table",
        trigger={"once": "true", "availableNow": "True", "continuous": "false", "processingTime": "10 seconds"},
    )
    assert config.trigger["once"] is True
    assert config.trigger["availableNow"] is True
    assert config.trigger["continuous"] is False
    assert config.trigger["processingTime"] == "10 seconds"


def test_output_config_trigger_with_actual_booleans():
    config = OutputConfig(location="output_table", trigger={"once": True, "continuous": False})
    assert config.trigger["once"] is True
    assert config.trigger["continuous"] is False


# Test ProfilerConfig
def test_profiler_config_defaults():
    config = ProfilerConfig()
    assert config.summary_stats_file == "profile_summary_stats.yml"
    assert config.sample_fraction == 0.3
    assert config.sample_seed is None
    assert config.limit == 1000
    assert config.filter is None


def test_profiler_config_custom_values():
    config = ProfilerConfig(
        summary_stats_file="custom_stats.yml",
        sample_fraction=0.5,
        sample_seed=42,
        limit=5000,
        filter="col1 > 0",
    )
    assert config.summary_stats_file == "custom_stats.yml"
    assert config.sample_fraction == 0.5
    assert config.sample_seed == 42
    assert config.limit == 5000
    assert config.filter == "col1 > 0"


# Test LLMModelConfig
def test_llm_model_config_defaults():
    config = LLMModelConfig()
    assert config.model_name == "databricks/databricks-claude-sonnet-4-5"
    assert config.api_key == ""
    assert config.api_base == ""


def test_llm_model_config_custom_values():
    config = LLMModelConfig(
        model_name="custom-model", api_key="secret_scope/secret_key", api_base="https://api.example.com"
    )
    assert config.model_name == "custom-model"
    assert config.api_key == "secret_scope/secret_key"
    assert config.api_base == "https://api.example.com"


# Test LLMConfig
def test_llm_config_defaults():
    config = LLMConfig()
    assert isinstance(config.model, LLMModelConfig)
    assert config.model.model_name == "databricks/databricks-claude-sonnet-4-5"


def test_llm_config_with_custom_model():
    model = LLMModelConfig(model_name="custom-model")
    config = LLMConfig(model=model)
    assert config.model.model_name == "custom-model"


# Test ExtraParams
def test_extra_params_defaults():
    config = ExtraParams()
    assert not config.result_column_names
    assert not config.user_metadata
    assert config.run_time_overwrite is None
    assert config.run_id_overwrite is None


def test_extra_params_custom_values():
    config = ExtraParams(
        result_column_names={"col1": "renamed_col1"},
        user_metadata={"key": "value"},
        run_time_overwrite="2025-01-01",
        run_id_overwrite="custom_run_id",
    )
    assert config.result_column_names == {"col1": "renamed_col1"}
    assert config.user_metadata == {"key": "value"}
    assert config.run_time_overwrite == "2025-01-01"
    assert config.run_id_overwrite == "custom_run_id"


# Test WorkspaceConfig.as_dict()
def test_workspace_config_as_dict():
    config = WorkspaceConfig(
        run_configs=[DEFAULT_RUN_CONFIG],
        log_level="DEBUG",
        serverless_clusters=False,
    )
    result = config.as_dict()
    assert isinstance(result, dict)
    assert result["log_level"] == "DEBUG"
    assert result["serverless_clusters"] is False
    assert len(result["run_configs"]) == 1
    # Verify the method properly converts the config to a dictionary
    assert "profiler_max_parallelism" in result
    assert "quality_checker_max_parallelism" in result


# Test LakebaseChecksStorageConfig validation and properties
def test_lakebase_config_missing_instance_name():
    with pytest.raises(InvalidParameterError, match="Instance name must not be empty or None"):
        LakebaseChecksStorageConfig(location="db.schema.table", instance_name=None, user="test_user")


def test_lakebase_config_missing_user():
    with pytest.raises(InvalidParameterError, match="User must not be empty or None"):
        LakebaseChecksStorageConfig(location="db.schema.table", instance_name="instance", user=None)


def test_lakebase_config_invalid_location_format():
    with pytest.raises(InvalidConfigError, match="Invalid Lakebase table name.*Must be in the format"):
        LakebaseChecksStorageConfig(location="invalid_table", instance_name="instance", user="test_user")


def test_lakebase_config_invalid_mode():
    with pytest.raises(InvalidConfigError, match="Invalid mode.*Must be 'append' or 'overwrite'"):
        LakebaseChecksStorageConfig(
            location="db.schema.table", instance_name="instance", user="test_user", mode="invalid"
        )


def test_lakebase_config_properties():
    config = LakebaseChecksStorageConfig(
        location="my_db.my_schema.my_table", instance_name="instance", user="test_user"
    )
    assert config.database_name == "my_db"
    assert config.schema_name == "my_schema"
    assert config.table_name == "my_table"


def test_lakebase_config_properties_cached():
    config = LakebaseChecksStorageConfig(location="db1.sch1.tbl1", instance_name="instance", user="test_user")
    # Access properties multiple times to ensure caching works
    assert config.database_name == "db1"
    assert config.database_name == "db1"
    assert config.schema_name == "sch1"
    assert config.schema_name == "sch1"
    assert config.table_name == "tbl1"
    assert config.table_name == "tbl1"


def test_lakebase_config_valid_modes():
    config_append = LakebaseChecksStorageConfig(
        location="db.schema.table", instance_name="instance", user="test_user", mode="append"
    )
    assert config_append.mode == "append"

    config_overwrite = LakebaseChecksStorageConfig(
        location="db.schema.table", instance_name="instance", user="test_user", mode="overwrite"
    )
    assert config_overwrite.mode == "overwrite"


# Test VolumeFileChecksStorageConfig valid paths
def test_volume_file_config_valid_yml_path():
    config = VolumeFileChecksStorageConfig(location="/Volumes/main/demo/files/checks.yml")
    assert config.location == "/Volumes/main/demo/files/checks.yml"


def test_volume_file_config_valid_yaml_path():
    config = VolumeFileChecksStorageConfig(location="/Volumes/catalog/schema/volume/subfolder/checks.yaml")
    assert config.location == "/Volumes/catalog/schema/volume/subfolder/checks.yaml"


def test_volume_file_config_valid_json_path():
    config = VolumeFileChecksStorageConfig(location="/Volumes/cat/sch/vol/checks.json")
    assert config.location == "/Volumes/cat/sch/vol/checks.json"


# Test TableChecksStorageConfig with defaults
def test_table_config_defaults():
    config = TableChecksStorageConfig(location="catalog.schema.table")
    assert config.location == "catalog.schema.table"
    assert config.run_config_name == "default"
    assert config.mode == "overwrite"


def test_table_config_custom_values():
    config = TableChecksStorageConfig(location="my_table", run_config_name="custom_run", mode="append")
    assert config.location == "my_table"
    assert config.run_config_name == "custom_run"
    assert config.mode == "append"


# Test FileChecksStorageConfig valid path
def test_file_config_valid_path():
    config = FileChecksStorageConfig(location="/path/to/checks.yml")
    assert config.location == "/path/to/checks.yml"


# Test WorkspaceFileChecksStorageConfig valid path
def test_workspace_file_config_valid_path():
    config = WorkspaceFileChecksStorageConfig(location="/Workspace/Users/user@example.com/checks.yml")
    assert config.location == "/Workspace/Users/user@example.com/checks.yml"


# Test InstallationChecksStorageConfig defaults
def test_installation_config_defaults():
    config = InstallationChecksStorageConfig(location="installation")
    assert config.location == "installation"
    assert config.run_config_name == "default"
    assert config.product_name == "dqx"
    assert config.assume_user is True
    assert config.install_folder is None
    assert config.overwrite_location is False


def test_installation_config_custom_values():
    config = InstallationChecksStorageConfig(
        location="custom_location",
        run_config_name="custom_run",
        product_name="custom_product",
        assume_user=False,
        install_folder="/custom/folder",
        overwrite_location=True,
    )
    assert config.location == "custom_location"
    assert config.run_config_name == "custom_run"
    assert config.product_name == "custom_product"
    assert config.assume_user is False
    assert config.install_folder == "/custom/folder"
    assert config.overwrite_location is True


# Test RunConfig
def test_run_config_defaults():
    config = RunConfig()
    assert config.name == "default"
    assert config.input_config is None
    assert config.output_config is None
    assert config.quarantine_config is None
    assert config.metrics_config is None
    assert isinstance(config.profiler_config, ProfilerConfig)
    assert config.checks_user_requirements is None
    assert config.checks_location == "checks.yml"
    assert config.warehouse_id is None
    assert not config.reference_tables
    assert not config.custom_check_functions
    assert config.lakebase_instance_name is None
    assert config.lakebase_user is None
    assert config.lakebase_port is None


def test_run_config_with_input_output():
    input_cfg = InputConfig(location="input_table")
    output_cfg = OutputConfig(location="output_table")
    config = RunConfig(name="test_run", input_config=input_cfg, output_config=output_cfg)
    assert config.name == "test_run"
    assert config.input_config == input_cfg
    assert config.output_config == output_cfg

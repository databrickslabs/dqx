import yaml

from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


def model(dbt, session):
    input_df = dbt.ref("dummy_model")  # reference to the model/table that checks will be applied to

    dq_engine = DQEngine(WorkspaceClient())

    checks = yaml.safe_load("""
    # completeness check
    - criticality: error
      check:
        function: is_not_null
        arguments:
          column: id
    # uniqueness check
    - criticality: warn
      check:
        function: is_unique
        arguments:
          columns:
          - id
    """)

    # Checks can also be loaded from a file in the workspace or delta table
    # checks_path = dbt.config.get("checks_file_path")  # get from dbt var
    # checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=checks_path))

    # apply quality checks with issues reported in _warnings and _errors columns
    df = dq_engine.apply_checks_by_metadata(input_df, checks)

    # dbt python models must return a single DataFrame
    return df

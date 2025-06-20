import pytest

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQDatasetRule


def test_foreign_key_check_missing_provided_both_ref_df_and_table():
    with pytest.raises(ValueError, match="Both 'ref_df_name' and 'ref_table' are provided"):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            column="a",
            check_func_kwargs={
                "ref_column": "a",
                "ref_df_name": "ref_df",
                "ref_table": "table",
            },
        )


def test_foreign_key_check_missing_missing_ref_df_and_table():
    with pytest.raises(ValueError, match="Either 'ref_df_name' or 'ref_table' must be provided"):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            column="a",
            check_func_kwargs={
                "ref_column": "a",
            },
        )

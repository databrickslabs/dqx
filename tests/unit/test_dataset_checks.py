import pytest

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQDatasetRule


def test_foreign_key_check_missing_provided_both_ref_df_and_table():
    with pytest.raises(ValueError, match="Both 'ref_df_name' and 'ref_table' are provided"):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_df_name": "ref_df",
                "ref_table": "table",
            },
        )


def test_foreign_key_check_missing_ref_df_and_table():
    with pytest.raises(ValueError, match="Either 'ref_df_name' or 'ref_table' must be provided"):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
            },
        )


def test_foreign_key_check_null_ref_df_name():
    with pytest.raises(ValueError, match="Either 'ref_df_name' or 'ref_table' must be provided"):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_df_name": "",
            },
        )


def test_foreign_key_check_null_ref_table():
    with pytest.raises(ValueError, match="Either 'ref_df_name' or 'ref_table' must be provided"):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_table": "",
            },
        )


def test_foreign_key_check_not_equal_number_of_columns():
    with pytest.raises(ValueError, match="The number of columns to check against the reference columns must be equal"):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a", "b"],
                "ref_table": "table",
            },
        )

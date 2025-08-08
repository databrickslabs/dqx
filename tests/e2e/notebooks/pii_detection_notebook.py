# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")
%pip install '{dbutils.widgets.get("test_library_ref")}' pytest

# COMMAND ----------

import pytest
from importlib import import_module

def test_import_pii_module_fails_without_installation():
  with pytest.raises(ImportError):
      import_module('databricks.labs.dqx.pii')

test_import_pii_module_fails_without_installation()

# COMMAND ----------

%pip install 'databricks-labs-dqx[pii] @ {dbutils.widgets.get("test_library_ref")}' chispa==0.10.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.functions as F
from databricks.labs.dqx.pii.nlp_engine_config import NLPEngineConfig
from databricks.labs.dqx.pii.pii_detection_funcs import contains_pii
from chispa import assert_df_equality

# COMMAND ----------
# DBTITLE 1,test_contains_pii_basic

def test_contains_pii_basic():
    schema_pii = "col1: string, col2: string, col3: string"
    test_df = spark.createDataFrame(
        [
            ["Hello world", "John Doe", "john.doe@example.com"],
            ["No sensitive data here", "Not a person", "not-an-email"],
            ["", "", ""],
            [None, None, None],
        ],
        schema_pii,
    )

    actual = test_df.select(
        contains_pii("col1"),
        contains_pii("col2"),
        contains_pii("col3"),
    )

    checked_schema = "col1_contains_pii: string, col2_contains_pii: string, col3_contains_pii: string"
    expected = spark.createDataFrame(
        [
            [
                None,
                """Column 'col2' contains PII: [{"entity_type": "PERSON", "score": 1.0, "text": "John Doe"}]""",
                """Column 'col3' contains PII: [{"entity_type": "EMAIL_ADDRESS", "score": 1.0, "text": "john.doe@example.com"}]""",
            ],
            [
                None,
                None,
                None,
            ],
            [
                None,
                None,
                None,
            ],
            [
                None,
                None,
                None,
            ],
        ],
        checked_schema,
    )
    transforms = [
        lambda df: df.select(
            F.ilike("col1_contains_pii", F.lit("Column 'col1' contains PII: %")).alias("col1_contains_pii"),
            F.ilike("col2_contains_pii", F.lit("Column 'col2' contains PII: %")).alias("col2_contains_pii"),
            F.ilike("col3_contains_pii", F.lit("Column 'col3' contains PII: %")).alias("col3_contains_pii"),
        )
    ]
    assert_df_equality(actual, expected, transforms=transforms)

test_contains_pii_basic()

# COMMAND ----------
# DBTITLE 1,test_contains_pii_with_entities_list

def test_contains_pii_with_entities_list():
    schema_pii = "col1: string, col2: string"
    test_df = spark.createDataFrame(
        [
            ["John Doe", "John Doe lives at 123 Main St and can be reached at john@example.com"],
            ["test@email.com", "Just an email here"],
            ["No PII content", "Nothing sensitive"],
            [None, None],
        ],
        schema_pii,
    )

    actual = test_df.select(
        contains_pii("col1", entities=["PERSON", "EMAIL_ADDRESS"]),
        contains_pii("col2", entities=["PERSON"]),
    )

    checked_schema = "col1_contains_pii: string, col2_contains_pii: string"
    expected = spark.createDataFrame(
        [
            [
                """Column 'col1' contains PII: [{"entity_type": "PERSON", "score": 1.0, "text": "John Doe"}]""",
                """Column 'col2' contains PII: [{"entity_type": "PERSON", "score": 1.0, "text": "John Doe"},{"entity_type": "EMAIL_ADDRESS", "score": 1.0, "text": "john@example.com"}]""",
            ],
            [
                """Column 'col1' contains PII: [{"entity_type": "EMAIL_ADDRESS", "score": 1.0, "text": "test@email.com"}]""",
                None,
            ],
            [
                None,
                None,
            ],
            [
                None,
                None,
            ],
        ],
        checked_schema,
    )
    transforms = [
        lambda df: df.select(
            F.ilike("col1_contains_pii", F.lit("Column 'col1' contains PII: %")).alias("col1_contains_pii"),
            F.ilike("col2_contains_pii", F.lit("Column 'col2' contains PII: %")).alias("col2_contains_pii"),
        )
    ]
    assert_df_equality(actual, expected, transforms=transforms)

test_contains_pii_with_entities_list()

# COMMAND ----------
# DBTITLE 1,test_contains_pii_with_builtin_nlp_engine_config

def test_contains_pii_with_builtin_nlp_engine_config():
    schema_pii = "col1: string"
    test_df = spark.createDataFrame(
        [
            ["Dr. Jane Smith works at Memorial Hospital"],
            ["Patient ID: 12345, DOB: 1990-01-01"],
            ["Regular text without PII"],
            [None],
        ],
        schema_pii,
    )

    actual = test_df.select(contains_pii("col1", entities=["PERSON", "DATE_TIME"], nlp_engine_config=NLPEngineConfig.SPACY_MEDIUM))

    checked_schema = "col1_contains_pii: string"
    expected = spark.createDataFrame(
        [
            ["""Column 'col1' contains PII: [{"entity_type": "PERSON", "score": 1.0, "text": "Jane Smith"}]"""],
            ["""Column 'col1' contains PII: [{"entity_type": "DATE_TIME", "score": 1.0, "text": "1990-01-01"}]"""],
            [None],
            [None],
        ],
        checked_schema,
    )
    transforms = [
        lambda df: df.select(
            F.ilike("col1_contains_pii", F.lit("Column 'col1' contains PII: %")).alias("col1_contains_pii"),
        )
    ]
    assert_df_equality(actual, expected, transforms=transforms)

test_contains_pii_with_builtin_nlp_engine_config()

# COMMAND ----------
# DBTITLE 1,test_contains_pii_with_custom_nlp_config_dict

def test_contains_pii_with_custom_nlp_config_dict():
    schema_pii = "col1: string"
    test_df = spark.createDataFrame(
        [
            ["Dr. Jane Smith treated patient John Doe at City Hospital"],
            ["Lorem ipsum dolor sit amet"],
            [None],
        ],
        schema_pii,
    )
    custom_nlp_engine_config = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
    }
    actual = test_df.select(contains_pii("col1", entities=["PERSON"], nlp_engine_config=custom_nlp_engine_config))

    checked_schema = "col1_contains_pii: string"
    expected = spark.createDataFrame(
        [
            [
                """Column 'col1' contains PII: [{"entity_type": "PERSON", "score": 1.0, "text": "Jane Smith"},{"entity_type": "PERSON", "score": 1.0, "text": "John Doe"}]"""
            ],
            [None],
            [None],
        ],
        checked_schema,
    )
    transforms = [
        lambda df: df.select(
            F.ilike("col1_contains_pii", F.lit("Column 'col1' contains PII: %")).alias("col1_contains_pii"),
        )
    ]
    assert_df_equality(actual, expected, transforms=transforms)

test_contains_pii_with_custom_nlp_config_dict()

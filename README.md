Databricks Labs DQX
===

Simplified Data Quality checking at Scale for PySpark Workloads on streaming and standard DataFrames.

[![build](https://github.com/databrickslabs/dqx/actions/workflows/push.yml/badge.svg)](https://github.com/databrickslabs/dqx/actions/workflows/push.yml) [![codecov](https://codecov.io/github/databrickslabs/dqx/graph/badge.svg?token=x1JSVddfZa)](https://codecov.io/github/databrickslabs/dqx) ![linesofcode](https://aschey.tech/tokei/github/databrickslabs/dqx?category=code)

<!-- TOC -->
* [Databricks Labs DQX](#databricks-labs-dqx)
* [Motivation](#motivation)
* [Key Capabilities](#key-capabilities)
* [Prerequisites](#prerequisites)
* [Installation](#installation)
  * [Installation as Library](#installation-as-library)
  * [Installation in a Databricks Workspace](#installation-in-a-databricks-workspace)
    * [Authentication](#authentication)
    * [Install DQX in the Databricks workspace](#install-dqx-in-the-databricks-workspace)
    * [Install the tool on the Databricks cluster](#install-the-tool-on-the-databricks-cluster)
    * [Upgrade DQX in the Databricks workspace](#upgrade-dqx-in-the-databricks-workspace)
    * [Uninstall DQX from the Databricks workspace](#uninstall-dqx-from-the-databricks-workspace)
* [How to use it](#how-to-use-it)
  * [Data Profiling](#data-profiling)
    * [In Python](#in-python)
    * [Using CLI](#using-cli)
  * [Validating quality rules (checks)](#validating-quality-rules--checks-)
    * [In Python](#in-python-1)
    * [Using CLI](#using-cli-1)
  * [Adding quality checks to the application](#adding-quality-checks-to-the-application)
    * [Quality rules defined as config](#quality-rules-defined-as-config)
    * [Quality rules defined as code](#quality-rules-defined-as-code)
    * [Integration with DLT (Delta Live Tables)](#integration-with-dlt--delta-live-tables-)
  * [Re-ingesting curated data](#re-ingesting-curated-data)
* [Quality rules / functions](#quality-rules--functions)
  * [Creating your own checks](#creating-your-own-checks)
* [Contribution](#contribution)
* [Project Support](#project-support)
<!-- TOC -->

# Motivation

Current data quality frameworks often fall short in providing detailed explanations for specific row or column 
data quality issues and are primarily designed for complete datasets, 
making integration into streaming workloads difficult.

This project introduces a simple Python validation framework for assessing data quality of PySpark DataFrames. 
It enables real-time quality validation during data processing rather than relying solely on post-factum monitoring.
The validation output includes detailed information on why specific rows and columns have issues, 
allowing for quicker identification and resolution of data quality problems.

![problem](docs/dqx.png?)

Invalid data can be quarantined to make sure bad data is never written to the output.

![problem](docs/dqx_quarantine.png?)

In the Lakehouse architecture, the validation of new data should happen at the time of data entry into the Curated Layer 
to make sure bad data is not propagated to the subsequent layers. With DQX you can easily quarantine invalid data and re-ingest it 
after curation to ensure that data quality constraints are met.

![problem](docs/dqx_lakehouse.png?)

For monitoring the data quality of already persisted data in a Delta table (post-factum monitoring), we recommend to use 
[Databricks Lakehouse Monitoring](https://docs.databricks.com/en/lakehouse-monitoring/index.html).

# Key Capabilities

- Info of why a check has failed.
- Data format agnostic (works on Spark DataFrames).
- Support for Spark Batch and Streaming including DLT (Delta Live Tables).
- Different reactions on failed checks, e.g. drop, mark, or quarantine invalid data.
- Support for check levels: warning (mark) or errors (mark and don't propagate the rows).
- Support for quality rules at row and column level.
- Profiling and generation of data quality rules candidates.
- Re-ingestion of curated data.
- Checks definition as code or config.
- Validation summary and data quality dashboard for identifying and tracking data quality issues.

# Prerequisites

- Python 3.10 or later. See [instructions](https://www.python.org/downloads/).
- Unity Catalog-enabled [Databricks workspace](https://docs.databricks.com/en/getting-started/index.html).
- Network access to your Databricks Workspace used for the [installation process](#installation).
- (Optional) Databricks CLI v0.213 or later. See [instructions](https://docs.databricks.com/dev-tools/cli/databricks-cli.html).

[[back to top](#databricks-labs-dqx)]

# Installation

The project can be installed on a Databricks workspace or used as a standalone library.

## Installation as Library

Install the project via `pip`:

```commandline
pip install databricks-labs-dqx
```

## Installation in a Databricks Workspace

### Authentication

Once you install Databricks CLI, authenticate your current machine to your Databricks Workspace:

```commandline
databricks auth login --host <WORKSPACE_HOST>
```

To enable debug logs, simply add `--debug` flag to any command.
More about authentication options [here](https://docs.databricks.com/en/dev-tools/cli/authentication.html).

### Install DQX in the Databricks workspace

Install DQX in your Databricks workspace via Databricks CLI:

```commandline
databricks labs install dqx
```

You'll be prompted to select a [configuration profile](https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication) created by `databricks auth login` command,
and other configuration options.

The cli command will install the following components in the workspace:
- A Python [wheel file](https://peps.python.org/pep-0427/) with the library packaged.
- DQX configuration file ('config.yml').
- Quality dashboard for monitoring to display information about the data quality issues.

DQX configuration file can contain multiple run configurations defining specific set of input, output and quarantine locations etc.
You can add additional run configs after the installation by editing the 'config.yml' file:
```yaml
log_level: INFO
run_config:
- name: default
  checks_file: checks.yml
  curated_location: main.dqx.curated
  input_locations: main.dqx.input
  output_location: main.dqx.output
  profile_summary_stats_file: checks.yml
  quarantine_location: main.dqx.quarantine
- name: another_run_config
  ...
```

To select a specific run config when executing the dqx labs cli commands use `--run-config` parameter. 
When not provided the "default" run config is used.

By default, DQX is installed in the user home directory (under '/Users/<user>/.dqx'). You can also install DQX globally
by setting 'DQX_FORCE_INSTALL' environment variable. The following options are available:
* `DQX_FORCE_INSTALL=global databricks labs install dqx`: will force the installation to be for root only ('/Applications/dqx')
* `DQX_FORCE_INSTALL=user databricks labs install dqx`: will force the installation to be for user only ('/Users/<user>/.dqx')

### Install the tool on the Databricks cluster

After you install the tool on the workspace, you need to install the DQX package on a Databricks cluster.
You can install the DQX library either from PYPI or use a wheel file generated as part of the installation in the workspace.

There are multiple ways to install libraries in a Databricks cluster (see [here](https://docs.databricks.com/en/libraries/index.html)).
For example, you can install DQX directly from a notebook cell as follows:
```python
# using PYPI package:
%pip install databricks-labs-dqx

# using wheel file, DQX installed for the current user:
%pip install /Workspace/Users/<user-name>/.dqx/wheels/databricks_labs_dqx-*.whl

# using wheel file, DQX installed globally:
%pip install /Applications/dqx/wheels/databricks_labs_dqx-*.whl
```

Restart the kernel after the package is installed in the notebook:
```python
# in a separate cell run:
dbutils.library.restartPython()
```

### Upgrade DQX in the Databricks workspace

Verify that DQX is installed:

```commandline
databricks labs installed
```

Upgrade DQX via Databricks CLI:

```commandline
databricks labs upgrade dqx
```

### Uninstall DQX from the Databricks workspace

Uninstall DQX via Databricks CLI:

```commandline
databricks labs uninstall dqx
```

Databricks CLI will confirm a few options:
- Whether you want to remove all dqx artefacts from the workspace as well. Defaults to 'no'.

[[back to top](#databricks-labs-dqx)]

# How to use it

## Data Profiling

Data profiling is run to generate quality rule candidates and provide summary statistics of the input data.
The generated rules are input for the quality checking (see [Adding quality checks to the application](#adding-quality-checks-to-the-application)).

### In Python

```python
from databricks.labs.dqx.profiler.profiler import profile

df = spark.read.table("catalog1.schema1.table1")
summary_stats, checks = profile(df)
```

### Using CLI 

You must install DQX in the workspace before (see [installation](#installation-in-a-databricks-workspace)).

Run profiling job:
```commandline
databricks labs dqx profile --run-config "default"
```

If run config is not provided, the "default" run config will be used. The run config is used to select specific run configuration from 'config.yml'.

The following DQX configuration from 'config.yml' will be used by default:
- 'input_location': input data as a path or a table.
- 'checks_file': relative location of the generated quality rule candidates (default: `checks.yml`). Can be json or yaml file.
- 'profile_summary_stats_file': relative location of the summary statistics (default: `profile_summary.yml`). Can be json or yaml file.

You can overwrite the default parameters as follows:
```commandline
databricks labs dqx profile --input-location "catalog1.schema1.table1" --checks-file "checks.yml" --profile-summary-stats-file "profile_summary.yml"
```

## Validating quality rules (checks)

If you manually adjust the generated rules or create your own configuration, you can validate them before using in your application
(see [Adding quality checks to the application](#adding-quality-checks-to-the-application)).

### In Python

```python
from databricks.labs.dqx.engine import validate_rules

valid = validate_rules(checks)  # returns boolean
```

### Using CLI

Validate checks stored in the installation folder:
```commandline
databricks labs dqx validate --run-config "default"
```
The following DQX configuration from 'config.yml' will be used by default:
- 'checks_file': relative location of the quality rule (default: `checks.yml`).

You can overwrite the default parameters as follows:
```commandline
databricks labs dqx validate  --run-config "default" --checks-file "checks.yml"
```

## Adding quality checks to the application

### Quality rules defined as config

Quality rules stored in a yaml file (e.g. 'checks.yml'):
```yaml
- criticality: error
  check:
    function: is_not_null
    arguments:
      col_names:
      - col1
      - col2
- name: col_col3_is_null_or_empty
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments:
      col_name: col3
- criticality: warn
  check:
    function: value_is_in_list
    arguments:
      col_name: col4
      allowed:
      - 1
      - 2
```
Fields:
- `criticality`: either "error" (data going only into "bad/quarantine" dataframe) or "warn" (data going into both dataframes).
- `check`: column expression containing "function" (check function to apply), "arguments" (check function arguments), and "col_name" (column name as str to apply to check for) or "col_names" (column names as array to apply the check for). 
- (optional) `name` for the check: autogenerated if not provided.

Json format is also supported for the checks.

**Method 1 (load checks from a workspace file):**

If the tool is installed in the workspace, the config contains path to the checks file:

```python
from databricks.labs.dqx.engine import apply_checks_by_metadata, apply_checks_by_metadata_and_split
from databricks.labs.dqx.engine import load_checks_from_file
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.installation import Installation

# use check file specified in the default installation config ('config.yml')
# if filename provided it's a relative path to the workspace installation directory
ws = WorkspaceClient()
installation = Installation.current(ws, "dqx", assume_user=True)
checks = load_checks_from_file(installation)

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes 
valid_df, quarantined_df = apply_checks_by_metadata_and_split(input_df, checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = apply_checks_by_metadata(input_df, checks)
```

**Method 2 (load checks from a local file):**

The checks can also be loaded from a file in the local file system:

```python
from databricks.labs.dqx.engine import apply_checks_by_metadata, apply_checks_by_metadata_and_split
from databricks.labs.dqx.engine import load_checks_from_local_file

checks = load_checks_from_local_file("checks.yml")

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes 
valid_df, quarantined_df = apply_checks_by_metadata(input_df, checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = apply_checks_by_metadata_and_split(input_df, checks)
```

### Quality rules defined as code

**Method 1 (using DQX classes):**
```python
from databricks.labs.dqx.col_functions import is_not_null, is_not_null_and_not_empty, value_is_in_list
from databricks.labs.dqx.engine import DQRule, DQRuleColSet, apply_checks, apply_checks_and_split

checks = DQRuleColSet( # define rule for multiple columns at once
            columns=["col1", "col2"], 
            criticality="error", 
            check_func=is_not_null).get_rules() + [
         DQRule( # define rule for a single column
            name='col3_is_null_or_empty',
            criticality='error', 
            check=is_not_null_and_not_empty('col3')),
         DQRule( # name auto-generated if not provided       
            criticality='warn', 
            check=value_is_in_list('col4', ['1', '2']))
        ]

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes 
valid_df, quarantined_df = apply_checks_and_split(input_df, checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = apply_checks(input_df, checks)
```

See details of the check functions [here](#quality-rules--functions).

**Method 2 (using dictionary):**
```python
import yaml
from databricks.labs.dqx.engine import apply_checks_by_metadata, apply_checks_by_metadata_and_split

checks = yaml.safe_load("""
- criticality: "error"
  check:
    function: "is_not_null"
    arguments:
      col_names:
        - "col1"
        - "col2"

- criticality: "error"
  check:
    function: "is_not_null_and_not_empty"
    arguments:
      col_name: "col3"

- criticality: "warn"
  check:
    function: "value_is_in_list"
    arguments:
      col_name: "col4"
      allowed:
        - 1
        - 2
""")

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes 
valid_df, quarantined_df = apply_checks_by_metadata_and_split(input_df, checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = apply_checks_by_metadata(input_df, checks)
```

See details of the check functions [here](#quality-rules--functions).

### Integration with DLT (Delta Live Tables)

DLT provides [expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html) to enforce data quality constraints. However, expectations don't offer detailed insights into why certain checks fail.
The example below demonstrates how to integrate DQX checks with DLT to provide comprehensive information on why quality checks failed.
The integration does not use expectations but the DQX checks directly.

**Option 1: apply quality rules and quarantine bad records**

```python
import dlt
from databricks.labs.dqx.engine import apply_checks, get_invalid, get_valid

checks = ... # quality rules / checks

@dlt.view
def bronze_dq_check():
  df = dlt.read_stream("bronze")
  return apply_checks(df, checks)

@dlt.table
def silver():
  df = dlt.read_stream("bronze_dq_check")
  # get rows without errors or warnings, and drop auxiliary columns
  return get_valid(df)

@dlt.table
def quarantine():
  df = dlt.read_stream("bronze_dq_check")
  # get only rows with errors or warnings
  return get_invalid(df)
```

**Option 2: apply quality rules as additional columns (`_warning` and `_error`)**

```python
import dlt
from databricks.labs.dqx.engine import apply_checks

checks = ... # quality rules / checks

@dlt.view
def bronze_dq_check():
  df = dlt.read_stream("bronze")
  return apply_checks(df, checks)

@dlt.table
def silver():
  df = dlt.read_stream("bronze_dq_check")
  return df
```

## Re-ingesting curated data

Once the application is run, the user should analyse and investigate data quality problems, 
for example by looking at a summary, quality dashboard, and/or quarantined dataset etc.).

Once the root cause analysis is performed, user can curate the quarantined data and store it in the curated table.

To re-ingest the curated data to the output table run:
```commandline
databricks labs dqx re-ingest-curated --run-config "default"
```

The following DQX configuration from 'config.yml' will be used by default:
- 'curated_table' - table for the curated data.
- 'output_table' - table for the output data.
- 'quarantine_table' - table for the quarantine data.

You can overwrite the default parameters as follows:
```commandline
databricks labs dqx re-ingest-curated --output-table "catalog1.schema1.table1" --curated-table "catalog1.schema1.table2"
```

[[back to top](#databricks-labs-dqx)]

# Quality rules / functions

The following quality rules / functions are currently available:

| Check                                                | Description                                                                                                                                                     | Arguments                                                                                                                                                     |
|------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| is_not_null                                          | Check if input column is not null                                                                                                                               | col_name: column name to check                                                                                                                                |
| is_not_empty                                         | Check if input column is not empty                                                                                                                              | col_name: column name to check                                                                                                                                |
| is_not_null_and_not_empty                            | Check if input column is not null or empty                                                                                                                      | col_name: column name to check; trim_strings: boolean flag to trim spaces from strings                                                                        |
| value_is_in_list                                     | Check if the provided value is present in the input column.                                                                                                     | col_name: column name to check; allowed: list of allowed values                                                                                               |
| value_is_not_null_and_is_in_list                     | Check if provided value is present if the input column is not null                                                                                              | col_name: column name to check; allowed: list of allowed values                                                                                               |
| is_in_range                                          | Check if input column is in the provided range (inclusive of both boundaries)                                                                                   | col_name: column name to check; min_limit: min limit; max_limit: max limit                                                                                    |
| is_not_in_range                                      | Check if input column is not within defined range (inclusive of both boundaries)                                                                                | col_name: column name to check; min_limit: min limit value; max_limit: max limit value                                                                        |                                                            
| not_less_than                                        | Check if input column is not less than the provided limit                                                                                                       | col_name: column name to check; limit: limit value                                                                                                            |
| not_greater_than                                     | Check if input column is not greater than the provided limit                                                                                                    | col_name: column name to check; limit: limit value                                                                                                            |
| not_in_future                                        | Check if input column defined as date is not in the future (future defined as current_timestamp + offset)                                                       | col_name: column name to check; offset: offset to use; curr_timestamp: current timestamp, if not provided current_timestamp() function is used                |
| not_in_near_future                                   | Check if input column defined as date is not in the near future (near future defined as grater than current timestamp but less than current timestamp + offset) | col_name: column name to check; offset: offset to use; curr_timestamp: current timestamp, if not provided current_timestamp() function is used                |
| is_older_than_n_days                                 | Check if input column is older than n number of days                                                                                                            | col_name: column name to check; days: number of days; curr_date: current date, if not provided current_date() function is used                                |
| is_older_than_col2_for_n_days                        | Check if one column is not older than another column by n number of days                                                                                        | col_name1: first column name to check; col_name2: second column name to check; days: number of days                                                           |
| regex_match                                          | Check if input column matches a given regex                                                                                                                     | col_name: column name to check; regex: regex to check; negate: if the condition should be negated (true) or not                                               |
| sql_expression                                       | Check if input column is matches the provided sql expression, eg. a = 'str1', a > b                                                                             | expression: sql expression to check; msg: optional message to output; name: optional name of the resulting column; negate: if the condition should be negated |

You can check implementation details of the rules [here](src/databricks/labs/dqx/col_functions.py).

## Creating your own checks

If a check that you need does not exist in DQX, you may be able to define it using sql expression rule (`sql_expression`),
for example:
```yaml
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: a LIKE %foo
      msg: a ends with foo
```

Sql expression is also useful if you want to make cross-field validation, for example:
```yaml
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: a > b
      msg: a is greater than b
```

Alternatively, you can define your own checks. A check is a function available from 'globals' that returns `pyspark.sql.Column`, for example:

```python
import pyspark.sql.functions as F
from pyspark.sql import Column
from databricks.labs.dqx.col_functions import make_condition

def ends_with_foo(col_name: str) -> Column:
    column = F.col(col_name)
    return make_condition(~(column.endswith("foo")), f"Column {col_name} ends with foo", f"{col_name}_ends_with_foo")
```

Then define the check, for example:
```yaml
- criticality: error
  check:
    function: ends_with_foo
    arguments:
      col_name: a
```

You can see all existing DQX checks [here](src/databricks/labs/dqx/col_functions.py). 

Feel free to submit a PR to DQX with your own check so that other can benefit from it (see [contribution guide](#contribution)).

[[back to top](#databricks-labs-dqx)]

# Contribution

See contribution guidance [here](CONTRIBUTING.md).

[[back to top](#databricks-labs-dqx)]

# Project Support

Please note that this project is provided for your exploration only and is not 
formally supported by Databricks with Service Level Agreements (SLAs). They are 
provided AS-IS, and we do not make any guarantees of any kind. Please do not 
submit a support ticket relating to any issues arising from the use of this project.

Any issues discovered through the use of this project should be filed as GitHub 
[Issues on this repository](https://github.com/databrickslabs/dqx/issues). 
They will be reviewed as time permits, but no formal SLAs for support exist.

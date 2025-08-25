# Version changelog

## 0.9.1

## 0.9.1

* Added quality checker and end to end workflows ([#519](https://github.com/databrickslabs/dqx/issues/519)). This release introduces no-code solution for applying checks. The following workflows were added: quality-checker (apply checks and save results to tables) and end-to-end (e2e) workflows (profile input data, generate quality checks, apply the checks, save results to tables). The workflows enable quality checking for data at-rest without the need for code-level integration. It supports reference data for checks using tables (e.g., required by foreign key or compare datasets checks) as well as custom python check functions (mapping of custom check funciton to the module path in the workspace or Unity Catalog volume containing the function definition). The workflows handle one run config for each job run. Future release will introduce functionality to execute this across multiple tables. In addition, CLI commands have been added to execute the workflows. Additionaly, DQX workflows are configured now to execute using serverless clusters, with an option to use standards clusters as well. InstallationChecksStorageHandler now support absolute workspace path locations. 
* Added built-in row-level check for PII detection ([#486](https://github.com/databrickslabs/dqx/issues/486)). Introduced a new built-in check for Personally Identifiable Information (PII) detection, which utilizes the Presidio framework and can be configured using various parameters, such as NLP entity recognition configuration. This check can be defined using the `does_not_contain_pii` check function and can be customized to suit specific use cases. The check requires `pii` extras to be installed: `pip install databricks-labs-dqx[pii]`. Furthermore, a new enum class `NLPEngineConfig` has been introduced to define various NLP engine configurations for PII detection. Overall, these updates aim to provide more robust and customizable quality checking capabilities for detecting PII data. 
* Added equality row-level checks ([#535](https://github.com/databrickslabs/dqx/issues/535)). Two new row-level checks, `is_equal_to` and `is_not_equal_to`, have been introduced to enable equality checks on column values, allowing users to verify whether the values in a specified column are equal to or not equal to a given value, which can be a numeric literal, column expression, string literal, date literal, or timestamp literal. 
* Added demo for Spark Structured Streaming ([#518](https://github.com/databrickslabs/dqx/issues/518)). Added demo to showcase usage of DQX with Spark Structured Streaming for in-transit data quality checking. The demo is available as Databricks notebook, and can be run on any Databricks workspace.
* Added clarification to profiler summary statistics ([#523](https://github.com/databrickslabs/dqx/issues/523)). Added new section on understanding summary statistics, which explains how these statistics are computed on a sampled subset of the data and provides a reference for the various summary statistics fields. 
* Fixed rounding datetimes in the checks generator ([#517](https://github.com/databrickslabs/dqx/issues/517)). The generator has been enhanced to correctly handle midnight values when rounding "up", ensuring that datetime values already at midnight remain unchanged, whereas previously they were rounded to the next day. 
* Added API Docs ([#520](https://github.com/databrickslabs/dqx/issues/520)). The DQX API documentation is generated automatically using docstrings. As part of this change the library's documentation has been updated to follow Google style.
* Improved test automation by adding end-to-end test for the asset bundles demo ([#533](https://github.com/databrickslabs/dqx/issues/533)).

BREAKING CHANGES!

* `ExtraParams` was moved from `databricks.labs.dqx.rule` module to `databricks.labs.dqx.config`

## 0.9.0

* Added quality checker and end to end workflows ([#519](https://github.com/databrickslabs/dqx/issues/519)). This release introduces no-code solution for applying checks. The following workflows were added: quality-checker (apply checks and save results to tables) and end-to-end (e2e) workflows (profile input data, generate quality checks, apply the checks, save results to tables). The workflows enable quality checking for data at-rest without the need for code-level integration. It supports reference data for checks using tables (e.g., required by foreign key or compare datasets checks) as well as custom python check functions (mapping of custom check funciton to the module path in the workspace or Unity Catalog volume containing the function definition). The workflows handle one run config for each job run. Future release will introduce functionality to execute this across multiple tables. In addition, CLI commands have been added to execute the workflows. Additionaly, DQX workflows are configured now to execute using serverless clusters, with an option to use standards clusters as well. InstallationChecksStorageHandler now support absolute workspace path locations. 
* Added built-in row-level check for PII detection ([#486](https://github.com/databrickslabs/dqx/issues/486)). Introduced a new built-in check for Personally Identifiable Information (PII) detection, which utilizes the Presidio framework and can be configured using various parameters, such as NLP entity recognition configuration. This check can be defined using the `does_not_contain_pii` check function and can be customized to suit specific use cases. The check requires `pii` extras to be installed: `pip install databricks-labs-dqx[pii]`. Furthermore, a new enum class `NLPEngineConfig` has been introduced to define various NLP engine configurations for PII detection. Overall, these updates aim to provide more robust and customizable quality checking capabilities for detecting PII data. 
* Added equality row-level checks ([#535](https://github.com/databrickslabs/dqx/issues/535)). Two new row-level checks, `is_equal_to` and `is_not_equal_to`, have been introduced to enable equality checks on column values, allowing users to verify whether the values in a specified column are equal to or not equal to a given value, which can be a numeric literal, column expression, string literal, date literal, or timestamp literal. 
* Added demo for Spark Structured Streaming ([#518](https://github.com/databrickslabs/dqx/issues/518)). Added demo to showcase usage of DQX with Spark Structured Streaming for in-transit data quality checking. The demo is available as Databricks notebook, and can be run on any Databricks workspace.
* Added clarification to profiler summary statistics ([#523](https://github.com/databrickslabs/dqx/issues/523)). Added new section on understanding summary statistics, which explains how these statistics are computed on a sampled subset of the data and provides a reference for the various summary statistics fields. 
* Fixed rounding datetimes in the checks generator ([#517](https://github.com/databrickslabs/dqx/issues/517)). The generator has been enhanced to correctly handle midnight values when rounding "up", ensuring that datetime values already at midnight remain unchanged, whereas previously they were rounded to the next day. 
* Added API Docs ([#520](https://github.com/databrickslabs/dqx/issues/520)). The DQX API documentation is generated automatically using docstrings. As part of this change the library's documentation has been updated to follow Google style.
* Improved test automation by adding end-to-end test for the asset bundles demo ([#533](https://github.com/databrickslabs/dqx/issues/533)).

BREAKING CHANGES!

* `ExtraParams` was moved from `databricks.labs.dqx.rule` module to `databricks.labs.dqx.config`


## 0.8.0

* Added new row-level freshness check ([#495](https://github.com/databrickslabs/dqx/issues/495)). A new data quality check function, `is_data_fresh`, has been introduced to identify stale data resulting from delayed pipelines, enabling early detection of upstream issues. This function assesses whether the values in a specified timestamp column are within a specified number of minutes from a base timestamp column. The function takes three parameters: the column to check, the maximum age in minutes before data is considered stale, and an optional base timestamp column, defaulting to the current timestamp if not provided. 
* Added new dataset-level freshess check ([#499](https://github.com/databrickslabs/dqx/issues/499)). A new dataset-level check function, `is_data_fresh_per_time_window`, has been added to validate whether at least a specified minimum number of records arrive within every specified time window, ensuring data freshness. This function is customizable, allowing users to define the time window, minimum records per window, and lookback period. 
* Improvements have been made to the performance of aggregation check functions, and the check message format has been updated for better readability. 
* Created llm util function to get check functions details ([#469](https://github.com/databrickslabs/dqx/issues/469)). A new utility function has been introduced to provide definitions of all check functions, enabling the generation of prompts for Large Language Models (LLMs) to create check functions.
* Added equality safe row and column matching in compare datasets check ([#473](https://github.com/databrickslabs/dqx/issues/473)). The compare datasets check functionality has been enhanced to handle null values during row matching and column value comparisons, improving its robustness and flexibility. Two new optional parameters, `null_safe_row_matching` and `null_safe_column_value_matching`, have been introduced to control how null values are handled, both defaulting to True. These parameters allow for null-safe primary key matching and column value matching, ensuring accurate comparison results even when null values are present in the data. The check now excludes specific columns from value comparison using the `exclude_columns` parameter while still considering them for row matching. 
* Fixed datetime rounding logic in profiler ([#483](https://github.com/databrickslabs/dqx/issues/483)). The datetime rounding logic has been improved in profiler to respect the `round=False` option, which was previously ignored. The code now handles the `OverflowError` that occurs when rounding up the maximum datetime value by capping the result and logging a warning. 
* Added loading and saving checks from file in Unity Catalog Volume ([#512](https://github.com/databrickslabs/dqx/issues/512)). This change introduces support for storing quality checks in a Unity Catalog Volume, in addition to existing storage types such as tables, files, and workspace files. The storage location of quality checks has been unified into a single configuration field called `checks_location`, replacing the previous `checks_file` and `checks_table` fields, to simplify the configuration and remove ambiguity by ensuring only one storage location can be defined per run configuration.  The `checks_location` field can point to a file in the local path, workspace, installation folder, or Unity Catalog Volume, providing users with more flexibility and clarity when managing their quality checks. 
* Refactored methods for loading and saving checks ([#487](https://github.com/databrickslabs/dqx/issues/487)). The `DQEngine` class has undergone significant changes to improve modularity and maintainability, including the unification of methods for loading and saving checks under the `load_checks` and `save_checks` methods, which take a `config` parameter to determine the storage type, such as `FileChecksStorageConfig`, `WorkspaceFileChecksStorageConfig`, `TableChecksStorageConfig`, or `InstallationChecksStorageConfig`.
* Storing checks using dqx classes ([#474](https://github.com/databrickslabs/dqx/issues/474)). The data quality engine has been enhanced with methods to convert quality checks between `DQRule` objects and Python dictionaries, allowing for flexibility in check definition and usage. The `serialize_checks` method converts a list of `DQRule` instances into a dictionary representation, while the `deserialize_checks` method performs the reverse operation, converting a dictionary representation back into a list of `DQRule` instances. Additionally, the `DQRule` class now includes a `to_dict` method to convert a `DQRule` instance into a structured dictionary, providing a standardized representation of the rule's metadata. These changes enable users to work with checks in both formats, store and retrieve checks easily, and improve the overall management and storage of data quality checks. The conversion process supports local execution and handles non-complex column expressions, although complex PySpark expressions or Python functions may not be fully reconstructable when converting from class to metadata format.
* Added llm utility funciton to extract checks examples in yaml from docs ([#506](https://github.com/databrickslabs/dqx/issues/506)). This is achieved through a new Python script that extracts YAML examples from MDX documentation files and creates a combined YAML file with all the extracted examples. The script utilizes regular expressions to extract YAML code blocks from MDX content, validates each YAML block, and combines all valid blocks into a single list. The combined YAML file is then created in the LLM resources directory for use in language model processing. 

BREAKING CHANGES!

* The `checks_file` and `checks_table` fields have been removed from the installation run configuration. They are now consolidated into the single `checks_location` field. This change simplifies the configuration and clearly defines where checks are stored.
* The `load_run_config` method has been moved to `config_loader.RunConfigLoader`, as it is not intended for direct use and falls outside the `DQEngine` core responsibilities.

DEPRECIATION CHANGES!

If you are loading or saving checks from a storage (file, workspace file, table, installation), you are affected. We are deprecating the below methods. We are keeping the methods in the `DQEngine` but you should update your code as these methods will be removed in future versions.
* Loading checks to storage has been unified under `load_checks` method. The following methods have been removed from the `DQEngine`:
`load_checks_from_local_file`, `load_checks_from_workspace_file`, `load_checks_from_installation`, `load_checks_from_table`.
* Saving checks in storage has been unified under `load_checks` method. The following methods have been removed from the `DQEngine`:
`save_checks_in_local_file`, `save_checks_in_workspace_file`, `save_checks_in_installation`, `save_checks_in_table`.

The `save_checks` and  `load_checks` take `config` as a parameter, which determines the storage types used. The following storage configs are currently supported: 
* `FileChecksStorageConfig`: file in the local filesystem (YAML or JSON)
* `WorkspaceFileChecksStorageConfig`: file in the workspace (YAML or JSON)
* `TableChecksStorageConfig`: a table
* `InstallationChecksStorageConfig`: storage defined in the installation context, using either the `checks_table` or `checks_file` field from the run configuration.

## 0.7.1

* Added type validation for apply checks method ([#465](https://github.com/databrickslabs/dqx/issues/465)). The library now enforces stricter type validation for data quality rules, ensuring all elements in the checks list are instances of `DQRule`. If invalid types are encountered, a `TypeError` is raised with a descriptive error message, suggesting alternative methods for passing checks as dictionaries. Additionally, input attribute validation has been enhanced to verify the criticality value, which must be either `warn` or "error", and raises a `ValueError` for invalid values.
* Databricks Asset Bundle (DAB) demo ([#443](https://github.com/databrickslabs/dqx/issues/443)). A new demo showcasing the usage of DQX with DAB has been added.
* Check to compare datasets ([#463](https://github.com/databrickslabs/dqx/issues/463)). A new dataset-level check, `compare_datasets`, has been introduced to compare two DataFrames at both row and column levels, providing detailed information about differences, including new or missing rows and column-level changes. This check compares only columns present in both DataFrames, excludes map type columns, and can be customized to exclude specific columns or perform a FULL OUTER JOIN to identify missing records. The `compare_datasets` check can be used with a reference DataFrame or table name, and its results include information about missing and extra rows, as well as a map of changed columns and their differences. 
* Demo on how to use DQX with dbt projects ([#460](https://github.com/databrickslabs/dqx/issues/460)). New demo has been added to showcase on how to use DQX with dbt transformation framework.
* IP V4 address validation ([#464](https://github.com/databrickslabs/dqx/issues/464)). The library has been enhanced with new checks to validate IPv4 address. Two new row checks, `is_valid_ipv4_address` and `is_ipv4_address_in_cidr`, have been introduced to verify whether values in a specified column are valid IPv4 addresses and whether they fall within a given CIDR block, respectively.
* Improved loading checks from Delta table ([#462](https://github.com/databrickslabs/dqx/issues/462)). Loading checks from Delta tables have been improved to eliminate the need to escape string arguments, providing a more robust and user-friendly experience for working with quality checks defined in Delta tables.

## 0.7.0

* Added end-to-end quality checking methods ([#364](https://github.com/databrickslabs/dqx/issues/364)). The library now includes end-to-end quality checking methods, allowing users to read data from a table or view, apply checks, and write the results to a table. The `DQEngine` class has been updated to utilize `InputConfig` and `OutputConfig` objects to handle input and output configurations, providing more flexibility in the quality checking flow. The `apply_checks_and_write_to_table` and `apply_checks_by_metadata_and_write_to_table` methods have been introduced to support this functionality, applying checks using DQX classes and configuration, respectively. Additionally, the profiler configuration options have been reorganized into `input_config` and `profiler_config` sections, making it easier to understand and customize the profiling process. The changes aim to provide a more streamlined and efficient way to perform end-to-end quality checking and data validation, with improved configuration flexibility and readability.
* Added equality checks for aggregate values and negate option for Foreign Key ([#387](https://github.com/databrickslabs/dqx/issues/387)). The library now includes two new checks, `is_aggr_equal` and `is_aggr_not_equal`, which enable users to perform equality checks on aggregate values, such as count, sum, average, minimum, and maximum, allowing verification that an aggregation on a column or group of columns is equal to or not equal to a specified limit. These checks can be configured with a criticality level of either `error` or `warn` and can be applied to specific columns or groups of columns. Additionally, the `foreign_key` check has been updated with a `negate` option, allowing the condition to be negated so that the check fails when the foreign key values exist in the reference dataframe or table, rather than when they do not exist. This expanded functionality enhances the library's data quality checking capabilities, providing more flexibility and power in validating data integrity.
* Extend options for profiling multiple tables ([#420](https://github.com/databrickslabs/dqx/issues/420)). The profiler now supports wildcard patterns for profiling multiple tables, replacing the previous regex pattern support, and options can be passed as a list of dictionaries to apply different options to each table based on pattern matching. The profiler job is setup now with IO cache enabled cluste.
* Improved quick demo to showcase defining checks using DQX classes and renamed DLT into Lakeflow Pipeline in docs ([#399](https://github.com/databrickslabs/dqx/issues/399)). `DLT` has been renamed to `Lakeflow Pipeline` in documentation and docstrings, to maintain consistency in terminology. The quick demo has been enhanced to showcase defining checks using DQX classes, providing a more comprehensive approach to data quality validation. Additionally, performance information related to dataset-level checks has been added to the documentation, and instructions on how to use the Environment to install DQX in Lakeflow Pipelines have been provided.
* Populate columns in the results from kwargs of the check if provided ([#416](https://github.com/databrickslabs/dqx/issues/416)). Additionally, the `sql_expression` now supports optional `columns` argument that is propagated to the results.

## 0.6.0

* Added Demo from Data and AI Summit 2025 - DQX Demo for Manufacturing Industry ([#391](https://github.com/databrickslabs/dqx/issues/391)).
* Added Github Action to check if all commits are signed ([#392](https://github.com/databrickslabs/dqx/issues/392)). The library now includes a GitHub Action that automates the verification of signed commits in pull requests, enhancing the security and integrity of the codebase. This action checks each commit in a pull request to ensure it is signed using the `git commit -S` command, and if any unsigned commits are found, it posts a comment with instructions on how to properly sign commits.
* Added methods to profile multiple tables ([#374](https://github.com/databrickslabs/dqx/issues/374)). The data profiling feature has been significantly enhanced with the introduction of two new methods, `profile_table` and `profile_tables`, which enable direct profiling of Delta tables, allowing users to generate summary statistics and candidate data quality rules. These methods provide a convenient way to profile data stored in Delta tables, with `profile_table` generating a profile from a single Delta table and `profile_tables` generating profiles from multiple Delta tables using explicit table lists or regex patterns for inclusion and exclusion. The profiling process is highly customizable, supporting extensive configuration options such as sampling, outlier detection, null value handling, and string handling. The generated profiles can be used to create Delta Live Tables expectations for enforcing data quality rules, and the profiling results can be stored in a table or file as YAML or JSON for easy management and reuse.
* Created a quick start demo ([#367](https://github.com/databrickslabs/dqx/issues/367)). A new demo notebook has been introduced to provide a quickstart guide for utilizing the library, enabling users to easily test features using the Databricks Power Tools and Databricks Extension in VS Code. This demo notebook showcases both configuration styles side by side, applying the same rules for direct comparison, and includes a small, hardcoded sample dataset for quick experimentation, designed to be executed cell-by-cell in VS Code. The addition of this demo aims to help new users understand and compare both configuration approaches in a practical context, facilitating a smoother onboarding experience.
* Added Dataset-level checks, Foreign Key and SQL Script checks ([#375](https://github.com/databrickslabs/dqx/issues/375)). The data quality library has been enhanced with the introduction of dataset-level checks, which allow users to apply quality checks at the dataset level, in addition to existing row-level checks. Similar to row-level checks, the results of the dataset-level quality checks are reported for each individual row in the result columns. A new `DQDatasetRule` class has been added to define dataset-level checks, and several new check functions have been added including the `foreign_key` and `sql_query` dataset-level checks. The library now also supports custom dataset-level checks using arbitrary SQL queries and provides the ability to define checks on multiple DataFrames or Tables. The `DQEngine` class has been modified to optionally accept Spark session as a parameter in its constructor, allowing users to pass their own Spark session. Major internal refactorization has been carried out to improve code maintenance and structure. 
* Pin GitHub URLs in docs to the latest released version ([#390](https://github.com/databrickslabs/dqx/issues/390)). The formatting process now includes an additional step to update GitHub URLs, ensuring they point to the latest released version instead of the main branch, which helps prevent access to unreleased changes. This update is automated during the release process and allows users to review changes before committing.

BREAKING CHANGES!

* Moved existing `is_unique`, `is_aggr_not_greater_than` and `is_aggr_not_less_than` checks under dataset-level checks umbrella. These checks must be defined using `DQDatasetRule` class and not `DQRowRule` anymore. Input parameters remain the same as before. This is a breaking change for checks defined using DQX classes. Yaml/Json definitions are not affected.
* `DQRowRuleForEachCol` has been renamed to `DQForEachColRule` to make it generic and handle both row and dataset level rules.
* Renamed `column_names` to `result_column_names` in the `ExtraParams` for clarity as they may be confused with column(s) specified for the rules itself. This is a breaking change!
## 0.5.0

*  Fix spark remote version detection in CI (#342) [#342](https://github.com/databrickslabs/dqx/pull/342)
*  Fix spark remote installation [#346](https://github.com/databrickslabs/dqx/pull/346)
*  Load and save checks from a Delta table [#339](https://github.com/databrickslabs/dqx/pull/339)
*  Handle nulls in uniqueness check for composite keys [(#345)](https://github.com/databrickslabs/dqx/pull/345)
*  Allow user metadata for individual checks [#352](https://github.com/databrickslabs/dqx/pull/352)
*  Add functionality to save results in delta table [#319](https://github.com/databrickslabs/dqx/pull/319)
*  Fix checks older than [#354](https://github.com/databrickslabs/dqx/pull/354)
*  Add PII-detection example [#358](https://github.com/databrickslabs/dqx/pull/358)
*  Add aggregation type of checks [#357](https://github.com/databrickslabs/dqx/pull/357)

## 0.4.0

* Added input spark options and schema for reading from the storage ([#312](https://github.com/databrickslabs/dqx/issues/312)). This commit enhances the data quality framework used for profiling and validating data in a Databricks workspace with new options and functionality for reading data from storage. It allows for the usage of input spark options and schema, and supports fully qualified Unity Catalog or Hive Metastore table names in the format of catalog.schema.table or schema.table. Additionally, the code now includes a new dataclass field, input_schema, and a new dictionary field, input_read_options, to the RunConfig class. The documentation is updated with examples of how to use the new functionality.
* Added an example of uniqueness check for composite key ([#312](https://github.com/databrickslabs/dqx/issues/312)). Additionally, the code now includes a new dataclass field, input_schema, and a new dictionary field, input_read_options, to the RunConfig class. The documentation is updated with examples of how to use the new functionality.
* Renamed row checks module for more clarity ([#314](https://github.com/databrickslabs/dqx/issues/314)). This change renames the `col_check_functions` module to `row_checks` for clarity and to distinguish it from other types of checks. The `import *` syntax is removed and unused imports are removed from the demo. This change requires updating import statements that reference `col_check_functions` to use the new name `row_checks`. Checks defined using DQX classes require a simple update.

## 0.3.1

* Removed usage of lambda in quality checking ([#310](https://github.com/databrickslabs/dqx/issues/310)). We have replaced the usage of lambda functions n the quality checking with a more efficient implementation, and updated the method to handle optional arguments in validation. These changes improve the performance of the quality checking.

## 0.3.0

* Added sampling to the profiler ([#303](https://github.com/databrickslabs/dqx/issues/303)). The profiler's performance has been significantly improved in this release through the addition of sampling and limiting the input data. The profiler now samples input data with a 30% sampling factor and limits the number of records to 1000 by default, reducing the amount of data processed and enhancing performance. These changes are configurable and can be customized. This resolves issue [#215](https://github.com/databrickslabs/dqx/issues/215).
* Added support for complex column types like struct, map and array. The support is added by extending the col_name to accept expressions ([#214](https://github.com/databrickslabs/dqx/issues/214)). Comprehensive examples have been included in the demo and documentation on how to apply checks on complex types.
* Fixed profiler bug when trying to cast a decimal string to int ([#211](https://github.com/databrickslabs/dqx/issues/211)). This modification resolves issue [#172](https://github.com/databrickslabs/dqx/issues/172) and ensures proper handling of decimal strings during the casting process. This enhancement improves the profiler's robustness and flexibility when processing different data types, specifically integers and decimals.
* Renamed DQRule to DQColRule, and DQRuleColSet to DQColSetRule ([#300](https://github.com/databrickslabs/dqx/issues/300)). In this release, the class names `DQRule` and `DQRuleColSet` have been renamed to `DQRuleCol` and `DQColSetRule`, respectively, to support the addition of more rule types in the future, such as `DQDatasetRule`. The renaming includes corresponding changes in imports and method calls throughout the codebase. A deprecation warning has been added to the old classes. In addition, the `col_functions` module has been renamed to `col_check_functions`. This introduces a breaking change!. It is recommended to to update any references to the old class names in your code to ensure a smooth transition.
* Trim autogenerated check name to 255 chars ([#301](https://github.com/databrickslabs/dqx/issues/301)). This change ensures that potential issues arising from long check names are avoided by truncating the auto-generated check name to a reasonable lenght.
* Updated sql expression logic ([#212](https://github.com/databrickslabs/dqx/issues/212)). In this release, the SQL expression logic in our data quality library has been updated to cause the `sql_expression` check to fail if the condition is not met, introducing a potential breaking change.
* Added context info to output ([#206](https://github.com/databrickslabs/dqx/issues/206)). Additional context information is now added to the results of quality checks, including name, message, column name, filter, function, runtime, and user-provided metadata for every failed check. This allows users to provide custom metadata that is stored in the reporting columns for failed checks. This change is a breaking change for checks defined using classes! It is advised to consult the latest documentation for the updated syntax of defining checks using DQX classes.

## 0.2.0

* Added uniqueness check([#200](https://github.com/databrickslabs/dqx/issues/200)). A uniqueness check has been added, which reports an issue for each row containing a duplicate value in a specified column. This resolves issue [154](https://github.com/databrickslabs/dqx/issues/154).
* Added column expression support for limits in not less and not greater than checks, and updated docs ([#200](https://github.com/databrickslabs/dqx/issues/200)). This commit introduces several changes to simplify and enhance data quality checking in PySpark workloads for both streaming and batch data. The naming conventions of rule functions have been unified, and the `is_not_less_than` and `is_not_greater_than` functions now accept column names or expressions as limits. The input parameters for range checks have been unified, and the logic of `is_not_in_range` has been updated to be inclusive of the boundaries. The project's documentation has been improved, with the addition of comprehensive examples, and the contribution guidelines have been clarified. This change includes a breaking change for some of the checks. Users are advised to review and test the changes before implementation to ensure compatibility and avoid any disruptions. Reslves issues: [131](https://github.com/databrickslabs/dqx/issues/131), [197](https://github.com/databrickslabs/dqx/pull/200), [175](https://github.com/databrickslabs/dqx/issues/175), [205](https://github.com/databrickslabs/dqx/issues/205)
* Include predefined check functions by default when applying custom checks by metadata ([#203](https://github.com/databrickslabs/dqx/issues/203)). The data quality engine has been updated to include predefined check functions by default when applying custom checks using metadata in the form of YAML or JSON. This change simplifies the process of defining custom checks, as users no longer need to manually import predefined functions, which were previously required and could be cumbersome. The default behavior now is to import all predefined checks. The `validate_checks` method has been updated to accept a dictionary of custom check functions instead of global variables. This improvement resolves issue [#48](https://github.com/databrickslabs/dqx/issues/48).

## 0.1.13

* Fixed cli installation and demo ([#177](https://github.com/databrickslabs/dqx/issues/177)). In this release, changes have been made to adjust the dashboard name, ensuring compliance with new API naming rules. The dashboard name now only contains alphanumeric characters, hyphens, or underscores, and the reference section has been split for clarity. In addition, demo for the tool has been updated to work regardless if a path or UC table is provided in the config. Furthermore, documentation has been refactored and udpated to improve clarity. The following issue have been closed: [#171](https://github.com/databrickslabs/dqx/issues/171) and [#198](https://github.com/databrickslabs/dqx/issues/198).
* [Feature] Update is_(not)_in_range ([#87](https://github.com/databrickslabs/dqx/issues/87)) to support max/min limits from col ([#153](https://github.com/databrickslabs/dqx/issues/153)). In this release, the `is_in_range` and `is_not_in_range` quality rule functions have been updated to support a column as the minimum or maximum limit, in addition to a literal value. This change is accomplished through the introduction of optional `min_limit_col_expr` and `max_limit_col_expr` arguments, allowing users to specify a column expression as the minimum or maximum limit. Extensive testing, including unit tests and integration tests, has been conducted to ensure the correct behavior of the new functionality. These enhancements offer increased flexibility when defining quality rules, catering to a broader range of use cases and scenarios.


## 0.1.12

* Fixed installation process for Serverless ([#150](https://github.com/databrickslabs/dqx/issues/150)). This commit removes the pyspark dependency from the library to avoid spark version conflicts in Serverless and future DBR versions. CLI has been updated to install pyspark for local command execution.
* Updated demos and documentation ([#169](https://github.com/databrickslabs/dqx/issues/169)). In this release, the quality checks in the demos have been updated to better showcase the capabilities of DQX. Documentation has been updated in various places for increased clarity. Additional contributing guides have been added.


## 0.1.11

* Provided option to customize reporting column names ([#127](https://github.com/databrickslabs/dqx/issues/127)). In this release, the DQEngine library has been enhanced to allow for customizable reporting column names. A new constructor has been added to DQEngine, which accepts an optional ExtraParams object for extra configurations. A new Enum class, DefaultColumnNames, has been added to represent the columns used for error and warning reporting. New tests have been added to verify the application of checks with custom column naming. These changes aim to improve the customizability, flexibility, and user experience of DQEngine by providing more control over the reporting columns and resolving issue [#46](https://github.com/databrickslabs/dqx/issues/46).
* Fixed parsing error when loading checks from a file ([#165](https://github.com/databrickslabs/dqx/issues/165)). In this release, we have addressed a parsing error that occurred when loading checks (data quality rules) from a file, fixing issue [#162](https://github.com/databrickslabs/dqx/issues/162). The specific issue being resolved is a SQL expression parsing error. The changes include refactoring tests to eliminate code duplication and improve maintainability, as well as updating method and variable names to use `filepath` instead of "path". Additionally, new unit and integration tests have been added and manually tested to ensure the correct functionality of the updated code.
* Removed usage of try_cast spark function from the checks to make sure DQX can be run on more runtimes ([#163](https://github.com/databrickslabs/dqx/issues/163)). In this release, we have refactored the code to remove the usage of the `try_cast` Spark function and replace it with `cast` and `isNull` checks to improve code compatibility, particularly for runtimes where `try_cast` is not available. The affected functionality includes null and empty column checks, checking if a column value is in a list, and checking if a column value is a valid date or timestamp. We have added unit and integration tests to ensure functionality is working as intended. 
* Added filter to rules so that you can make conditional checks ([#141](https://github.com/databrickslabs/dqx/issues/141)). The filter serves as a condition that data must meet to be evaluated by the check function. The filters restrict the evaluation of checks to only apply to rows that meet the specified conditions. This feature enhances the flexibility and customizability of data quality checks in the DQEngine.


## 0.1.10

*  Support datetime arguments for column range functions (#142) [View](https://github.com/databrickslabs/dqx/pull/142)
*  DQX engine refactor and docs update (#138) [View](https://github.com/databrickslabs/dqx/pull/138)
*  Add column functions to check for valid date strings (#144) [View](https://github.com/databrickslabs/dqx/pull/144)
*  Generate rules for DLT as Python dictionary (#148) [View](https://github.com/databrickslabs/dqx/pull/148)
*  Make DQX compatible with Serverless (#147) [View](https://github.com/databrickslabs/dqx/pull/147)

## 0.1.9

* New dashboard query, Update to demos and docs [#133](https://github.com/databrickslabs/dqx/pull/133)
* Patch user agent to enable tracking [#121](https://github.com/databrickslabs/dqx/pull/121)
* Added docs build on push [#129](https://github.com/databrickslabs/dqx/pull/129)

## 0.1.8

* Add Dashboard as Code, DQX Data Quality Summmary Dashboard [#86](https://github.com/databrickslabs/dqx/pull/86)
* Updated profiling documentation with cost consideration [#126](https://github.com/databrickslabs/dqx/pull/126)
* Improve docs styling [#118](https://github.com/databrickslabs/dqx/pull/118)
* Added search for docs [#119](https://github.com/databrickslabs/dqx/pull/119)
* Updated docs [#117](https://github.com/databrickslabs/dqx/pull/117)

## 0.1.7

* Bug fixed profiler
* Released docs at https://databrickslabs.github.io/dqx/
* Updated README

## 0.1.6

* Added new check: is_not_null_and_not_empty_array
* Fixed links to image for pypi
* Minor documentation updates

## 0.1.5

* Updated release process
* Updated README

## 0.1.4

* Updated release process

## 0.1.1

* Bug fixed cli installation
* Fixed Github release process
* Updated demos

## 0.1.0

Initial release of the project

## 0.0.0

Initial dqx commit

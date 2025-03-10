# Version changelog

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

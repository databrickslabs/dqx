# Version changelog

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

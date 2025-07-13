Deploying DQX jobs with Databricks Asset Bundles
===
This demo shows how to use [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) to deploy workloads that use DQX as a library.

## Prerequisites
Using Databricks Asset Bundles requires local installation of the [Databricks Command Line Interface (CLI)](https://docs.databricks.com/aws/en/dev-tools/cli/).

Once installed, you must configure authentication between the Databricks CLI and your target workspace.

## Resources
The directory includes:
- [Asset bundle configuration file](/demos/dqx_demo_asset_bundle/databricks.yml) for deploying and running the job
- [Example notebook](/demos/dqx_demo_asset_bundle/dqx_demo_notebook.py) applying DQX checks and saving the output datasets
- Quality rules files:
  - [Sensor data quality rules](/demos/dqx_demo_asset_bundle/sensor_data_quality_rules.yml)
  - [Maintenance data quality rules](/demos/dqx_demo_asset_bundle/maint_data_quality_rules.yml)

## Deploying the job
Navigate to `/dqx_demo_asset_bundle` and execute the following commands:

```
databricks bundle deploy
```
***Note:** We can set the Databricks profile for running CLI commands by specifying the `--profile` option.*

## Running the job
To run the job, execute another CLI command:

```
databricks bundle run
```

## Removing the job, notebook, and files
To tear-down the job, notebook, and files, run a final CLI command:

```
databricks bundle destroy
```

***Note:** Catalogs, schemas, or tables created by running the notebook should be removed manually.*
Running DQX with DBT projects
===

This demo shows how to apply data quality checks from [dbt projects](https://github.com/dbt-labs/dbt-core).
The DQX quality checking is executed using [dbt python models](https://docs.getdbt.com/docs/build/python-models). 

## Prerequisites

* [Install dbt](https://docs.getdbt.com/docs/core/pip-install) and required adapters:
```bash
# install dbt core and dbt databricks adapter
pip install dbt-core dbt-databricks databricks-labs-dqx

# verify installation
dbt --version
```

## Project configuration

Open `profiles.yml` file and update the following parameters:
* `http_path` to specify Databricks SQL Warehouse ID (`<warehouse_id>`) to use to execute the dbt sql models.
* (optionally) default `catalog` and `schema` to use (default: `main.default`).

The project is using serverless cluster by default to execute dbt python models (dqx quality checks).
To change the default cluster, open `dbt_project.yml` file and update `submission_method` (see more [here](https://docs.getdbt.com/docs/build/python-models)).

## Execution

Provide authentication credentials to connect to the Databricks workspace by running the following in your console:
```bash
export DBT_ACCESS_TOKEN=<databricks_pat_token>
export DBT_HOST=<databricks_workspace_url>
export DBT_WAREHOUSE_ID=<warehouse_id>
```

Navigate to `/dqx_demo_dbt` and execute the following commands:
```bash
dbt run
```

This will create the following tables:
- `main.default.dummy_model` - input table with dummy data
- `main.default.dummy_model_dqx` - output table of quality checking containing DQX reporting columns

***Note:** Tables created by running this dbt project should be removed manually.*

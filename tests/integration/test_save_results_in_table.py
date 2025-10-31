import pyspark.errors.exceptions.connect
import pytest
from chispa.dataframe_comparer import assert_df_equality  # type: ignore

from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidConfigError


def test_save_results_in_table(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table_mode = "overwrite"
    output_config = OutputConfig(location=output_table, mode=output_table_mode)
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    quarantine_table_mode = "overwrite"
    quarantine_config = OutputConfig(location=quarantine_table, mode=quarantine_table_mode)

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_config=output_config,
        quarantine_config=quarantine_config,
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)

    output_config.mode = "append"
    output_config.options = {"overwriteSchema": "true"}
    quarantine_config.mode = "append"
    quarantine_config.options = {"overwriteSchema": "true"}

    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_config=output_config,
        quarantine_config=quarantine_config,
    )

    assert_df_equality(output_df.union(output_df), output_df_loaded)
    assert_df_equality(quarantine_df.union(quarantine_df), quarantine_df_loaded)


def test_save_results_in_table_only_output(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table_mode = "overwrite"
    output_config = OutputConfig(location=output_table, mode=output_table_mode)

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        output_config=output_config,
    )

    output_df_loaded = spark.table(output_table)

    assert_df_equality(output_df, output_df_loaded)


def test_save_results_in_table_only_quarantine(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    quarantine_table_mode = "overwrite"
    quarantine_config = OutputConfig(location=quarantine_table, mode=quarantine_table_mode)

    schema = "a: int, b: int"
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(quarantine_df=quarantine_df, quarantine_config=quarantine_config)

    output_df_loaded = spark.table(quarantine_table)
    assert_df_equality(quarantine_df, output_df_loaded)


def test_save_results_in_table_in_user_installation(ws, spark, installation_ctx, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.output_config = OutputConfig(location=output_table)
    run_config.quarantine_config = OutputConfig(location=quarantine_table)
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)


def test_save_results_in_table_in_user_installation_only_output(ws, spark, installation_ctx, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.output_config = OutputConfig(location=output_table)
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    output_df_loaded = spark.table(output_table)
    assert_df_equality(output_df, output_df_loaded)


def test_save_results_in_table_in_user_installation_only_quarantine(
    ws, spark, installation_ctx, make_schema, make_random
):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.quarantine_config = OutputConfig(location=quarantine_table)
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        quarantine_df=quarantine_df,
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    output_df_loaded = spark.table(quarantine_table)
    assert_df_equality(quarantine_df, output_df_loaded)


def test_save_results_in_table_in_user_installation_output_table_provided(
    ws, spark, installation_ctx, make_schema, make_random
):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.quarantine_config = OutputConfig(location=quarantine_table)
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_config=OutputConfig(location=output_table),
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)


def test_save_results_in_table_in_user_installation_quarantine_table_provided(
    ws, spark, installation_ctx, make_schema, make_random
):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.output_config = OutputConfig(location=output_table)
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        quarantine_config=OutputConfig(location=quarantine_table),
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)


def test_save_results_in_table_in_user_installation_missing_output_and_quarantine_table(
    ws, spark, installation_ctx, make_schema, make_random
):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    data_schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], data_schema)
    quarantine_df = spark.createDataFrame([[3, 4]], data_schema)

    engine = DQEngine(ws, spark)
    with pytest.raises(
        pyspark.errors.exceptions.connect.AnalysisException, match="The schema `main.dqx_test` cannot be found"
    ):
        engine.save_results_in_table(
            output_df=output_df,
            quarantine_df=quarantine_df,
            run_config_name=run_config.name,
            product_name=product_name,
            assume_user=True,
        )

    assert (
        spark.sql(f"SHOW TABLES FROM {catalog_name}.{schema.name} LIKE '{output_table}'").count() == 0
    ), "Output table should not have been saved"
    assert (
        spark.sql(f"SHOW TABLES FROM {catalog_name}.{schema.name} LIKE '{quarantine_table}'").count() == 0
    ), "Quarantine table should not have been saved"


def test_save_results_in_table_missing_output_and_quarantine_dfs(ws, spark):
    engine = DQEngine(ws, spark)
    with pytest.raises(
        InvalidConfigError, match="At least one of 'output_df' or 'quarantine_df' Dataframe must be present."
    ):
        engine.save_results_in_table()


def test_save_results_in_table_in_custom_folder_installation(
    ws, spark, installation_ctx_custom_install_folder, make_schema, make_random
):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"

    config = installation_ctx_custom_install_folder.config
    run_config = config.get_run_config()
    run_config.output_config = OutputConfig(location=output_table)
    run_config.quarantine_config = OutputConfig(location=quarantine_table)
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)
    product_name = installation_ctx_custom_install_folder.product_info.product_name()
    install_folder = installation_ctx_custom_install_folder.install_folder

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
        install_folder=install_folder,
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)


def test_save_streaming_results_in_table(ws, spark, make_schema, make_random, make_volume):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"
    random_name = make_random(10).lower()
    output_table = f"{catalog_name}.{schema.name}.{random_name}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema.name)

    schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], schema)
    input_df.write.format("delta").mode("append").saveAsTable(input_table)
    streaming_input_df = spark.readStream.table(input_table)

    output_table_mode = "append"
    output_table_options = {
        "checkpointLocation": f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{random_name}"
    }
    output_table_trigger = {"availableNow": True}
    output_config = OutputConfig(
        location=output_table, mode=output_table_mode, options=output_table_options, trigger=output_table_trigger
    )

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=streaming_input_df,
        output_config=output_config,
    )

    output_df_loaded = spark.table(output_table)
    assert_df_equality(input_df, output_df_loaded)

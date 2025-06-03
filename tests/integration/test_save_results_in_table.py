from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.engine import DQEngine


def test_save_results_in_table(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_table=output_table,
        quarantine_table=quarantine_table,
        output_table_mode="overwrite",
        quarantine_table_mode="overwrite",
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)

    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_table=output_table,
        quarantine_table=quarantine_table,
        output_table_mode="append",
        quarantine_table_mode="append",
        output_table_options={"overwriteSchema": "true"},
        quarantine_table_options={"overwriteSchema": "true"},
    )

    assert_df_equality(output_df.union(output_df), output_df_loaded)
    assert_df_equality(quarantine_df.union(quarantine_df), quarantine_df_loaded)


def test_save_results_in_table_only_output(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        output_df=output_df,
        output_table=output_table,
        output_table_mode="overwrite",
        quarantine_table_mode="overwrite",
    )

    output_df_loaded = spark.table(output_table)

    assert_df_equality(output_df, output_df_loaded)


def test_save_results_in_table_only_quarantine(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    schema = "a: int, b: int"
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        quarantine_df=quarantine_df,
        quarantine_table=quarantine_table,
        output_table_mode="overwrite",
        quarantine_table_mode="overwrite",
    )

    output_df_loaded = spark.table(quarantine_table)
    assert_df_equality(quarantine_df, output_df_loaded)


def test_save_results_in_table_in_user_installation(ws, spark, installation_ctx, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.output_table = output_table
    run_config.quarantine_table = quarantine_table
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
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
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.output_table = output_table
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)

    engine = DQEngine(ws)
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
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.quarantine_table = quarantine_table
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
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
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.quarantine_table = quarantine_table
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_table=output_table,
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
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.output_table = output_table
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        quarantine_table=quarantine_table,
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
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    data_schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], data_schema)
    quarantine_df = spark.createDataFrame([[3, 4]], data_schema)

    engine = DQEngine(ws)
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

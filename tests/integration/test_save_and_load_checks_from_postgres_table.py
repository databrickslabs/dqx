from testing.postgresql import Postgresql

from sqlalchemy import create_engine, insert, select

from databricks.labs.dqx.checks_storage import LakebaseChecksStorageHandler
from databricks.labs.dqx.config import LakebaseChecksStorageConfig

from tests.conftest import compare_checks
from tests.unit.test_save_checks import TEST_CHECKS

LOCATION = "test.public.checks"


def test_lakebase_checks_storage_handler_save(ws, spark):
    with Postgresql() as postgresql:
        connection_string = postgresql.url()
        engine = create_engine(connection_string)
        handler = LakebaseChecksStorageHandler(ws, spark, engine)
        config = LakebaseChecksStorageConfig(LOCATION, connection_string)
        table = handler.get_table_definition(config.schema_name, config.table_name)

        handler.save(TEST_CHECKS, config)

        with engine.connect() as conn:
            result = conn.execute(select(table)).mappings().all()
            result = [dict(check) for check in result]

        compare_checks(result, TEST_CHECKS)


def test_lakebase_checks_storage_handler_load(ws, spark):
    with Postgresql() as postgresql:
        connection_string = postgresql.url()
        engine = create_engine(connection_string)
        handler = LakebaseChecksStorageHandler(ws, spark, engine)
        config = LakebaseChecksStorageConfig(LOCATION, connection_string)
        table = handler.get_table_definition(config.schema_name, config.table_name)

        table.metadata.create_all(engine, checkfirst=True)

        with engine.begin() as conn:
            conn.execute(insert(table), TEST_CHECKS)

        result = handler.load(config)

        compare_checks(result, TEST_CHECKS)

import cloudpickle  # type: ignore
import pytest

from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.engine import DQEngine, DQEngineCore
from databricks.labs.dqx.checks_storage import ChecksStorageHandlerFactory
from databricks.labs.dqx.config import (
    FileChecksStorageConfig,
    InstallationChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
    LakebaseChecksStorageConfig,
    VolumeFileChecksStorageConfig,
)


def test_generator_pickling(spark):
    generator = DQGenerator(spark=spark)
    serialized = cloudpickle.dumps(generator)
    assert serialized


def test_profiler_pickling(spark):
    profiler = DQProfiler(spark=spark)
    serialized = cloudpickle.dumps(profiler)
    assert serialized


def test_engine_base_pickling():
    engine_base = DQEngineBase()
    serialized = cloudpickle.dumps(engine_base)
    assert serialized


def test_engine_core_pickling(spark):
    engine_core = DQEngineCore(spark=spark)
    serialized = cloudpickle.dumps(engine_core)
    assert serialized


def test_engine_pickling(spark):
    engine = DQEngine(spark=spark)
    serialized = cloudpickle.dumps(engine)
    assert serialized


@pytest.mark.parametrize(
    "config",
    [
        (FileChecksStorageConfig("/tmp/checks.yml")),
        (InstallationChecksStorageConfig("/tmp/checks.yml")),
        (WorkspaceFileChecksStorageConfig("/tmp/checks.yml")),
        (TableChecksStorageConfig("catalog.schema.tbl")),
        (LakebaseChecksStorageConfig("cat.sch.tbl", instance_name="inst", user="user")),
        (VolumeFileChecksStorageConfig("/Volumes/catalog/schema/tmp/checks.yml")),
    ],
)
def test_config_serializer_pickling(config, spark):
    handler_factory = ChecksStorageHandlerFactory(spark=spark)
    handler = handler_factory.create(config)
    serialized = cloudpickle.dumps(handler)
    assert serialized

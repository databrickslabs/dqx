import cloudpickle  # type: ignore
import pytest

from databricks.connect import DatabricksSession
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

SPARK_MOCK = DatabricksSession.builder.serverless().getOrCreate()
STORAGE_FACTORY = ChecksStorageHandlerFactory(spark=SPARK_MOCK)


@pytest.mark.parametrize(
    "obj",
    [
        (DQGenerator()),
        (DQProfiler()),
        (DQEngineBase()),
        (DQEngine()),
        (DQEngineCore()),
        (STORAGE_FACTORY.create(FileChecksStorageConfig("/tmp/checks.yml"))),
        (STORAGE_FACTORY.create(InstallationChecksStorageConfig("/tmp/checks.yml"))),
        (STORAGE_FACTORY.create(WorkspaceFileChecksStorageConfig("/tmp/checks.yml"))),
        (STORAGE_FACTORY.create(TableChecksStorageConfig("catalog.schema.tbl"))),
        (STORAGE_FACTORY.create(LakebaseChecksStorageConfig("cat.sch.tbl", instance_name="inst", user="user"))),
        (STORAGE_FACTORY.create(VolumeFileChecksStorageConfig("/Volumes/catalog/schema/tmp/checks.yml"))),
    ],
)
def test_object_serialization(obj):
    serialized = cloudpickle.dumps(obj)
    assert serialized

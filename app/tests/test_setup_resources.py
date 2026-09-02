"""Tests for deployment-agnostic setup resource normalization."""

from pydantic import SecretStr
import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.runtime import Runtime
from databricks_labs_dqx_app.backend.setup.resources import (
    ActiveResources,
    LakebaseConnection,
    VolumeLocation,
    parse_volume_path,
    resolve_lakebase_connection,
)


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/Volumes/a/b",
        "/Volumes/a/b/c/d",
        "/Volumes/a/../c",
        "Volumes/a/b/c",
        "/Volumes/a/b/c/",
        "/Volumes/a/b/bad\nvolume",
    ],
)
def test_parse_volume_path_rejects_invalid_shapes(path: str) -> None:
    with pytest.raises(InvalidParameterError):
        parse_volume_path(path)


def test_parse_volume_path_derives_catalog_and_schema() -> None:
    assert parse_volume_path("/Volumes/main/dqx_studio/wheels") == VolumeLocation(
        catalog="main",
        schema="dqx_studio",
        volume="wheels",
        path="/Volumes/main/dqx_studio/wheels",
    )


def test_endpoint_lakebase_values_are_normalized() -> None:
    config = AppConfig(
        _env_file=None,
        lakebase_endpoint=" projects/p/branches/b/endpoints/e ",
        lakebase_database_name="databricks_postgres",
        lakebase_schema_name="dqx_studio",
    )

    assert resolve_lakebase_connection(config, {}) == LakebaseConnection(
        endpoint="projects/p/branches/b/endpoints/e",
        host=None,
        port=5432,
        database="databricks_postgres",
        username=None,
        password=None,
        schema="dqx_studio",
    )


def test_platform_lakebase_values_are_normalized() -> None:
    config = AppConfig(_env_file=None, lakebase_endpoint="", lakebase_schema_name="dqx_studio")

    result = resolve_lakebase_connection(
        config,
        {
            "PGHOST": "db.example",
            "PGPORT": "5433",
            "PGDATABASE": "dqx",
            "PGUSER": "app",
            "PGPASSWORD": "secret",
        },
    )

    assert result == LakebaseConnection(
        endpoint=None,
        host="db.example",
        port=5433,
        database="dqx",
        username="app",
        password=SecretStr("secret"),
        schema="dqx_studio",
    )


def test_missing_lakebase_input_is_unresolved() -> None:
    config = AppConfig(_env_file=None, lakebase_endpoint="")

    assert resolve_lakebase_connection(config, {}) is None


def test_invalid_platform_port_is_rejected() -> None:
    config = AppConfig(_env_file=None, lakebase_endpoint="")

    with pytest.raises(InvalidParameterError):
        resolve_lakebase_connection(config, {"PGHOST": "db.example", "PGPORT": "not-a-port"})


def test_runtime_exposes_only_activated_resources() -> None:
    runtime = Runtime()
    with pytest.raises(RuntimeError, match="resources are not ready"):
        runtime.require_resources()

    resources = ActiveResources(
        volume=VolumeLocation("main", "dqx", "wheels", "/Volumes/main/dqx/wheels"),
        lakebase=LakebaseConnection(
            endpoint="projects/p/branches/b/endpoints/e",
            host=None,
            port=5432,
            database="databricks_postgres",
            username=None,
            password=None,
            schema="dqx_studio",
        ),
        warehouse_id="warehouse-id",
        job_id=None,
        tmp_schema="dqx_studio_tmp",
        genie_schema="genie",
    )

    runtime.activate(resources)

    assert runtime.require_resources() is resources

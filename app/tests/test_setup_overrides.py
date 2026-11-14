"""Tests for administrator setup overrides."""

import dataclasses
from datetime import datetime, timezone

import pytest

from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.setup.models import SetupStepId
from databricks_labs_dqx_app.backend.setup.overrides import SetupOverrides, override_fingerprint
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, LakebaseConnection, VolumeLocation


class MemorySettings:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.writers: dict[str, str | None] = {}

    def get_setting(self, key: str) -> str | None:
        return self.values.get(key)

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None:
        self.values[key] = value
        self.writers[key] = user_email


class BrokenSettings:
    def get_setting(self, key: str) -> str | None:
        raise RuntimeError("lakebase unavailable")

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None:
        raise RuntimeError("lakebase unavailable")


@pytest.fixture
def resources() -> ActiveResources:
    return ActiveResources(
        volume=VolumeLocation("main", "dqx_studio", "wheels", "/Volumes/main/dqx_studio/wheels"),
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
        genie_schema="dqx_studio_genie",
        demo_schema="dqx_studio_demo",
        audience=resolve_audience(["data-team"], "admins", allow_broad=False),
    )


def _overrides(settings: MemorySettings) -> SetupOverrides:
    return SetupOverrides(settings, clock=lambda: datetime(2026, 10, 9, tzinfo=timezone.utc))


def test_recorded_override_matches_its_fingerprint(resources: ActiveResources) -> None:
    settings = MemorySettings()
    overrides = _overrides(settings)
    fingerprint = override_fingerprint(SetupStepId.WAREHOUSE, resources, "warehouse-id")

    assert overrides.record(SetupStepId.WAREHOUSE, fingerprint, user_email="admin@example.com") is True

    assert overrides.is_overridden(SetupStepId.WAREHOUSE, fingerprint)
    assert not overrides.is_overridden(SetupStepId.ACCESS, fingerprint)
    assert "admin@example.com" in settings.writers.values()


def test_clear_removes_the_override(resources: ActiveResources) -> None:
    overrides = _overrides(MemorySettings())
    fingerprint = override_fingerprint(SetupStepId.ACCESS, resources)
    overrides.record(SetupStepId.ACCESS, fingerprint, user_email=None)

    overrides.clear(SetupStepId.ACCESS)

    assert not overrides.is_overridden(SetupStepId.ACCESS, fingerprint)


def test_clear_without_an_override_writes_nothing() -> None:
    settings = MemorySettings()

    _overrides(settings).clear(SetupStepId.ACCESS)

    assert settings.values == {}


def test_fingerprint_is_case_insensitive_for_principals(resources: ActiveResources) -> None:
    upper = dataclasses.replace(resources, audience=resolve_audience(["DATA-TEAM"], "admins", allow_broad=False))

    assert override_fingerprint(SetupStepId.ACCESS, resources) == override_fingerprint(SetupStepId.ACCESS, upper)


@pytest.mark.parametrize(
    "change",
    [
        lambda r: dataclasses.replace(r, volume=dataclasses.replace(r.volume, catalog="other")),
        lambda r: dataclasses.replace(r, volume=dataclasses.replace(r.volume, schema="other_prefix")),
        lambda r: dataclasses.replace(r, audience=resolve_audience(["other-team"], "admins", allow_broad=False)),
    ],
    ids=["catalog", "prefix", "audience"],
)
def test_override_stops_applying_when_configuration_changes(resources: ActiveResources, change) -> None:
    overrides = _overrides(MemorySettings())
    overrides.record(SetupStepId.ACCESS, override_fingerprint(SetupStepId.ACCESS, resources), user_email=None)

    assert not overrides.is_overridden(SetupStepId.ACCESS, override_fingerprint(SetupStepId.ACCESS, change(resources)))


def test_override_stops_applying_when_subject_changes(resources: ActiveResources) -> None:
    overrides = _overrides(MemorySettings())
    overrides.record(
        SetupStepId.WAREHOUSE, override_fingerprint(SetupStepId.WAREHOUSE, resources, "old"), user_email=None
    )

    assert not overrides.is_overridden(
        SetupStepId.WAREHOUSE, override_fingerprint(SetupStepId.WAREHOUSE, resources, "new")
    )


def test_corrupt_override_counts_as_absent(resources: ActiveResources) -> None:
    settings = MemorySettings()
    settings.values["setup_override_access"] = "not json"

    assert not _overrides(settings).is_overridden(
        SetupStepId.ACCESS, override_fingerprint(SetupStepId.ACCESS, resources)
    )


def test_store_failures_never_raise(resources: ActiveResources) -> None:
    overrides = SetupOverrides(BrokenSettings())
    fingerprint = override_fingerprint(SetupStepId.ACCESS, resources)

    assert overrides.record(SetupStepId.ACCESS, fingerprint, user_email=None) is False
    assert overrides.is_overridden(SetupStepId.ACCESS, fingerprint) is False
    overrides.clear(SetupStepId.ACCESS)

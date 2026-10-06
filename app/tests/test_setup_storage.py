"""Prefix-derived Unity Catalog storage contract."""

import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks_labs_dqx_app.backend.setup.storage import DEFAULT_PREFIX, derive_storage, validate_prefix


def test_default_prefix_derives_every_studio_location() -> None:
    storage = derive_storage("main", DEFAULT_PREFIX)

    assert storage.schemas == ("dqx_studio", "dqx_studio_tmp", "dqx_studio_genie", "dqx_studio_demo")
    assert storage.volume_location.path == "/Volumes/main/dqx_studio/wheels"
    assert storage.full_name(storage.tmp_schema) == "main.dqx_studio_tmp"


def test_explicit_schema_overrides_win_over_derived_names() -> None:
    storage = derive_storage("main", "studio", genie_schema="genie")

    assert storage.schema == "studio"
    assert storage.genie_schema == "genie"
    assert storage.tmp_schema == "studio_tmp"


@pytest.mark.parametrize("prefix", ["", "Studio", "1studio", "studio-x", "studio x", "a`b", "studio\n"])
def test_prefix_rejects_unsafe_or_ambiguous_values(prefix: str) -> None:
    with pytest.raises(InvalidParameterError):
        validate_prefix(prefix)


def test_prefix_rejects_overlong_derived_name() -> None:
    with pytest.raises(InvalidParameterError):
        derive_storage("main", "a" * 250)


def test_catalog_must_be_a_single_safe_identifier() -> None:
    with pytest.raises(InvalidParameterError):
        derive_storage("main`; DROP", DEFAULT_PREFIX)
    with pytest.raises(InvalidParameterError):
        derive_storage("", DEFAULT_PREFIX)


def test_dotted_catalog_is_rejected() -> None:
    with pytest.raises(InvalidParameterError):
        derive_storage("a.b", DEFAULT_PREFIX)


@pytest.mark.parametrize("override", ["schema", "tmp_schema", "genie_schema", "demo_schema"])
def test_dotted_schema_override_is_rejected(override: str) -> None:
    with pytest.raises(InvalidParameterError):
        derive_storage("main", DEFAULT_PREFIX, **{override: "x.y"})


def test_duplicate_schema_names_are_rejected() -> None:
    with pytest.raises(InvalidParameterError):
        derive_storage("main", "studio", tmp_schema="studio")

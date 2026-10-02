"""Tests for AppConfig fields and env-var overrides."""

import pytest
from pydantic import ValidationError


def test_audience_groups_are_explicit_and_scoped() -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    assert AppConfig(_env_file=None).user_groups == []
    assert AppConfig(_env_file=None, user_groups=["studio-authors", "studio-viewers"]).user_groups == [
        "studio-authors",
        "studio-viewers",
    ]


@pytest.mark.parametrize("group", ["account users", "users", "`account users`", "`UsErS`", " account users "])
def test_broad_audience_groups_are_rejected(group: str) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    with pytest.raises(ValidationError):
        AppConfig(_env_file=None, user_groups=[group])


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ('["studio-authors", "studio-viewers"]', ["studio-authors", "studio-viewers"]),
        ("studio-authors,studio-viewers", ["studio-authors", "studio-viewers"]),
        (" studio-authors , studio-viewers , studio-authors ", ["studio-authors", "studio-viewers"]),
        ("studio-authors", ["studio-authors"]),
        ('["studio,authors", "studio-viewers"]', ["studio,authors", "studio-viewers"]),
        ("[]", []),
    ],
)
def test_audience_groups_env_accepts_json_and_simple_csv(
    monkeypatch: pytest.MonkeyPatch, value: str, expected: list[str]
) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.setenv("DQX_USER_GROUPS", value)

    assert AppConfig(_env_file=None).user_groups == expected


@pytest.mark.parametrize(
    "value",
    [
        '["studio-authors"',
        '["studio-authors",]',
        '"studio-authors","studio-viewers"',
        "['studio-authors']",
        '{"group": "studio-authors"}',
        '"studio-authors"',
        "null",
        "true",
        "123",
        "[123]",
        "[null]",
        '[["studio-authors"]]',
        "",
        " ",
        "studio-authors,",
        ",studio-authors",
        "studio-authors,,studio-viewers",
        "studio-authors,users",
        "studio-authors, Account Users ",
        '["UsErS"]',
        "studio-authors,`studio-viewers`",
        "studio-authors,studio\nviewers",
        '["studio\\u0000viewers"]',
    ],
)
def test_invalid_audience_groups_env_has_actionable_validation(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.setenv("DQX_USER_GROUPS", value)

    with pytest.raises(ValidationError) as raised:
        AppConfig(_env_file=None)

    errors = raised.value.errors(include_input=False, include_context=False)
    assert all(error["loc"][0] == "DQX_USER_GROUPS" for error in errors)


def test_malformed_audience_json_explains_supported_formats(monkeypatch: pytest.MonkeyPatch) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.setenv("DQX_USER_GROUPS", '["studio-authors",]')

    with pytest.raises(ValidationError) as raised:
        AppConfig(_env_file=None)

    message = raised.value.errors(include_input=False, include_context=False)[0]["msg"]
    assert "DQX_USER_GROUPS" in message
    assert "JSON list" in message
    assert "comma-separated" in message
    assert "JSONDecodeError" not in message


def test_lakebase_pool_min_size_defaults_to_zero(monkeypatch):
    # Scale-to-zero: the pool must be allowed to drain to zero idle
    # connections so a suspended Lakebase endpoint isn't kept warm.
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.delenv("DQX_LAKEBASE_POOL_MIN_SIZE", raising=False)
    assert AppConfig(_env_file=None).lakebase_pool_min_size == 0


def test_lakebase_pool_min_size_env_override(monkeypatch):
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.setenv("DQX_LAKEBASE_POOL_MIN_SIZE", "2")
    assert AppConfig(_env_file=None).lakebase_pool_min_size == 2


def test_admin_group_defaults_to_workspace_admins(monkeypatch):
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.delenv("DQX_ADMIN_GROUP", raising=False)
    assert AppConfig(_env_file=None).admin_group == "admins"


def test_admin_group_rejects_whitespace_only_value() -> None:
    """A blank bootstrap group must fail clearly instead of locking every user out."""
    from databricks_labs_dqx_app.backend.config import AppConfig

    with pytest.raises(ValidationError, match="admin_group"):
        AppConfig(_env_file=None, admin_group="   ")


def test_bundle_resource_tagging_defaults_off(monkeypatch) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.delenv("DQX_TAG_BUNDLE_OWNED_RESOURCES", raising=False)
    assert AppConfig(_env_file=None).tag_bundle_owned_resources is False


def test_bundle_resource_tagging_accepts_dab_opt_in(monkeypatch) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.setenv("DQX_TAG_BUNDLE_OWNED_RESOURCES", "1")
    assert AppConfig(_env_file=None).tag_bundle_owned_resources is True


def test_sibling_schema_names_follow_bound_volume_schema(monkeypatch):
    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_TMP_SCHEMA", raising=False)
    # Re-import so pydantic-settings picks up the cleared env.
    import importlib

    import databricks_labs_dqx_app.backend.config as config_module

    importlib.reload(config_module)
    from databricks_labs_dqx_app.backend.config import AppConfig

    config = AppConfig(_env_file=None, wheels_volume="/Volumes/main/dqx/wheels")
    assert config.tmp_schema_name == "dqx_tmp"
    assert config.genie_schema_name == "dqx_genie"

    monkeypatch.setenv("DQX_GENIE_SCHEMA", "custom_genie")
    monkeypatch.setenv("DQX_TMP_SCHEMA", "custom_tmp")
    configured = AppConfig(_env_file=None, wheels_volume="/Volumes/main/dqx/wheels")
    assert configured.genie_schema_name == "custom_genie"
    assert configured.tmp_schema_name == "custom_tmp"


@pytest.mark.parametrize(
    "volume_path",
    ["/Volumes/main/../wheels", "/Volumes/main/bad\nschema/wheels", "/Volumes/main/studio/wheels/"],
)
def test_invalid_volume_does_not_determine_sibling_schema_names(
    monkeypatch: pytest.MonkeyPatch, volume_path: str
) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_TMP_SCHEMA", raising=False)
    config = AppConfig(_env_file=None, schema_name="configured", wheels_volume=volume_path)

    assert config.tmp_schema_name == "configured_tmp"
    assert config.genie_schema_name == "configured_genie"


def test_explicit_schema_mismatch_warns_without_exposing_identifiers(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.setenv("DQX_SCHEMA", "configured_schema")
    monkeypatch.delenv("DQX_TMP_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)
    config = AppConfig(_env_file=None, wheels_volume="/Volumes/main/bound_schema/wheels")

    assert config.tmp_schema_name == "bound_schema_tmp"
    assert "DQX_SCHEMA differs from the bound volume schema" in caplog.text
    assert "configured_schema" not in caplog.text
    assert "bound_schema" not in caplog.text


@pytest.mark.parametrize("schema", [None, "bound_schema"])
def test_default_or_matching_schema_does_not_warn(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, schema: str | None
) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.delenv("DQX_SCHEMA", raising=False)
    if schema is not None:
        monkeypatch.setenv("DQX_SCHEMA", schema)
    AppConfig(_env_file=None, wheels_volume="/Volumes/main/bound_schema/wheels")

    assert "DQX_SCHEMA differs" not in caplog.text

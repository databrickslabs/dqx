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



def test_users_is_accepted_as_deployment_broad_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.setenv("DQX_USER_GROUPS", '["users"]')

    assert AppConfig().user_groups == ["users"]


def test_deployment_storage_requires_explicit_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.delenv("DQX_CATALOG", raising=False)
    assert AppConfig().has_deployment_storage is False

    monkeypatch.setenv("DQX_CATALOG", "main")
    monkeypatch.setenv("DQX_PREFIX", "studio")
    config = AppConfig()
    assert config.has_deployment_storage is True
    assert config.prefix == "studio"


def test_no_volume_setting_is_required_at_startup() -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    assert not any("volume" in name for name in AppConfig.model_fields)
    assert "VOLUME" not in (AppConfig.model_fields["require_task_runner"].description or "").upper()

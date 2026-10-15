"""Permission contracts for the bundle-managed Studio deployment."""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_BUNDLE = Path(__file__).resolve().parents[1] / "databricks.yml"
_APP_SP = "${resources.apps.dqx-studio.service_principal_client_id}"
_RUNNER = "${var.dqx_service_principal_application_id}"
_UC_AUDIENCE = "${var.studio_uc_principal}"
_APP_SCHEMA_PRIVILEGES = {
    "USE_SCHEMA",
    "CREATE_TABLE",
    "CREATE_FUNCTION",
    "CREATE_VOLUME",
    "SELECT",
    "MODIFY",
    "EXECUTE",
    "READ_VOLUME",
    "WRITE_VOLUME",
    "APPLY_TAG",
    "MANAGE",
}


@pytest.fixture(scope="module")
def bundle() -> dict:
    return yaml.safe_load(_BUNDLE.read_text(encoding="utf-8"))


def _grants(bundle: dict, kind: str, key: str) -> dict[str, list[str]]:
    return {grant["principal"]: grant["privileges"] for grant in bundle["resources"][kind][key]["grants"]}


def test_storage_names_derive_from_prefix(bundle: dict) -> None:
    variables = bundle["variables"]
    assert variables["prefix"]["default"] == "dqx_studio"
    assert variables["schema_name"]["default"] == "${var.prefix}"
    assert variables["tmp_schema_name"]["default"] == "${var.prefix}_tmp"
    assert variables["genie_schema_name"]["default"] == "${var.prefix}_genie"
    assert variables["demo_schema_name"]["default"] == "${var.prefix}_demo"
    assert "wheels_volume_name" not in variables
    assert bundle["resources"]["schemas"]["demo_schema"]["name"] == "${var.demo_schema_name}"
    assert bundle["resources"]["volumes"]["wheels"]["name"] == "wheels"


def test_runner_has_least_privilege(bundle: dict) -> None:
    assert _grants(bundle, "schemas", "main_schema")[_RUNNER] == ["USE_SCHEMA", "SELECT", "MODIFY"]
    assert _grants(bundle, "schemas", "tmp_schema")[_RUNNER] == ["USE_SCHEMA", "SELECT"]
    assert _RUNNER not in _grants(bundle, "schemas", "genie_schema")
    assert _RUNNER not in _grants(bundle, "schemas", "demo_schema")
    assert _grants(bundle, "volumes", "wheels")[_RUNNER] == ["READ_VOLUME"]


@pytest.mark.parametrize("schema", ["main_schema", "tmp_schema", "genie_schema", "demo_schema"])
def test_app_can_manage_studio_schemas(bundle: dict, schema: str) -> None:
    privileges = _grants(bundle, "schemas", schema)[_APP_SP]
    # The bundle engine silently drops MANAGE when it is combined with ALL_PRIVILEGES.
    assert "ALL_PRIVILEGES" not in privileges
    assert set(privileges) == _APP_SCHEMA_PRIVILEGES
    assert "MANAGE" in privileges


def test_audience_never_gets_genie_select_or_main_access(bundle: dict) -> None:
    assert "SELECT" not in _grants(bundle, "schemas", "genie_schema")[_UC_AUDIENCE]
    assert _UC_AUDIENCE not in _grants(bundle, "schemas", "main_schema")


def test_uc_grants_never_name_workspace_groups(bundle: dict) -> None:
    for schema in bundle["resources"]["schemas"].values():
        principals = {grant["principal"] for grant in schema["grants"]}
        assert "${var.studio_user_group}" not in principals
        assert "${var.admin_group}" not in principals
        assert "users" not in principals
        assert "account users" not in principals


def test_app_sp_manages_warehouse_and_admins_can_use(bundle: dict) -> None:
    permissions = next(iter(bundle["resources"]["sql_warehouses"].values()))["permissions"]
    assert {"level": "CAN_MANAGE", "service_principal_name": _APP_SP} in permissions
    assert {"level": "CAN_USE", "group_name": "${var.admin_group}"} in permissions
    assert {"level": "CAN_USE", "group_name": "${var.studio_user_group}"} in permissions


def test_app_is_shared_with_audience_and_admins(bundle: dict) -> None:
    permissions = bundle["resources"]["apps"]["dqx-studio"]["permissions"]
    groups = {item.get("group_name"): item["level"] for item in permissions}
    assert groups["${var.studio_user_group}"] == "CAN_USE"
    assert groups["${var.admin_group}"] == "CAN_USE"
    assert "genie" in bundle["resources"]["apps"]["dqx-studio"]["user_api_scopes"]


def test_dashboard_is_readable_by_audience_and_admins(bundle: dict) -> None:
    permissions = next(iter(bundle["resources"]["dashboards"].values()))["permissions"]
    assert {"level": "CAN_READ", "group_name": "${var.studio_user_group}"} in permissions
    assert {"level": "CAN_READ", "group_name": "${var.admin_group}"} in permissions


def test_app_sp_can_manage_bundle_dashboard(bundle: dict) -> None:
    permissions = next(iter(bundle["resources"]["dashboards"].values()))["permissions"]
    assert {"level": "CAN_MANAGE", "service_principal_name": _APP_SP} in permissions


def test_app_warehouse_binding_grants_can_manage(bundle: dict) -> None:
    resources = bundle["resources"]["apps"]["dqx-studio"]["resources"]
    warehouse = next(item for item in resources if "sql_warehouse" in item)
    assert warehouse["sql_warehouse"]["permission"] == "CAN_MANAGE"


def test_app_env_has_no_volume_binding(bundle: dict) -> None:
    env = {item["name"] for item in bundle["variables"]["app_config"]["default"]["env"]}
    assert "DQX_WHEELS_VOLUME" not in env
    assert {"DQX_CATALOG", "DQX_PREFIX", "DQX_DEMO_SCHEMA", "DQX_USER_GROUPS"} <= env


def _deploy_command(*make_args: str) -> str:
    repo_root = _BUNDLE.parents[1]
    result = subprocess.run(
        ["make", "-n", "app-deploy", "PROFILE=x", "TARGET=dev", *make_args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    return next(line for line in result.stdout.splitlines() if "bundle deploy" in line)


@pytest.mark.skipif(shutil.which("make") is None, reason="make is not installed")
class TestMakeDeployVariables:
    def test_broad_mode_sets_account_users_uc_principal(self) -> None:
        command = _deploy_command("STUDIO_USER_GROUP=users")
        assert "--var studio_user_group=users" in command
        assert '--var "studio_uc_principal=account users"' in command

    def test_explicit_scoped_group_never_widens_uc_grants(self) -> None:
        command = _deploy_command("STUDIO_USER_GROUP=users", "BUNDLE_VARS=--var=studio_user_group=data-team")
        assert "studio_uc_principal" not in command
        assert "--var=studio_user_group=data-team" in command

    @pytest.mark.parametrize("group", ["Users", "USERS", "uSeRs"])
    def test_non_lowercase_broad_keyword_fails_clearly(self, group: str) -> None:
        result = subprocess.run(
            ["make", "-n", "app-deploy", "PROFILE=x", "TARGET=dev", f"STUDIO_USER_GROUP={group}"],
            cwd=_BUNDLE.parents[1],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode != 0
        assert "STUDIO_USER_GROUP=users" in result.stderr
        assert "bundle deploy" not in result.stdout

    def test_no_inputs_adds_no_studio_variables(self) -> None:
        command = _deploy_command()
        assert "studio_" not in command
        assert "prefix" not in command

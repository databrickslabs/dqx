"""Contract tests for DQX Studio bundle resource tags."""

from pathlib import Path

import yaml

_BUNDLE = Path(__file__).resolve().parents[1] / "databricks.yml"


def _resources() -> dict[str, object]:
    document = yaml.safe_load(_BUNDLE.read_text(encoding="utf-8"))
    return document["resources"]


def test_bundle_created_resources_have_dqx_studio_tag() -> None:
    """Bundle-managed compute and storage resources carry the Studio tag."""
    resources = _resources()

    assert resources["jobs"]["dqx_task_runner"]["tags"]["app"] == "dqx-studio"
    assert resources["sql_warehouses"]["dqx_sql_warehouse"]["tags"] == {
        "custom_tags": [{"key": "app", "value": "dqx-studio"}]
    }
    assert resources["postgres_projects"]["dqx_studio"]["custom_tags"] == [{"key": "app", "value": "dqx-studio"}]


def test_workspace_entities_do_not_use_unsupported_dab_tag_fields() -> None:
    """Apps and dashboards omit tag fields unsupported by their DAB resources."""
    resources = _resources()

    assert "tags" not in resources["apps"]["dqx-studio"]
    assert "tags" not in resources["dashboards"]["dqx_quality_overview"]

"""Security contracts for bundle-managed Genie access."""

from pathlib import Path

import yaml


def test_bundle_does_not_grant_users_access_to_every_genie_object() -> None:
    """Future Genie objects must not become user-readable through a schema grant."""
    bundle = Path(__file__).resolve().parents[1] / "databricks.yml"
    document = yaml.safe_load(bundle.read_text(encoding="utf-8"))
    grants = document["resources"]["schemas"]["genie_schema"]["grants"]

    user_grants = [grant for grant in grants if grant["principal"] == "account users"]
    assert user_grants
    assert all(
        "SELECT" not in grant["privileges"] and "ALL_PRIVILEGES" not in grant["privileges"] for grant in user_grants
    )

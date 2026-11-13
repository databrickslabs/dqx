"""Tests for regenerating the Marketplace lock (scripts/lock_marketplace.py)."""

import tomllib
from pathlib import Path

import pytest

from scripts.lock_marketplace import normalize_lock_urls, set_bundle_dqx_version

PROXY_LOCK = """version = 1

[[package]]
name = "certifi"
version = "2026.10.19"
source = { registry = "https://proxy.example/simple" }
sdist = { url = "https://proxy.example/packages/aa/bb/cc/certifi-2026.10.19.tar.gz", hash = "sha256:00", size = 12 }

[[package]]
name = "databricks-labs-dqx-app"
version = "0.1.3"
source = { editable = "." }
"""


def test_normalize_lock_urls_points_everything_at_public_pypi() -> None:
    normalized = normalize_lock_urls(PROXY_LOCK)

    certifi = next(p for p in tomllib.loads(normalized)["package"] if p["name"] == "certifi")
    assert certifi["source"] == {"registry": "https://pypi.org/simple"}
    assert certifi["sdist"] == {
        "url": "https://files.pythonhosted.org/packages/aa/bb/cc/certifi-2026.10.19.tar.gz",
        "hash": "sha256:00",
    }
    assert "proxy.example" not in normalized


def test_normalize_lock_urls_leaves_editable_sources_alone() -> None:
    assert 'source = { editable = "." }' in normalize_lock_urls(PROXY_LOCK)


def test_set_bundle_dqx_version_updates_only_the_default(tmp_path: Path) -> None:
    bundle = tmp_path / "databricks.yml"
    bundle.write_text('variables:\n  dqx_version:\n    default: "0.17.0"\n  other:\n    default: "0.17.0"\n')

    set_bundle_dqx_version("0.17.2", bundle)

    assert bundle.read_text() == 'variables:\n  dqx_version:\n    default: "0.17.2"\n  other:\n    default: "0.17.0"\n'


def test_set_bundle_dqx_version_fails_when_the_declaration_changed(tmp_path: Path) -> None:
    bundle = tmp_path / "databricks.yml"
    bundle.write_text("variables:\n  dqx_version: 0.17.0\n")

    with pytest.raises(RuntimeError, match="declaration changed"):
        set_bundle_dqx_version("0.17.2", bundle)

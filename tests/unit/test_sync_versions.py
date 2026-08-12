"""Unit tests for the release version-sync script (``docs/dqx/sync_versions.py``).

The script propagates the DQX version from ``__about__.py`` to the places that must track it on every
release: versioned GitHub URLs in the docs, the literal ``databricks-labs-dqx==<version>`` pins in the
Studio app, and — the delicate part — the single ``dqx_version`` bundle variable in the MCP server's
``databricks.yml`` that the pin and the in-repo wheel filename both DERIVE from via
``${var.dqx_version}``. A regression here silently ships a stale version to a deploy, so the behaviour
is pinned down explicitly.

The module lives under ``docs/dqx`` and is not importable via the project's pythonpath, so it is loaded
by file path. ``update_dqx_pins`` resolves its targets relative to the CWD, so each test runs against a
throwaway repo layout under ``tmp_path`` rather than the real tree.
"""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "docs" / "dqx" / "sync_versions.py"


def _load_module():
    """Load sync_versions.py by path (it is not on the pythonpath)."""
    spec = importlib.util.spec_from_file_location("sync_versions", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sync_versions = _load_module()


# A minimal MCP-style databricks.yml: the derived wheel filename + pin, exactly as the real bundle
# expresses them, plus a literal ``dqx_version`` default (the single value a release edits).
_MCP_BUNDLE = """\
variables:
  dqx_wheel_filename:
    default: "databricks_labs_dqx-${var.dqx_version}-py3-none-any.whl"
  dqx_version:
    default: "0.15.0"
  runner_environment_dependencies:
    default:
      - "databricks-labs-dqx[datacontract]==${var.dqx_version}"
"""

# The Studio app pins DQX with a LITERAL version (it does not derive from a bundle variable).
_APP_PYPROJECT = """\
[project]
dependencies = [
    "databricks-labs-dqx[llm,datacontract]==0.15.0",
]
"""


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A throwaway repo layout with the files update_dqx_pins targets, CWD pointed at it."""
    (tmp_path / "app").mkdir()
    (tmp_path / "mcp-server").mkdir()
    (tmp_path / "app" / "pyproject.toml").write_text(_APP_PYPROJECT)
    (tmp_path / "mcp-server" / "databricks.yml").write_text(_MCP_BUNDLE)
    monkeypatch.chdir(tmp_path)
    return tmp_path


class TestGetDqxVersion:
    def test_reads_the_version_literal(self, tmp_path):
        about = tmp_path / "__about__.py"
        about.write_text('__version__ = "1.2.3"\n')
        assert sync_versions.get_dqx_version(about) == "1.2.3"

    def test_raises_when_absent(self, tmp_path):
        about = tmp_path / "__about__.py"
        about.write_text("# no version here\n")
        with pytest.raises(ValueError):
            sync_versions.get_dqx_version(about)


class TestUpdateDqxPins:
    def test_bumps_the_single_dqx_version_default_in_the_mcp_bundle(self, repo):
        sync_versions.update_dqx_pins("0.16.0")
        bundle = (repo / "mcp-server" / "databricks.yml").read_text()
        assert 'default: "0.16.0"' in bundle

    def test_leaves_derived_references_untouched(self, repo):
        """The wheel filename and the pin derive from ${var.dqx_version}; bumping must not touch them."""
        sync_versions.update_dqx_pins("0.16.0")
        bundle = (repo / "mcp-server" / "databricks.yml").read_text()
        # Derivation is preserved verbatim — no literal version leaks into either.
        assert "databricks_labs_dqx-${var.dqx_version}-py3-none-any.whl" in bundle
        assert "databricks-labs-dqx[datacontract]==${var.dqx_version}" in bundle
        # And crucially no stray literal version anywhere in the derived forms.
        assert "0.16.0-py3-none-any.whl" not in bundle
        assert "databricks-labs-dqx[datacontract]==0.16.0" not in bundle

    def test_leaves_the_app_pyproject_dqx_pin_untouched(self, repo):
        """The Studio app installs DQX from the registry through its frozen uv.lock, so its pin is
        bumped at release-publish time — never here, where a not-yet-published version would break CI."""
        sync_versions.update_dqx_pins("0.16.0")
        pyproject = (repo / "app" / "pyproject.toml").read_text()
        assert "databricks-labs-dqx[llm,datacontract]==0.15.0" in pyproject
        assert "==0.16.0" not in pyproject

    def test_is_idempotent_at_the_current_version(self, repo):
        """Re-running with an unchanged version rewrites nothing (the make-fmt promise)."""
        before_bundle = (repo / "mcp-server" / "databricks.yml").read_text()
        before_app = (repo / "app" / "pyproject.toml").read_text()
        sync_versions.update_dqx_pins("0.15.0")
        assert (repo / "mcp-server" / "databricks.yml").read_text() == before_bundle
        assert (repo / "app" / "pyproject.toml").read_text() == before_app

    def test_a_bump_leaves_a_single_source_of_the_version(self, repo):
        """After a bump, the only literal version in the MCP bundle is the dqx_version default itself.

        This is the whole point of deriving the pin and wheel name: exactly one value carries the
        version, so the pin, the wheel filename, and the telemetry token can never disagree.
        """
        sync_versions.update_dqx_pins("0.16.0")
        bundle = (repo / "mcp-server" / "databricks.yml").read_text()
        # The new version appears once — as the dqx_version default. The derived forms carry the
        # ${var.dqx_version} placeholder, never the digits.
        assert bundle.count("0.16.0") == 1
        assert 'default: "0.16.0"' in bundle
        # And no trace of the old version survives anywhere. This is what catches a regression where a
        # literal (e.g. a hard-coded wheel name) was left behind un-bumped: the count check alone would
        # still pass (the stale 0.15.0 is a *different* string), but this makes the intent robust.
        assert "0.15.0" not in bundle

    def test_skips_missing_files(self, tmp_path, monkeypatch):
        """A layout without the app/ files must not raise — only present files are rewritten."""
        (tmp_path / "mcp-server").mkdir()
        (tmp_path / "mcp-server" / "databricks.yml").write_text(_MCP_BUNDLE)
        monkeypatch.chdir(tmp_path)
        sync_versions.update_dqx_pins("0.16.0")  # no app/ dir — must be a no-op there, not an error
        assert 'default: "0.16.0"' in (tmp_path / "mcp-server" / "databricks.yml").read_text()


class TestUpdateMdxFiles:
    def test_rewrites_versioned_github_urls(self, tmp_path):
        mdx = tmp_path / "page.mdx"
        mdx.write_text("See https://github.com/databrickslabs/dqx/blob/main/src/foo.py for details.\n")
        sync_versions.update_mdx_files(tmp_path, "0.16.0")
        assert "blob/v0.16.0/src/foo.py" in mdx.read_text()

    def test_rewrites_an_already_versioned_url(self, tmp_path):
        mdx = tmp_path / "page.mdx"
        mdx.write_text("https://github.com/databrickslabs/dqx/blob/v0.15.0/src/foo.py\n")
        sync_versions.update_mdx_files(tmp_path, "0.16.0")
        assert "blob/v0.16.0/src/foo.py" in mdx.read_text()

    def test_leaves_unrelated_urls_alone(self, tmp_path):
        mdx = tmp_path / "page.mdx"
        original = "https://github.com/databrickslabs/other/blob/main/x.py\n"
        mdx.write_text(original)
        sync_versions.update_mdx_files(tmp_path, "0.16.0")
        assert mdx.read_text() == original

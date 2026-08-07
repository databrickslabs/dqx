"""Tests for the app's startup work: publishing the runner wheel to the wheels volume."""

import re
import tomllib
from pathlib import Path
from unittest.mock import MagicMock, create_autospec, patch

from databricks.sdk import WorkspaceClient

_MCP_SERVER = Path(__file__).resolve().parents[1]


def _bundle_text() -> str:
    """The bundle as text. Deliberately not parsed with a YAML library: `make mcp-test` installs
    only pytest into the server's runtime env, and these assertions are simple enough to grep."""
    return (_MCP_SERVER / "databricks.yml").read_text()


class TestRunnerWheelVersionPin:
    """The wheel filename is duplicated in the bundle and the runner package; keep them in step.

    The runner job installs the wheel by ABSOLUTE path (…/dqx_artifacts/<filename>), so a version
    bump in runner/pyproject.toml without the matching bundle change means every data tool fails on
    library install. This test is the guard; the app also warns at startup if they diverge.
    """

    def test_bundle_pin_matches_runner_version(self):
        version = tomllib.loads((_MCP_SERVER / "runner" / "pyproject.toml").read_text())["project"]["version"]
        match = re.search(r"runner_wheel_filename:\s*\n\s*default:\s*\"([^\"]+)\"", _bundle_text())
        assert match, "runner_wheel_filename variable not found in databricks.yml"
        pinned = match.group(1)

        assert pinned == f"dqx_mcp_runner-{version}-py3-none-any.whl", (
            f"runner/pyproject.toml is version {version} but the bundle pins {pinned!r}. "
            "Bump the runner_wheel_filename bundle variable to match."
        )

    def test_bundle_pins_the_repo_dqx_version(self):
        """The runner job's DQX pin must track this repo's version.

        The job installs an exact `databricks-labs-dqx[...]==<version>` from PyPI (mirroring the DQX
        Studio task runner) rather than building the parent wheel. Nothing else couples that pin to
        src/databricks/labs/dqx/__about__.py, so without this guard a DQX release silently leaves the
        MCP runner on the previous version — the tools keep working, against stale library code.
        """
        about = (_MCP_SERVER.parent / "src" / "databricks" / "labs" / "dqx" / "__about__.py").read_text()
        repo_version = re.search(r'__version__\s*=\s*"([^"]+)"', about)
        assert repo_version, "could not read __version__ from src/databricks/labs/dqx/__about__.py"

        pins = re.findall(r"databricks-labs-dqx(?:\[[^\]]*\])?==([0-9][^\s\"']*)", _bundle_text())
        assert pins, "no pinned databricks-labs-dqx dependency found in databricks.yml"
        assert set(pins) == {repo_version.group(1)}, (
            f"the repo is DQX {repo_version.group(1)} but databricks.yml pins {sorted(set(pins))}. "
            "Bump the runner job's databricks-labs-dqx pin to match (both targets, if overridden)."
        )

    def test_runner_job_installs_the_pinned_filename(self):
        """The job must install the wheel via the pinned var, not a relative glob."""
        text = _bundle_text()
        assert "${var.runner_wheel_filename}" in text
        # A relative "./.build/...whl" dependency would need workspace.artifact_path, which this
        # bundle deliberately does not set (the app publishes the wheel to a volume instead).
        assert "./.build/dqx_mcp_runner" not in text, "runner dep still uses a relative artifact path"
        assert "artifact_path:" not in text, "workspace.artifact_path must stay unset"


class TestFindRunnerWheel:
    def test_finds_wheel_in_build_dir(self, tmp_path, monkeypatch):
        from server.bootstrap import find_runner_wheel

        build = tmp_path / ".build"
        build.mkdir()
        wheel = build / "dqx_mcp_runner-0.1.0-py3-none-any.whl"
        wheel.write_bytes(b"wheel")
        monkeypatch.chdir(tmp_path)

        assert find_runner_wheel() == wheel

    def test_prefers_the_wheel_the_runner_job_installs(self, tmp_path, monkeypatch):
        """With several wheels present, the pinned filename wins — not a name sort.

        The runner job installs DQX_RUNNER_WHEEL_FILENAME by absolute path, so publishing any other
        wheel guarantees a library-install failure. Uses 0.9.0 vs 0.10.0 deliberately: a name sort
        picks 0.9.0 (strings, not versions), so this fails if selection reverts to sorting.
        """
        from server.bootstrap import find_runner_wheel

        build = tmp_path / ".build"
        build.mkdir()
        (build / "dqx_mcp_runner-0.9.0-py3-none-any.whl").write_bytes(b"sorts-last")
        pinned = build / "dqx_mcp_runner-0.10.0-py3-none-any.whl"
        pinned.write_bytes(b"pinned")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DQX_RUNNER_WHEEL_FILENAME", "dqx_mcp_runner-0.10.0-py3-none-any.whl")

        assert find_runner_wheel() == pinned

    def test_publishes_nothing_when_several_wheels_and_none_is_pinned(self, tmp_path, monkeypatch):
        """An ambiguous .build/ is reported, not resolved by guesswork.

        Publishing the wrong wheel fails every data tool at library-install time, and name order is
        not version order — so there is no safe guess. Returning None leaves the previously published
        wheel in place (the volume is not overwritten) and logs what to fix.
        """
        from server.bootstrap import find_runner_wheel

        build = tmp_path / ".build"
        build.mkdir()
        (build / "dqx_mcp_runner-0.1.0-py3-none-any.whl").write_bytes(b"a")
        (build / "dqx_mcp_runner-0.2.0-py3-none-any.whl").write_bytes(b"b")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DQX_RUNNER_WHEEL_FILENAME", "dqx_mcp_runner-9.9.9-py3-none-any.whl")

        assert find_runner_wheel() is None

    def test_single_wheel_is_used_even_if_the_pin_disagrees(self, tmp_path, monkeypatch):
        """One wheel and a mismatched pin still publishes: the drift warning covers that case.

        publish_runner_wheel logs the name mismatch loudly; refusing here would leave the volume
        empty on a first deploy whose pin is merely stale, which is a worse failure than publishing
        the only wheel that was built.
        """
        from server.bootstrap import find_runner_wheel

        build = tmp_path / ".build"
        build.mkdir()
        only = build / "dqx_mcp_runner-0.3.0-py3-none-any.whl"
        only.write_bytes(b"only")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DQX_RUNNER_WHEEL_FILENAME", "dqx_mcp_runner-0.1.0-py3-none-any.whl")

        assert find_runner_wheel() == only

    def test_returns_none_when_absent(self, tmp_path, monkeypatch):
        """No wheel anywhere on the search path ⇒ None (and a warning), not an exception.

        The walk-up fallback would otherwise find the real repo's own .build/ wheel, so point the
        module's __file__ at an isolated tree to exercise the genuinely-missing case.
        """
        import server.bootstrap as bootstrap

        (tmp_path / "requirements.txt").write_text("")  # anchors the walk-up inside tmp_path
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(bootstrap, "__file__", str(tmp_path / "server" / "bootstrap.py"))

        assert bootstrap.find_runner_wheel() is None


class TestPublishRunnerWheel:
    @staticmethod
    def _wheel(tmp_path: Path, name: str = "dqx_mcp_runner-0.1.0-py3-none-any.whl") -> Path:
        build = tmp_path / ".build"
        build.mkdir(exist_ok=True)
        wheel = build / name
        wheel.write_bytes(b"wheel-contents")
        return wheel

    def test_noop_without_volume_configured(self, tmp_path, monkeypatch):
        """No DQX_WHEELS_VOLUME (any non-coverage local run) must not touch the workspace."""
        from server.bootstrap import publish_runner_wheel

        self._wheel(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("DQX_WHEELS_VOLUME", raising=False)

        assert publish_runner_wheel() is None

    def test_uploads_wheel_and_hash_marker(self, tmp_path, monkeypatch):
        from server.bootstrap import publish_runner_wheel

        wheel = self._wheel(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DQX_WHEELS_VOLUME", "/Volumes/cat/dqx_mcp_tmp/dqx_artifacts")
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.files.download.side_effect = RuntimeError("no marker yet")

        with patch("databricks.sdk.WorkspaceClient", return_value=ws):
            published = publish_runner_wheel()

        assert published == f"/Volumes/cat/dqx_mcp_tmp/dqx_artifacts/{wheel.name}"
        uploaded = [c.args[0] for c in ws.files.upload.call_args_list]
        assert uploaded == [published, "/Volumes/cat/dqx_mcp_tmp/dqx_artifacts/.wheels_hash"]

    def test_skips_upload_when_hash_unchanged(self, tmp_path, monkeypatch):
        """Restarting the app must not re-upload an identical wheel."""
        from server.bootstrap import compute_wheel_hash, publish_runner_wheel

        wheel = self._wheel(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DQX_WHEELS_VOLUME", "/Volumes/cat/s/dqx_artifacts")
        ws = create_autospec(WorkspaceClient, instance=True)
        marker = MagicMock()
        marker.contents.read.return_value = compute_wheel_hash(wheel).encode()
        ws.files.download.return_value = marker

        with patch("databricks.sdk.WorkspaceClient", return_value=ws):
            published = publish_runner_wheel()

        assert published == f"/Volumes/cat/s/dqx_artifacts/{wheel.name}"
        ws.files.upload.assert_not_called()

    def test_upload_failure_is_swallowed(self, tmp_path, monkeypatch):
        """A publish failure must never stop the app from serving."""
        from server.bootstrap import publish_runner_wheel

        self._wheel(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DQX_WHEELS_VOLUME", "/Volumes/cat/s/dqx_artifacts")
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.files.download.side_effect = RuntimeError("no marker")
        ws.files.upload.side_effect = PermissionError("USE CATALOG denied")

        with patch("databricks.sdk.WorkspaceClient", return_value=ws):
            assert publish_runner_wheel() is None

    def test_warns_on_version_drift(self, tmp_path, monkeypatch, caplog):
        """A built wheel whose name differs from the job's pinned filename must be called out."""
        from server.bootstrap import publish_runner_wheel

        self._wheel(tmp_path, "dqx_mcp_runner-9.9.9-py3-none-any.whl")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DQX_WHEELS_VOLUME", "/Volumes/cat/s/dqx_artifacts")
        monkeypatch.setenv("DQX_RUNNER_WHEEL_FILENAME", "dqx_mcp_runner-0.1.0-py3-none-any.whl")
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.files.download.side_effect = RuntimeError("no marker")

        with patch("databricks.sdk.WorkspaceClient", return_value=ws), caplog.at_level("WARNING"):
            publish_runner_wheel()

        assert "version drift" in caplog.text

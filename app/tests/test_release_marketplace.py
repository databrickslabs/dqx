"""Tests for locally signed Marketplace release branches."""

import os
from pathlib import Path
import subprocess

import pytest

from scripts.release_marketplace import CommandResult, release_branch_name, release_marketplace


class RecordingCommandRunner:
    """Record release commands while simulating their filesystem effects."""

    def __init__(
        self,
        *,
        project_version: str = "0.1.0",
        fail_on: tuple[str, ...] | None = None,
        existing_branch: bool = False,
    ) -> None:
        self.project_version = project_version
        self.fail_on = fail_on
        self.existing_branch = existing_branch
        self.commands: list[tuple[str, ...]] = []
        self.created_worktree: Path | None = None

    def run(self, command: tuple[str, ...], *, cwd: Path, check: bool = True) -> CommandResult:
        self.commands.append(command)
        if command == ("git", "rev-parse", "--show-toplevel"):
            return CommandResult(returncode=0, stdout=f"{cwd}\n")
        if command[:2] == ("git", "verify-tag") and self.fail_on == command[:2]:
            if check:
                raise RuntimeError("command failed")
            return CommandResult(returncode=1)
        if command[:2] == ("git", "show"):
            return CommandResult(
                returncode=0,
                stdout=f'[project]\nname = "databricks-labs-dqx-app"\nversion = "{self.project_version}"\n',
            )
        if command[:4] == ("git", "show-ref", "--verify", "--quiet"):
            return CommandResult(returncode=0 if self.existing_branch else 1)
        if command[:3] == ("git", "worktree", "add"):
            self.created_worktree = Path(command[-2])
            self.created_worktree.mkdir(parents=True)
            return CommandResult(returncode=0)
        if command[:3] == ("git", "worktree", "remove"):
            self.created_worktree = self.created_worktree or Path(command[-1])
            self.created_worktree.rmdir()
            return CommandResult(returncode=0)
        if command == ("git", "diff", "--cached", "--quiet"):
            return CommandResult(returncode=1)
        return CommandResult(returncode=0)

    def contains(self, command: tuple[str, ...]) -> bool:
        return command in self.commands

    def contains_prefix(self, prefix: tuple[str, ...]) -> bool:
        return any(command[: len(prefix)] == prefix for command in self.commands)


def test_release_branch_name_is_derived_from_version_tag() -> None:
    assert release_branch_name("studio-v0.1.0") == "dqx-studio/marketplace/v0.1.0"


@pytest.mark.parametrize(
    "tag",
    ["0.1.0", "v0.1.0", "studio-0.1.0", "studio-v0.1", "studio-vnext", "studio-v0.1.0/extra"],
)
def test_release_rejects_invalid_tag_names(tag: str) -> None:
    with pytest.raises(ValueError, match="studio-vX.Y.Z"):
        release_branch_name(tag)


def test_release_verifies_tag_before_creating_branch(tmp_path: Path) -> None:
    commands = RecordingCommandRunner(fail_on=("git", "verify-tag"))
    with pytest.raises(RuntimeError, match="signed tag"):
        release_marketplace("studio-v0.1.0", tmp_path, commands)
    assert not commands.contains_prefix(("git", "worktree", "add"))


def test_release_creates_and_verifies_signed_commit_without_push(tmp_path: Path) -> None:
    commands = RecordingCommandRunner(project_version="0.1.0")
    branch = release_marketplace("studio-v0.1.0", tmp_path, commands)
    assert branch == "dqx-studio/marketplace/v0.1.0"
    assert commands.contains(("git", "show", "studio-v0.1.0:app/pyproject.toml"))
    assert commands.contains(("make", "app-install"))
    assert commands.contains(("uv", "run", "--frozen", "python", "app/scripts/build_app.py"))
    assert commands.contains(("uv", "run", "--frozen", "python", "app/scripts/build_marketplace.py"))
    assert commands.contains(
        (
            "uv",
            "run",
            "--frozen",
            "--group",
            "test",
            "pytest",
            "tests/test_build_marketplace.py",
            "tests/test_release_marketplace.py",
            "-v",
        )
    )
    assert commands.commands.index(("make", "app-install")) < commands.commands.index(
        ("uv", "run", "--frozen", "python", "app/scripts/build_app.py")
    )
    assert commands.contains(("git", "add", "-f", "-A", "app/marketplace"))
    assert commands.contains_prefix(("git", "commit", "-S"))
    assert commands.contains(("git", "verify-commit", "HEAD"))
    assert not commands.contains_prefix(("git", "push"))
    assert commands.created_worktree is not None
    assert not commands.created_worktree.exists()


def test_release_refuses_existing_local_branch(tmp_path: Path) -> None:
    commands = RecordingCommandRunner(project_version="0.1.0", existing_branch=True)
    with pytest.raises(RuntimeError, match="already exists"):
        release_marketplace("studio-v0.1.0", tmp_path, commands)
    assert not commands.contains_prefix(("git", "worktree", "add"))


def test_release_rejects_tag_version_that_differs_from_app(tmp_path: Path) -> None:
    commands = RecordingCommandRunner(project_version="0.1.1")
    with pytest.raises(RuntimeError, match="does not match"):
        release_marketplace("studio-v0.1.0", tmp_path, commands)


def test_shell_release_entrypoint_requires_one_tag_argument(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parent.parent / "scripts" / "release_marketplace.sh"

    completed = subprocess.run(
        [str(script)],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "Usage: app/scripts/release_marketplace.sh studio-vX.Y.Z" in completed.stderr


def test_shell_release_entrypoint_uses_frozen_uv_from_repo_root(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parent.parent / "scripts" / "release_marketplace.sh"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    invocation = tmp_path / "uv-invocation"
    fake_python = fake_bin / "python3"
    fake_python.write_text(
        "#!/usr/bin/env sh\nprintf '%s\\n' 'https://packages.example.test/simple'\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        f'#!/usr/bin/env sh\nprintf \'%s\\n\' "$PWD" "${{UV_DEFAULT_INDEX:-}}" "$@" > {invocation}\n',
        encoding="utf-8",
    )
    fake_uv.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"

    completed = subprocess.run(
        [str(script), "studio-v0.1.0"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert completed.returncode == 0
    assert invocation.read_text(encoding="utf-8").splitlines() == [
        str(script.parent.parent.parent),
        "https://packages.example.test/simple",
        "run",
        "--frozen",
        "python",
        "app/scripts/release_marketplace.py",
        "--tag",
        "studio-v0.1.0",
    ]


def test_makefile_exposes_marketplace_release_target() -> None:
    repo_root = Path(__file__).resolve().parents[2]

    completed = subprocess.run(
        ["make", "--dry-run", "app-release-marketplace", "TAG=studio-v0.1.0"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "app/scripts/release_marketplace.sh studio-v0.1.0" in completed.stdout

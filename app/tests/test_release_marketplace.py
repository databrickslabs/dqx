"""Tests for locally signed Marketplace release branches."""

from pathlib import Path

import pytest

from scripts.release_marketplace import CommandResult, release_branch_name, release_marketplace


class RecordingCommandRunner:
    """Record release commands while simulating their filesystem effects."""

    def __init__(
        self,
        *,
        project_version: str = "0.16.1",
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
    assert release_branch_name("v0.16.1") == "marketplace/v0.16.1"


@pytest.mark.parametrize("tag", ["0.16.1", "v0.16", "vnext", "v0.16.1/extra"])
def test_release_rejects_invalid_tag_names(tag: str) -> None:
    with pytest.raises(ValueError, match="vX.Y.Z"):
        release_branch_name(tag)


def test_release_verifies_tag_before_creating_branch(tmp_path: Path) -> None:
    commands = RecordingCommandRunner(fail_on=("git", "verify-tag"))
    with pytest.raises(RuntimeError, match="signed tag"):
        release_marketplace("v0.16.1", tmp_path, commands)
    assert not commands.contains_prefix(("git", "worktree", "add"))


def test_release_creates_and_verifies_signed_commit_without_push(tmp_path: Path) -> None:
    commands = RecordingCommandRunner(project_version="0.16.1")
    branch = release_marketplace("v0.16.1", tmp_path, commands)
    assert branch == "marketplace/v0.16.1"
    assert commands.contains(("git", "show", "v0.16.1:app/pyproject.toml"))
    assert commands.contains_prefix(("git", "commit", "-S"))
    assert commands.contains(("git", "verify-commit", "HEAD"))
    assert not commands.contains_prefix(("git", "push"))
    assert commands.created_worktree is not None
    assert not commands.created_worktree.exists()


def test_release_refuses_existing_local_branch(tmp_path: Path) -> None:
    commands = RecordingCommandRunner(project_version="0.16.1", existing_branch=True)
    with pytest.raises(RuntimeError, match="already exists"):
        release_marketplace("v0.16.1", tmp_path, commands)
    assert not commands.contains_prefix(("git", "worktree", "add"))


def test_release_rejects_tag_version_that_differs_from_app(tmp_path: Path) -> None:
    commands = RecordingCommandRunner(project_version="0.16.0")
    with pytest.raises(RuntimeError, match="does not match"):
        release_marketplace("v0.16.1", tmp_path, commands)

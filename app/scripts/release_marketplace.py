"""Create a locally signed Marketplace release branch from a signed tag."""

import argparse
import re
import subprocess
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class CommandResult:
    """Sanitized result returned by a release command runner."""

    returncode: int
    stdout: str = ""


class CommandRunner(Protocol):
    """Subprocess boundary used by the Marketplace release workflow."""

    def run(self, command: tuple[str, ...], *, cwd: Path, check: bool = True) -> CommandResult:
        """Run one command in *cwd* and return its result."""
        ...


class SubprocessCommandRunner:
    """Run release commands without invoking a shell."""

    def run(self, command: tuple[str, ...], *, cwd: Path, check: bool = True) -> CommandResult:
        """Run one command and optionally fail on a non-zero exit status."""
        completed = subprocess.run(command, cwd=cwd, check=False, capture_output=True, text=True)
        if check and completed.returncode != 0:
            raise RuntimeError(f"command failed: {command[0]} {command[1]}")
        return CommandResult(returncode=completed.returncode, stdout=completed.stdout)


def release_branch_name(tag: str) -> str:
    """Return the release branch name for a valid Marketplace version tag."""
    if re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.-]*)?", tag) is None:
        raise ValueError("TAG must use vX.Y.Z release syntax")
    return f"marketplace/{tag}"


def release_marketplace(tag: str, repo_root: Path, commands: CommandRunner) -> str:
    """Create and verify a local signed Marketplace release branch without pushing."""
    branch = release_branch_name(tag)
    root_result = commands.run(("git", "rev-parse", "--show-toplevel"), cwd=repo_root, check=False)
    if root_result.returncode != 0:
        raise RuntimeError("repo_root must be a Git worktree")
    try:
        resolved_root = Path(root_result.stdout.strip()).resolve(strict=True)
        requested_root = repo_root.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise RuntimeError("repo_root could not be safely resolved") from error
    if resolved_root != requested_root:
        raise RuntimeError("repo_root must be the Git worktree root")

    verified_tag = commands.run(("git", "verify-tag", tag), cwd=resolved_root, check=False)
    if verified_tag.returncode != 0:
        raise RuntimeError("TAG must be an annotated signed tag")

    tagged_pyproject = commands.run(("git", "show", f"{tag}:app/pyproject.toml"), cwd=resolved_root)
    try:
        project_version = tomllib.loads(tagged_pyproject.stdout)["project"]["version"]
    except (KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise RuntimeError("Tagged application version is missing") from error
    if project_version != tag.removeprefix("v"):
        raise RuntimeError("TAG version does not match the tagged application version")

    existing = commands.run(
        ("git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"),
        cwd=resolved_root,
        check=False,
    )
    if existing.returncode == 0:
        raise RuntimeError(f"Local release branch {branch} already exists")

    with tempfile.TemporaryDirectory(prefix="dqx-marketplace-release-") as temp_dir:
        worktree = Path(temp_dir) / "worktree"
        created = False
        try:
            commands.run(("git", "worktree", "add", "-b", branch, str(worktree), tag), cwd=resolved_root)
            created = True
            commands.run(("make", "app-install"), cwd=worktree)
            commands.run(("uv", "run", "--frozen", "python", "app/scripts/build_app.py"), cwd=worktree)
            commands.run(("uv", "run", "--frozen", "python", "app/scripts/build_marketplace.py"), cwd=worktree)
            commands.run(
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
                ),
                cwd=worktree / "app",
            )
            commands.run(("git", "add", "-f", "-A", "app/marketplace"), cwd=worktree)
            changed = commands.run(("git", "diff", "--cached", "--quiet"), cwd=worktree, check=False)
            if changed.returncode == 0:
                raise RuntimeError("Marketplace release produced no changes")
            commands.run(
                ("git", "commit", "-S", "-m", f"build(app): generate Marketplace release {tag}"),
                cwd=worktree,
            )
            commands.run(("git", "verify-commit", "HEAD"), cwd=worktree)
        finally:
            if created:
                commands.run(("git", "worktree", "remove", "--force", str(worktree)), cwd=resolved_root)
                commands.run(("git", "worktree", "prune"), cwd=resolved_root)
    return branch


def main() -> int:
    """Create a local Marketplace release branch and print its manual push command."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="annotated signed version tag, for example v0.16.1")
    args = parser.parse_args()
    try:
        branch = release_marketplace(args.tag, Path.cwd(), SubprocessCommandRunner())
    except (OSError, RuntimeError, ValueError) as error:
        raise SystemExit(f"error: {error}") from None
    print(f"Created and verified local branch {branch}.")
    print(f"Inspect it, then push manually: git push origin {branch}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

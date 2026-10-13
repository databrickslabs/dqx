"""Persist acceptance coverage before completion notifications and combine all reports."""

from collections.abc import Generator
import os
from pathlib import Path
import uuid

from coverage import Coverage, CoverageData
import pytest


def checkpoint_directory(data_file: Path) -> Path:
    """Return the directory kept outside pytest-cov's automatic combine/erase glob."""
    return data_file.with_name(f"{data_file.name}-checkpoints")


class CoverageCheckpoints:
    """Save cumulative process coverage before acceptance observes test completion."""

    def __init__(self, data_file: Path) -> None:
        self.directory = checkpoint_directory(data_file)
        self.name = f"{os.getpid()}-{uuid.uuid4().hex}.coverage"

    @pytest.hookimpl(wrapper=True, tryfirst=True)
    def pytest_runtest_logfinish(self) -> Generator[None, None, None]:
        """Checkpoint before xdist forwards logfinish or acceptance posts its result."""
        current = Coverage.current()
        if current is not None:
            self.directory.mkdir(parents=True, exist_ok=True)
            temporary = self.directory / f"{self.name}.tmp"
            snapshot = CoverageData(basename=str(temporary))
            snapshot.update(current.get_data())
            snapshot.write()
            temporary.replace(self.directory / self.name)
        yield


def pytest_configure(config: pytest.Config) -> None:
    """Enable checkpoints only in the existing acceptance runner's coverage session."""
    data_file = os.environ.get("COVERAGE_FILE")
    if data_file and os.environ.get("REPLY_URL"):
        config.pluginmanager.register(CoverageCheckpoints(Path(data_file).resolve()))


def combine_coverage(data_file: Path, output: Path, *, keep: bool = False, config_file: str | bool = True) -> None:
    """Merge existing, parallel and checkpoint data, failing on invalid inputs.

    Inputs survive a failed merge/report. No stale XML is left for an always-run
    upload step to publish. Checkpoints are complete before acceptance notifications,
    so aggregation does not depend on the later pytest session-shutdown flush.
    """
    output.unlink(missing_ok=True)
    parallel = sorted(
        path
        for path in data_file.parent.glob(f"{data_file.name}.*")
        if path.is_file() and not path.name.endswith(("-journal", "-wal", "-shm"))
    )
    checkpoints = sorted(checkpoint_directory(data_file).glob("*.coverage"))
    inputs = ([data_file] if data_file.exists() else []) + parallel + checkpoints
    if not inputs:
        raise ValueError("No coverage data available")

    # Coverage.combine warns and continues for unreadable files. Validate first so
    # a damaged report cannot silently lower the uploaded coverage percentage.
    modes = set()
    for path in inputs:
        data = CoverageData(basename=str(path))
        data.read()
        modes.add(data.has_arcs())
    if len(modes) != 1:
        raise ValueError("Cannot combine statement and branch coverage; align coverage configuration")

    combined = Coverage(data_file=str(data_file), config_file=config_file)
    combined.load()
    if parallel or checkpoints:
        combined.combine(data_paths=[str(path) for path in parallel + checkpoints], strict=True, keep=True)
    combined.save()
    combined.xml_report(outfile=str(output))
    if not keep:
        for path in parallel + checkpoints:
            path.unlink()


if __name__ == "__main__":
    combine_coverage(
        Path(os.environ.get("COVERAGE_FILE", ".coverage")).resolve(),
        Path("coverage-combined.xml"),
        keep=os.environ.get("KEEP_COVERAGE") == "1",
    )

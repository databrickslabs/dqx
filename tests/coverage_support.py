"""Combine existing and parallel coverage reports without hiding invalid inputs."""

import os
from pathlib import Path

from coverage import Coverage, CoverageData


def combine_coverage(data_file: Path, output: Path, *, config_file: str | bool = True) -> None:
    """Merge existing and parallel data, failing on invalid inputs.

    Inputs survive a failed merge/report. No stale XML is left for an always-run
    upload step to publish.
    """
    output.unlink(missing_ok=True)
    parallel = sorted(
        path
        for path in data_file.parent.glob(f"{data_file.name}.*")
        if path.is_file() and not path.name.endswith(("-journal", "-wal", "-shm"))
    )
    inputs = ([data_file] if data_file.exists() else []) + parallel
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
    if parallel:
        combined.combine(data_paths=[str(path) for path in parallel], strict=True, keep=True)
    combined.save()
    combined.xml_report(outfile=str(output))
    for path in parallel:
        path.unlink()


if __name__ == "__main__":
    combine_coverage(
        Path(os.environ.get("COVERAGE_FILE", ".coverage")).resolve(),
        Path("coverage-combined.xml"),
    )

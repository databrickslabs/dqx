"""Coverage lifecycle regressions using real coverage data and pytest processes."""

from pathlib import Path
import xml.etree.ElementTree as ET

from coverage import CoverageData
from coverage.exceptions import DataError
import pytest

from tests.coverage_support import checkpoint_directory, combine_coverage

pytest_plugins = ["pytester"]


@pytest.fixture
def coverage_source(tmp_path: Path) -> Path:
    source = tmp_path / "coverage_probe.py"
    source.write_text("def first():\n    return 1\n\ndef second():\n    return 2\n\ndef third():\n    return 3\n")
    return source


def write_coverage(path: Path, source: Path, line: int, *, branch: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = CoverageData(basename=str(path))
    if branch:
        data.add_arcs({str(source): [(-(line - 1), line), (line, -(line - 1))]})
    else:
        data.add_lines({str(source): {line}})
    data.write()


@pytest.mark.parametrize("keep", [False, True])
def test_combine_preserves_existing_parallel_and_checkpoint_hits(
    tmp_path: Path, coverage_source: Path, keep: bool
) -> None:
    data_file = tmp_path / ".coverage"
    worker = tmp_path / ".coverage.worker"
    checkpoint = checkpoint_directory(data_file) / "last-worker.coverage"
    write_coverage(data_file, coverage_source, 2)
    write_coverage(worker, coverage_source, 5)
    write_coverage(checkpoint, coverage_source, 8)
    output = tmp_path / "coverage.xml"

    combine_coverage(data_file, output, keep=keep, config_file=False)

    report = ET.parse(output)
    hits = {int(line.attrib["number"]): int(line.attrib["hits"]) for line in report.findall(".//line")}
    assert all(hits[line] > 0 for line in (2, 5, 8))
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert combined.has_arcs()
    lines = combined.lines(str(coverage_source))
    assert lines is not None
    assert set(lines) == {2, 5, 8}
    assert worker.exists() is keep
    assert checkpoint.exists() is keep


def test_combine_accepts_already_combined_data(tmp_path: Path, coverage_source: Path) -> None:
    data_file = tmp_path / ".coverage"
    write_coverage(data_file, coverage_source, 2)
    output = tmp_path / "coverage.xml"
    combine_coverage(data_file, output, config_file=False)
    line = ET.parse(output).find(".//line[@number='2']")
    assert line is not None and line.attrib["hits"] == "1"


@pytest.mark.parametrize("failure", ["missing", "corrupt", "mixed"])
def test_combine_failure_preserves_inputs_and_removes_stale_xml(
    tmp_path: Path, coverage_source: Path, failure: str
) -> None:
    data_file = tmp_path / ".coverage"
    worker = tmp_path / ".coverage.worker"
    if failure != "missing":
        write_coverage(data_file, coverage_source, 2)
        if failure == "corrupt":
            worker.write_bytes(b"not a coverage database")
        else:
            write_coverage(worker, coverage_source, 5, branch=False)
    before = {path: path.read_bytes() for path in tmp_path.glob(".coverage*") if path.is_file()}
    output = tmp_path / "coverage.xml"
    output.write_text("stale report")

    with pytest.raises((ValueError, DataError)):
        combine_coverage(data_file, output, config_file=False)

    assert not output.exists()
    assert all(path.read_bytes() == contents for path, contents in before.items())


@pytest.mark.parametrize("workers", [0, 2])
def test_acceptance_checkpoint_precedes_completion_notification(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    # Environment is the third-party acceptance boundary; no workspace/HTTP calls.
    monkeypatch.setenv("COVERAGE_FILE", str(pytester.path / ".coverage"))
    monkeypatch.setenv("REPLY_URL", "https://example.invalid/unused")
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[2]))
    pytester.makefile(".ini", pytest="[pytest]\n")
    pytester.makefile(".coveragerc", **{"": "[run]\nbranch = true\nparallel = true\n"})
    pytester.makepyfile(probe="def value():\n    return 42\n")
    pytester.makeconftest(
        '''
from pathlib import Path
from coverage import CoverageData
import pytest
pytest_plugins = ["tests.coverage_support"]

@pytest.hookimpl(trylast=True)
def pytest_runtest_logfinish(nodeid):
    # This is the same notification hook used by acceptance. Coverage must already
    # be readable here, before sessionfinish and worker/controller shutdown.
    source = str(Path("probe.py").resolve())
    reports = list(Path(".coverage-checkpoints").glob("*.coverage"))
    assert reports
    hits = set()
    for report in reports:
        data = CoverageData(basename=str(report))
        data.read()
        hits.update(data.lines(source) or [])
    assert 2 in hits
    Path(nodeid.split("::")[-1] + ".notified").write_text("flushed")
'''
    )
    pytester.makepyfile(
        test_probe='''
from probe import value

def test_pass():
    assert value() == 42

def test_fail():
    assert value() == -1
'''
    )
    result = pytester.runpytest_subprocess(
        "-q", "-n", str(workers), "--cov=probe", "--cov-report=", "--cov-config=.coveragerc", "-p", "no:rerunfailures"
    )
    result.assert_outcomes(passed=1, failed=1)
    assert (pytester.path / "test_pass.notified").read_text() == "flushed"
    assert (pytester.path / "test_fail.notified").read_text() == "flushed"
    # Session-end pytest-cov combination must not erase our independent checkpoints.
    assert list((pytester.path / ".coverage-checkpoints").glob("*.coverage"))


@pytest.mark.parametrize("workers", [0, 2])
def test_e2e_coverage_survives_subprocess_working_directory_change(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    repository = Path(__file__).resolve().parents[2]
    checkout = pytester.path / "checkout"
    suite = checkout / "tests" / "e2e"
    suite.mkdir(parents=True)
    source = checkout / "src" / "probe.py"
    source.parent.mkdir()
    source.write_text("def value(flag):\n    if flag:\n        return 42\n    return 0\n\nassert value(True) == 42\n")
    (checkout / ".coveragerc").write_text((repository / ".coveragerc").read_text())
    (suite / ".coveragerc").write_text((repository / "tests" / "e2e" / ".coveragerc").read_text())
    (suite / "pytest.ini").write_text("[pytest]\n")
    (suite / "test_probe.py").write_text(
        '''
from pathlib import Path
import subprocess
import sys


def test_child():
    checkout = Path(__file__).resolve().parents[2]
    subprocess.run([sys.executable, str(checkout / "src" / "probe.py")], cwd=checkout, check=True)
'''
    )
    # Isolate the third-party coverage environment from the outer unit-test run.
    for variable in ("COV_CORE_SOURCE", "COV_CORE_CONFIG", "COV_CORE_DATAFILE", "COV_CORE_BRANCH", "COV_CORE_CONTEXT"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("COVERAGE_FILE", str(suite / ".coverage"))
    monkeypatch.chdir(suite)

    result = pytester.runpytest_subprocess(
        "-q", "-n", str(workers), "--cov", "--cov-report=xml", "--cov-config=.coveragerc", "-p", "no:rerunfailures"
    )

    result.assert_outcomes(passed=1)
    # Successful tests alone miss a coverage session-end crash (pytest exit 3).
    assert result.ret == pytest.ExitCode.OK
    combined = CoverageData(basename=str(suite / ".coverage"))
    combined.read()
    assert combined.has_arcs()
    lines = combined.lines(str(source))
    assert lines is not None and 3 in lines, "The child process's execution must survive aggregation"
    report = ET.parse(suite / "coverage.xml").getroot()
    assert int(report.attrib["branches-valid"]) > 0

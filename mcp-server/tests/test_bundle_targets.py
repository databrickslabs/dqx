"""Guard the duplication between the base bundle config and the `dev-coverage` target.

DAB target overrides *replace* a list rather than merging into it, so the coverage target has to
re-list the whole base `app_environment` and `runner_environment_dependencies` just to add one entry
each. Nothing couples the two copies: change a production default and forget to mirror it, and CI
silently tests a configuration users never get — or the DQX pin drifts between production and CI.

These tests parse the YAML textually rather than resolving the bundle with the Databricks CLI: the
MCP test environment installs only pytest (see `make mcp-test`), so neither the CLI nor PyYAML is
available. The parsing is deliberately narrow — it reads the two `default:` lists under the named
variables — and asserts the relationship, not the exact contents, so ordinary edits do not break it.
"""

import re
from pathlib import Path

_BUNDLE = Path(__file__).resolve().parent.parent / "databricks.yml"


def _list_entries(text: str, block_start: int) -> list[str]:
    """Return the `- ...` items of the YAML list beginning after *block_start*.

    Stops at the first line that is neither a list item, a nested continuation, nor a comment/blank —
    i.e. at the next key at or above the list's own indentation.
    """
    lines = text[block_start:].splitlines()
    entries: list[str] = []
    indent: int | None = None
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        stripped = line.lstrip()
        current_indent = len(line) - len(stripped)
        if stripped.startswith("- "):
            if indent is None:
                indent = current_indent
            if current_indent == indent:
                entries.append(stripped[2:].strip())
            continue
        if indent is not None and current_indent > indent:
            # A nested mapping under the previous item (e.g. `value:` under `- name:`).
            entries.append(stripped)
            continue
        if indent is not None:
            break
    return entries


def _variable_default_list(var_name: str, occurrence: int) -> list[str]:
    """The `default:` list for *var_name*, taking its *occurrence*-th definition (0-based).

    The base definition comes first in the file and the `dev-coverage` override second, so occurrence
    0 is production and 1 is the coverage target.
    """
    text = _BUNDLE.read_text()
    positions = [m.end() for m in re.finditer(rf"^\s*{re.escape(var_name)}:\s*$", text, re.MULTILINE)]
    assert len(positions) > occurrence, f"{var_name} has {len(positions)} definitions in databricks.yml"
    after = text[positions[occurrence] :]
    default = re.search(r"^\s*default:\s*$", after, re.MULTILINE)
    assert default, f"no default list found for {var_name} (occurrence {occurrence})"
    return _list_entries(after, default.end())


class TestCoverageTargetMirrorsBase:
    def test_app_environment_is_a_superset_of_the_base(self):
        """Every production app env var must still be set under dev-coverage."""
        base = _variable_default_list("app_environment", 0)
        coverage = _variable_default_list("app_environment", 1)

        base_names = {e for e in base if e.startswith("name:")}
        coverage_names = {e for e in coverage if e.startswith("name:")}
        missing = base_names - coverage_names
        assert not missing, (
            f"the dev-coverage app_environment override is missing {sorted(missing)}. Target overrides "
            "replace the list, so every base entry must be repeated there or CI tests a configuration "
            "users never get."
        )

    def test_runner_dependencies_match_the_base_except_for_the_intended_swaps(self):
        """dev-coverage may only ADD the test wheels and SWAP DQX for the in-repo build.

        Two deliberate differences, and nothing else: the coverage bootstrap wheel is added, and the
        published `databricks-labs-dqx==<version>` pin is replaced by the wheel built from this repo's
        source (so CI can catch a library regression instead of testing the last release). Any *other*
        divergence means CI exercises a runner environment users never get.
        """
        base = _variable_default_list("runner_environment_dependencies", 0)
        coverage = _variable_default_list("runner_environment_dependencies", 1)

        def _is_published_dqx(dep: str) -> bool:
            return "databricks-labs-dqx" in dep and "@" not in dep

        missing = [d for d in base if d not in coverage and not _is_published_dqx(d)]
        assert not missing, (
            f"the dev-coverage runner dependencies are missing {missing}. Apart from the DQX swap, the "
            "coverage target must install everything production does — otherwise CI exercises a "
            "different runner environment than users get."
        )

        added = [d for d in coverage if d not in base]
        assert any("coverage_bootstrap" in d for d in added), (
            f"dev-coverage should add the coverage bootstrap wheel; it adds {added}. Without it the "
            "target is pointless — no remote coverage is produced."
        )

        # The swap itself: DQX must come from the in-repo wheel path.
        source_dqx = [d for d in coverage if "dqx_wheel_filename" in d]
        assert len(source_dqx) == 1, (
            f"dev-coverage should install DQX from the in-repo wheel exactly once; found {source_dqx}. "
            "Without it the MCP suite tests the last published release, so a library regression in the "
            "PR cannot fail CI."
        )
        assert not any(_is_published_dqx(d) for d in coverage), (
            "dev-coverage still pins a published DQX release alongside the source wheel; pip would "
            "resolve one of them arbitrarily."
        )

    def test_the_datacontract_extra_is_reproduced_for_the_wheel_install(self):
        """The [datacontract] extra's deps must be listed explicitly on the dev-coverage target.

        A wheel path cannot carry an extra: the PEP 508 `pkg[extra] @ file://...` form is valid for pip
        but the Databricks CLI rejects it ("first path segment in URL cannot contain colon"). So the
        extra's contents are duplicated into the dependency list — and duplication drifts, which is
        what this test exists to prevent. If pyproject.toml's [datacontract] extra changes, this fails
        rather than the runner quietly losing a dependency and generate_rules_from_contract breaking.
        """
        pyproject = (_BUNDLE.parent.parent / "pyproject.toml").read_text()
        block = re.search(r"^datacontract\s*=\s*\[(.*?)\]", pyproject, re.MULTILINE | re.DOTALL)
        assert block, "could not find the [datacontract] extra in pyproject.toml"
        required = re.findall(r'"([^"]+)"', block.group(1))
        assert required, "the [datacontract] extra appears to be empty"

        # Entries come back as written in the YAML, so strip the surrounding quotes before comparing.
        coverage = {d.strip('"').strip("'") for d in _variable_default_list("runner_environment_dependencies", 1)}
        missing = [dep for dep in required if dep not in coverage]
        assert not missing, (
            f"the dev-coverage runner dependencies are missing {missing} from the [datacontract] extra. "
            "The in-repo DQX wheel cannot carry the extra, so its dependencies are listed explicitly "
            "and must be kept in sync with pyproject.toml."
        )

    def test_dqx_version_is_the_single_source_and_matches_the_repo(self):
        """`dqx_version` is the one value a release bump edits, and it must match __version__.py.

        The wheel filename and the published pin both derive from ``${var.dqx_version}`` (see the two
        tests below), so this is the only literal that can drift from the repo. sync_versions.py
        rewrites it on ``make fmt``; this test fails if that step was skipped, before a stale value
        reaches a deploy as an opaque pip failure.
        """
        text = _BUNDLE.read_text()
        reported = re.search(r"^\s*dqx_version:\s*\n\s*default:\s*\"([^\"]+)\"", text, re.MULTILINE)
        assert reported, "dqx_version variable not found in databricks.yml"

        version_content = (_BUNDLE.parent.parent / "src" / "databricks" / "labs" / "dqx" / "__version__.py").read_text()
        version = re.search(r'__version__\s*=\s*"([^"]+)"', version_content)
        assert version, "could not read __version__ from src/databricks/labs/dqx/__version__.py"

        assert reported.group(1) == version.group(1), (
            f"dqx_version is {reported.group(1)!r} but the repo is at {version.group(1)!r}. Run "
            "`make fmt` (docs/dqx/sync_versions.py) to propagate the release version, or bump it here."
        )

    def test_the_in_repo_dqx_wheel_derives_from_dqx_version(self):
        """var.dqx_wheel_filename must derive from ${var.dqx_version}, not carry its own literal.

        The bundle names this file in a job dependency list. Deriving it means `uv build` (which
        produces ``databricks_labs_dqx-<dqx_version>-py3-none-any.whl``) and the bundle can never
        disagree — a version bump touches only dqx_version. A hard-coded version here could drift and
        surface as an opaque pip failure inside the runner job.
        """
        text = _BUNDLE.read_text()
        declared = re.search(r"^\s*dqx_wheel_filename:\s*\n\s*default:\s*\"([^\"]+)\"", text, re.MULTILINE)
        assert declared, "dqx_wheel_filename variable not found in databricks.yml"
        assert declared.group(1) == "databricks_labs_dqx-${var.dqx_version}-py3-none-any.whl", (
            f"dqx_wheel_filename is {declared.group(1)!r}; it must derive from ${{var.dqx_version}} so it "
            "cannot drift from the wheel `uv build` produces. Do not hard-code the version here."
        )

    def test_the_published_pin_derives_from_dqx_version(self):
        """The runner's published DQX pin must derive from ${var.dqx_version}, not a literal.

        `dqx_version` also feeds the `dqx/<version>` User-Agent token the telemetry pipeline reads into
        release_version. Deriving the pin from the same variable makes it structurally impossible for
        the installed release and the reported version to disagree — a literal pin could drift and
        attribute MCP usage to a release that never ran, which is worse than a missing version.
        """
        text = _BUNDLE.read_text()
        # No LITERAL databricks-labs-dqx==<digits> pin may remain: it would be a second source of the
        # version, able to drift from dqx_version. The app/ files still pin literally; this guards the
        # MCP bundle only.
        literal_pins = set(re.findall(r"databricks-labs-dqx(?:\[[^\]]*\])?==([0-9][^\s\"']*)", text))
        assert not literal_pins, (
            f"the MCP bundle has a literal DQX pin {sorted(literal_pins)}; pin via "
            "==${var.dqx_version} instead so it cannot drift from the reported telemetry version."
        )
        assert "databricks-labs-dqx[datacontract]==${var.dqx_version}" in text, (
            "the runner dependency list must pin databricks-labs-dqx[datacontract]==${var.dqx_version} "
            "so the installed release and the dqx_version telemetry token stay in lockstep."
        )

    def test_base_sync_does_not_ship_the_test_only_wheel(self):
        """A production deploy must not sync the coverage bootstrap wheel.

        Regression guard. The base list was once `.build/**`, which looks harmless because only
        `dev-coverage` *builds* the bootstrap wheel — but `.build/` is never cleaned, so a wheel left
        there by an earlier coverage deploy was synced by the next production deploy and published by
        `publish_extra_wheels`, shipping test instrumentation to a user deploy from nothing but a stale
        local file. Observed on a real `dev` deploy before the fix.
        """
        text = _BUNDLE.read_text()
        base_sync = re.search(r"^sync:\s*\n\s*include:\s*\n", text, re.MULTILINE)
        assert base_sync, "no top-level sync.include block found in databricks.yml"
        entries = _list_entries(text, base_sync.end())

        assert entries, "the base sync.include list is empty; the runner wheel must be synced"
        assert not any("coverage_bootstrap" in e for e in entries), (
            f"the base sync.include names the coverage bootstrap wheel: {entries}. Only dev-coverage " "may sync it."
        )
        wildcards = [e for e in entries if e.strip('"').strip("'") in (".build/**", ".build/*")]
        assert not wildcards, (
            f"the base sync.include uses a wildcard over .build ({wildcards}), which sweeps up a stale "
            "coverage bootstrap wheel from an earlier dev-coverage deploy. Name the runner wheel "
            "explicitly instead."
        )
        assert any("dqx_mcp_runner" in e for e in entries), (
            f"the base sync.include must still ship the runner wheel; it lists {entries}. Without it "
            "the app cannot publish the wheel and every data tool fails on library install."
        )

    def test_no_literal_dqx_version_can_drift_from_dqx_version_var(self):
        """Every DQX version reference derives from ${var.dqx_version}; no literal may reintroduce drift.

        The pin and the in-repo wheel filename used to carry their own version digits, so a bump had to
        be applied in three places or production and CI would test different releases. Both now derive
        from the single dqx_version variable — this guards that no literal ``databricks-labs-dqx==<v>``
        or ``databricks_labs_dqx-<v>-py3-none-any.whl`` creeps back in and becomes a second source.
        """
        text = _BUNDLE.read_text()
        literal_pins = set(re.findall(r"databricks-labs-dqx(?:\[[^\]]*\])?==([0-9][^\s\"']*)", text))
        # The wheel-name comment intentionally shows a ``<v>`` placeholder, not real digits, so a match
        # here is a genuine hard-coded version.
        literal_wheels = set(re.findall(r"databricks_labs_dqx-(\d+\.\d+\.\d+)-py3-none-any\.whl", text))
        assert not (literal_pins or literal_wheels), (
            f"databricks.yml hard-codes DQX version(s) pins={sorted(literal_pins)} "
            f"wheels={sorted(literal_wheels)}; derive them from ${{var.dqx_version}} so a release bump "
            "touches only that one value and production and CI can never test different releases."
        )


# --- the coverage target's wrapping entry point ---------------------------------------------------
#
# Nothing else couples these files. The bundle names an entry point; the test-only bootstrap wheel is
# what provides it; and the bootstrap defaults its checkpoint thread off precisely because that entry
# point persists coverage deterministically instead. Break any one of those and the failure is silent:
# the runner runs its pristine entry point, no flush happens, and the mcp flag quietly reports less
# coverage than the suite actually exercised — which is the exact failure mode this whole area exists
# to have fixed.

_BOOTSTRAP_PYPROJECT = (
    Path(__file__).resolve().parents[2] / "tests" / "integration_mcp" / "coverage_bootstrap" / "pyproject.toml"
)
_BOOTSTRAP_INIT = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "integration_mcp"
    / "coverage_bootstrap"
    / "src"
    / "dqx_mcp_coverage_bootstrap"
    / "__init__.py"
)


def test_the_coverage_target_runs_an_entry_point_the_bootstrap_wheel_actually_declares() -> None:
    """The overridden entry point must exist as a console script in the bootstrap wheel.

    A rename on either side deploys cleanly and then fails at task start, where the only symptom is
    missing coverage.
    """
    bundle = _BUNDLE.read_text(encoding="utf-8")
    entry_points = re.findall(r"^\s*entry_point:\s*(\S+)\s*$", bundle, re.MULTILINE)
    assert "dqx-mcp-runner-coverage" in entry_points, f"dev-coverage override missing; found {entry_points}"

    scripts = _BOOTSTRAP_PYPROJECT.read_text(encoding="utf-8")
    declared = re.findall(r"^(\S+)\s*=\s*\"dqx_mcp_coverage_bootstrap\.\S+\"\s*$", scripts, re.MULTILINE)
    assert "dqx-mcp-runner-coverage" in declared, f"bootstrap declares {declared}, not the overridden entry point"


def test_the_coverage_override_names_the_package_that_provides_the_entry_point() -> None:
    """``package_name`` selects the distribution the entry point is looked up in, so it must switch too."""
    bundle = _BUNDLE.read_text(encoding="utf-8")
    packages = re.findall(r"^\s*package_name:\s*(\S+)\s*$", bundle, re.MULTILINE)
    assert "dqx_mcp_runner" in packages, "the base task should still run the pristine runner package"
    assert "dqx_mcp_coverage_bootstrap" in packages, "the coverage override must name the bootstrap package"


def test_the_coverage_override_reuses_the_base_task_key() -> None:
    """DAB merges a target's task list into the base by ``task_key``.

    Naming a different key would append a second task instead of overriding the first, so the runner
    job would run the pristine entry point AND the wrapper.
    """
    bundle = _BUNDLE.read_text(encoding="utf-8")
    task_keys = re.findall(r"^\s*-\s*task_key:\s*(\S+)\s*$", bundle, re.MULTILINE)
    assert task_keys.count("dqx_run") == 2, f"expected the base task and one override, found {task_keys}"


def test_the_app_is_given_a_checkpoint_since_the_bootstrap_defaults_it_off() -> None:
    """The app has no wheel entry point to wrap, so it is the one runtime that still needs the thread.

    The bootstrap defaults the interval to 0 for the runner's sake; if the app's explicit value were
    dropped, the app would fall back to atexit alone and lose coverage whenever the platform's
    shutdown budget is exceeded.
    """
    bootstrap = _BOOTSTRAP_INIT.read_text(encoding="utf-8")
    assert (
        'os.getenv("DQX_COVERAGE_CHECKPOINT_SECONDS", "")' in bootstrap
        and "return float(raw) if raw else 0.0" in bootstrap
    ), "the bootstrap should default the checkpoint off; the runner's entry point flushes instead"
    assert "DQX_COVERAGE_CHECKPOINT_SECONDS" in _BUNDLE.read_text(
        encoding="utf-8"
    ), "the dev-coverage app environment must set a checkpoint interval for the app"

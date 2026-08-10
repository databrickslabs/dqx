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

    def test_the_in_repo_dqx_wheel_matches_the_repo_version(self):
        """var.dqx_wheel_filename must name the wheel `uv build` actually produces.

        The bundle names this file in a job dependency list, so a stale version here surfaces as an
        opaque pip failure inside the runner job rather than a deploy error.
        """
        text = _BUNDLE.read_text()
        declared = re.search(r"^\s*dqx_wheel_filename:\s*\n\s*default:\s*\"([^\"]+)\"", text, re.MULTILINE)
        assert declared, "dqx_wheel_filename variable not found in databricks.yml"

        about = (_BUNDLE.parent.parent / "src" / "databricks" / "labs" / "dqx" / "__about__.py").read_text()
        version = re.search(r'__version__\s*=\s*"([^"]+)"', about)
        assert version, "could not read __version__ from src/databricks/labs/dqx/__about__.py"

        expected = f"databricks_labs_dqx-{version.group(1)}-py3-none-any.whl"
        assert declared.group(1) == expected, (
            f"dqx_wheel_filename is {declared.group(1)!r} but the repo is at version "
            f"{version.group(1)!r}, so `uv build` produces {expected!r}. Update the variable."
        )

    def test_reported_dqx_version_matches_the_installed_pin(self):
        """The version reported to telemetry must be the one the runner actually installs.

        `dqx_version` feeds the `dqx/<version>` User-Agent token that the telemetry pipeline reads
        into release_version. If it drifts from the databricks-labs-dqx pin, adoption data is
        attributed to a release that never ran — worse than having no version at all, because it is
        wrong rather than absent.
        """
        text = _BUNDLE.read_text()
        reported = re.search(r"^\s*dqx_version:\s*\n\s*default:\s*\"([^\"]+)\"", text, re.MULTILINE)
        assert reported, "dqx_version variable not found in databricks.yml"
        pins = set(re.findall(r"databricks-labs-dqx(?:\[[^\]]*\])?==([0-9][^\s\"']*)", text))
        assert pins == {reported.group(1)}, (
            f"dqx_version reports {reported.group(1)!r} but the runner installs {sorted(pins)}. "
            "Telemetry would attribute MCP usage to a DQX release that never ran."
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

    def test_dqx_pin_is_identical_in_every_target(self):
        """The DQX pin appears in several places; production and CI must never test different versions."""
        pins = set(re.findall(r"databricks-labs-dqx(?:\[[^\]]*\])?==([0-9][^\s\"']*)", _BUNDLE.read_text()))
        assert len(pins) == 1, (
            f"databricks.yml pins more than one DQX version: {sorted(pins)}. The base and dev-coverage "
            "dependency lists are separate copies, so a bump has to be applied to both."
        )

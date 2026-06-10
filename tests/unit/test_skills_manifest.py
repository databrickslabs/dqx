import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SKILLS_DIRECTORY = REPO_ROOT / "skills"
MANIFEST_PATH = SKILLS_DIRECTORY / ".claude-plugin" / "plugin.json"
FRONTMATTER_PATTERN = re.compile(r"\A---\s*\n(.*?)\n---\s*\n", re.DOTALL)
REQUIRED_FIELDS = ("name", "description")


def list_skills() -> list[str]:
    return json.loads(MANIFEST_PATH.read_text())["skills"]


@pytest.mark.parametrize("entry", list_skills())
def test_manifest_entry_has_skill(entry: str) -> None:
    skill = SKILLS_DIRECTORY / entry / "SKILL.md"
    assert skill.is_file()


@pytest.mark.parametrize("entry", list_skills())
def test_skill_has_required_frontmatter(entry: str) -> None:
    skill = SKILLS_DIRECTORY / entry / "SKILL.md"
    match = FRONTMATTER_PATTERN.match(skill.read_text())
    assert match is not None

    frontmatter = match.group(1)
    for required_field in REQUIRED_FIELDS:
        assert re.search(rf"^{required_field}:", frontmatter, re.MULTILINE)


def test_skills_listed_in_manifest() -> None:
    manifest_entries = {Path(entry).name for entry in list_skills()}
    skills = {d.name for d in SKILLS_DIRECTORY.iterdir() if d.is_dir() and not d.name.startswith(".")}
    orphans = sorted(skills - manifest_entries)
    assert not orphans

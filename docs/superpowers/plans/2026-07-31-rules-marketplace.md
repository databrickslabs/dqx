# Rules Marketplace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an admin-only Marketplace page to DQX Studio where admins browse curated content packs of reusable DQ rules, select rules across packs, and import them into the Rules Registry as reusable templates via the existing batch-import path; relocate the Deploy-demo action into it; and fix a polarity omission in the shared read-only rule-logic view.

**Architecture:** Packs are DQX-YAML files bundled in the app wheel under `backend/marketplace/packs/`. A cached loader parses + validates them with the existing `DQEngine.validate_checks`. A new admin-gated router `GET /api/v1/marketplace/packs` returns packs with each rule's normalized check dict (same shape `normalizeImportedCheck` produces). The frontend filters/searches/selects client-side and imports via the existing `importChecksAsRegistryDrafts` → `POST /registry-rules/batch-import`. Rule preview reuses the exported `RuleLogicDisclosure`.

**Tech Stack:** Backend — Python 3.12, FastAPI, Pydantic 2, PyYAML, DQX library. Frontend — React 19, TypeScript, TanStack Router (file-based), TanStack Query, shadcn/ui, Tailwind 4, lucide-react, react-i18next. Tests — pytest (backend), bun test / vitest-style `*.test.ts` (frontend).

## Global Constraints

Every task's requirements implicitly include these:

- **Commits:** GPG-signed; commit message body ends with the trailer `Co-authored-by: Isaac`. Commit only the files each task names.
- **Never stage `*/uv.lock`** (or any `uv.lock`).
- **i18n 4-locale parity:** every new user-facing string key is added to all four locales — `en.json`, `pt-BR.json`, `it.json`, `es.json` (files under `app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/`). `en` is source of truth; translate the value in each non-English file (never leave the English string behind). Use `t()` for all display text — no hard-coded JSX literals, `aria-label`s, placeholders, toasts.
- **No lint suppression** (no new `# noqa`, `eslint-disable`, `# type: ignore`, `basedpyright: ignore`).
- **`make app-check`** = `bun run tsc -b` + `basedpyright --level error` + `bun test` (UI unit). Must be green before merge.
- **`make app-test`** = backend pytest suite (`cd app && uv run --group test pytest tests/`). Must be green before merge.
- **`make app-regen-api`** after any backend model/route change, to regenerate `ui/lib/api.ts` (orval). Never hand-edit `api.ts`.
- **Frontend server-state** uses orval-generated hooks from `lib/api.ts`; custom helpers live in `lib/api-custom.ts`.
- **Routes** are TanStack Router file-based under `ui/routes/`; `routeTree.gen.ts` regenerates while Vite runs or on `make app-build` — restart the dev server after adding a route file.
- **UI components** use shadcn/ui primitives from `components/ui/`; import alias `@/` → `src/databricks_labs_dqx_app/ui/`; conditional classes via `cn()`.
- **Path root for all app files below:** `app/src/databricks_labs_dqx_app/`. Backend package = `databricks_labs_dqx_app.backend`. Backend tests live in `app/tests/`.
- **SQL rules** must keep regex quantifiers bounded (ReDoS) per repo security rules.
- Whenever a task ships a major user-facing change, note it in the What's-new page (handled in Task 15).

---

## File Structure

**Backend (create):**
- `backend/marketplace/__init__.py` — package marker (empty).
- `backend/marketplace/models.py` — `MarketplaceRule`, `MarketplacePack` Pydantic models (internal, YAML-facing) + API-out models `MarketplaceRuleOut`, `MarketplacePackOut`, `MarketplacePacksOut`.
- `backend/marketplace/loader.py` — discover + parse + validate + cache pack YAML; produce normalized check dicts.
- `backend/marketplace/packs/pricing_and_money.yaml`
- `backend/marketplace/packs/contacts_and_people.yaml`
- `backend/marketplace/packs/addresses_and_geo.yaml`
- `backend/marketplace/packs/dates_and_freshness.yaml`
- `backend/marketplace/packs/standard_checks.yaml`
- `backend/marketplace/packs/codes_and_classifications.yaml`
- `backend/marketplace/packs/transactions_and_amounts.yaml`
- `backend/routes/v1/marketplace.py` — admin-gated router exposing `GET /marketplace/packs`.

**Backend (modify):**
- `backend/routes/v1/__init__.py` — register `marketplace_router`.
- `app/pyproject.toml` — ensure pack YAML ships in the wheel.

**Backend (test — create in `app/tests/`):**
- `app/tests/test_marketplace_loader.py`
- `app/tests/test_marketplace_packs.py`
- `app/tests/test_marketplace_route.py`

**Frontend (create):**
- `ui/lib/marketplace-selection.ts` — pure selection/filter helpers (unit-tested).
- `ui/components/marketplace/DeployDemoRow.tsx` — extracted demo action (amber row + confirm dialog).
- `ui/components/marketplace/PackGroup.tsx` — collapsible pack, tri-state header checkbox.
- `ui/components/marketplace/MarketplaceRuleRow.tsx` — rule row + badges + tags + inline `RuleLogicDisclosure`.
- `ui/components/marketplace/MarketplacePage.tsx` — toolbar, filters, demo row, pack list, import wiring.
- `ui/routes/_sidebar/marketplace.tsx` — admin-gated route.

**Frontend (test — create):**
- `ui/lib/marketplace-selection.test.ts`

**Frontend (modify):**
- `ui/components/apply-rules/RuleConfigCard.tsx` — add polarity line to `RuleLogicBody`.
- `ui/routes/_sidebar/settings.tsx` — remove `deployDemo` entry + `DeployDemoCard`, re-import shared demo row if the danger tab still needs nothing.
- `ui/routes/_sidebar/route.tsx` — add admin-gated "Marketplace" sidebar item above Documentation.
- `ui/lib/i18n/locales/{en,pt-BR,it,es}.json` — new `marketplace.*` keys + `monitoredTables.ruleLogicThenPasses/ruleLogicThenFails`.
- `ui/lib/api.ts` + `ui/types/routeTree.gen.ts` — regenerated (do not hand-edit).

**Docs (modify):**
- `docs/dqx/docs/studio/whats-new/index.mdx` — Marketplace + polarity-fix entry.

---

## Key established facts (verified during exploration)

- Router registration lives in `backend/routes/v1/__init__.py` (imports then `v1_router.include_router(...)`). Admin gate pattern: `router = APIRouter(dependencies=[require_role(UserRole.ADMIN)])` (see `admin.py:46`). `UserRole.ADMIN` is in `backend/common/authorization.py`; `require_role` in `backend/dependencies.py`.
- `get_check_validator()` (`dependencies.py:753`) returns `DQEngine.validate_checks`, signature `(checks: list[dict]) -> ChecksValidationStatus`. `ChecksValidationStatus.has_errors` (property) and `.errors: list[str]` / `.to_string()`.
- **`DQEngine.validate_checks` empirically accepts** `{{slot}}` placeholders in string args (column names, `regex`, `sql_expression.expression`, `is_in_list.allowed` list items, `is_unique.columns` list items) and top-level `name` + `user_metadata` keys. It **rejects** a `{{slot}}` string where a scalar `int` is required — `is_data_fresh.max_age_minutes` must be a literal int. `for_each_column` is a valid top-level key.
- `normalizeImportedCheck(raw)` (`ui/lib/import-registry-rules.ts`) returns `{criticality, check:{function, arguments}, user_metadata}` and **drops any other top-level keys** (including `for_each_column`). Import cannot carry `for_each_column` through without new logic — so the "Full name present" rule is authored as `sql_expression` (see catalogue).
- `importChecksAsRegistryDrafts({checks, checkFunctions, t, steward?, authorKind?, alsoSubmit?}) => Promise<ImportRegistryRulesResult>` where `ImportRegistryRulesResult = {saved, reused, submitted, submitFailed, failed, errors}`. It calls `batchImportRegistryRulesWithDedup({rules, also_submit, skip_duplicates:true})`.
- `RegistryRuleOut` has `polarity: RegistryPolarity | null` where `RegistryPolarity = "pass" | "fail"`. `RuleLogicDisclosure` (exported, `RuleConfigCard.tsx:208`) renders `RuleLogicBody`, which handles `lowcode` / dqx_native (`fn`) / sql (`sql`/`predicate`). i18n namespace is `monitoredTables.*` (`ruleLogicLabel` at en.json:2064).
- Demo card `DeployDemoCard` at `settings.tsx:~3023`, registered as `deployDemo` entry at `settings.tsx:3200` on the `danger` tab. Uses `useDeployDemoContent`, `useDemoContentStatus`, `getDemoContentStatusQueryKey`, `FlaskConical`, `config.demo*` keys.
- `ConfigPage` admin-redirect (settings.tsx:3162): `useEffect(() => { if (!isAdmin) navigate({ to: "/rules/active", replace: true }); }, [isAdmin, navigate])` with `const { isAdmin } = usePermissions()`.
- Sidebar bottom group at `route.tsx:204-226` (Documentation `<a>`). `usePermissions()` returns `isAdmin`.
- Reserved metadata keys (`ui/components/RegistryRuleBadges.tsx`): `RESERVED_DIMENSION_KEY="dimension"`, `RESERVED_SEVERITY_KEY="severity"`, `RESERVED_NAME_KEY="name"`, `RESERVED_DESCRIPTION_KEY="description"`.
- Wheel packaging: `app/pyproject.toml` uses hatchling; `[tool.hatch.build] exclude = [".../ui"]`. Non-py data under `backend/` is included by default (hatch ships the package tree), but the plan pins it explicitly with `[tool.hatch.build.targets.wheel] artifacts`/`include` to be safe.
- Backend tests: `app/tests/test_*.py`, plain pytest, no Databricks connection. Frontend tests: `*.test.ts` run by `bun test`; existing tests are pure-logic (no React rendering) — keep the polarity regression as a logic test on a small pure helper, not a DOM render.

---

## Task 1: Marketplace Pydantic models

**Files:**
- Create: `backend/marketplace/__init__.py` (empty)
- Create: `backend/marketplace/models.py`
- Test: `app/tests/test_marketplace_loader.py` (this task adds the models-only tests; loader tests come in Task 3)

**Interfaces:**
- Produces:
  - `MarketplaceRule` (BaseModel): `name: str`, `description: str`, `industries: list[str] = []`, `regions: list[str] = []`, `criticality: str = "error"`, `dimension: str`, `severity: str`, `check: dict[str, Any]`.
  - `MarketplacePack` (BaseModel): `id: str`, `title: str`, `icon: str`, `description: str`, `rules: list[MarketplaceRule]`.
  - `MarketplaceRuleOut` (BaseModel): `rule_key: str`, `name: str`, `description: str`, `industries: list[str]`, `regions: list[str]`, `dimension: str`, `severity: str`, `check: dict[str, Any]` (the normalized check dict).
  - `MarketplacePackOut` (BaseModel): `id: str`, `title: str`, `icon: str`, `description: str`, `rules: list[MarketplaceRuleOut]`.
  - `MarketplacePacksOut` (BaseModel): `packs: list[MarketplacePackOut]`.
  - Constants: `VALID_DIMENSIONS = {"Validity","Completeness","Accuracy","Consistency","Uniqueness","Timeliness"}`, `VALID_SEVERITIES = {"Low","Medium","High","Critical"}`.

- [ ] **Step 1: Write the failing test**

Create `app/tests/test_marketplace_loader.py`:

```python
from databricks_labs_dqx_app.backend.marketplace.models import (
    MarketplacePack,
    MarketplaceRule,
    VALID_DIMENSIONS,
    VALID_SEVERITIES,
)


def test_marketplace_rule_defaults():
    r = MarketplaceRule(
        name="Must not be null",
        description="Value is present.",
        dimension="Completeness",
        severity="High",
        check={"function": "is_not_null", "arguments": {"column": "{{column}}"}},
    )
    assert r.industries == []
    assert r.regions == []
    assert r.criticality == "error"


def test_marketplace_pack_parses_nested_rules():
    pack = MarketplacePack(
        id="standard-checks",
        title="Standard checks",
        icon="SquareCheck",
        description="Reusable baseline checks.",
        rules=[
            {
                "name": "Must not be null",
                "description": "Value is present.",
                "dimension": "Completeness",
                "severity": "High",
                "check": {"function": "is_not_null", "arguments": {"column": "{{column}}"}},
            }
        ],
    )
    assert pack.rules[0].name == "Must not be null"


def test_dimension_and_severity_vocabularies():
    assert "Validity" in VALID_DIMENSIONS
    assert "Critical" in VALID_SEVERITIES
    assert len(VALID_DIMENSIONS) == 6
    assert len(VALID_SEVERITIES) == 4
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && uv run --group test pytest tests/test_marketplace_loader.py -v`
Expected: FAIL — `ModuleNotFoundError: ...marketplace.models`.

- [ ] **Step 3: Write minimal implementation**

Create `backend/marketplace/__init__.py` (empty file).

Create `backend/marketplace/models.py`:

```python
"""Pydantic models for the Rules Marketplace pack catalogue.

Two layers:
- ``MarketplaceRule`` / ``MarketplacePack`` model the raw pack YAML on disk.
- ``MarketplaceRuleOut`` / ``MarketplacePackOut`` / ``MarketplacePacksOut`` are
  the API response shape returned by ``GET /marketplace/packs``; each rule
  carries the normalized DQX check dict so the UI can preview + import without
  a second round-trip.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

VALID_DIMENSIONS = {"Validity", "Completeness", "Accuracy", "Consistency", "Uniqueness", "Timeliness"}
VALID_SEVERITIES = {"Low", "Medium", "High", "Critical"}


class MarketplaceRule(BaseModel):
    """A single reusable rule as authored in a pack YAML file."""

    name: str
    description: str
    industries: list[str] = Field(default_factory=list)
    regions: list[str] = Field(default_factory=list)
    criticality: str = "error"
    dimension: str
    severity: str
    check: dict[str, Any]


class MarketplacePack(BaseModel):
    """A domain-organised bundle of reusable rules (one YAML file)."""

    id: str
    title: str
    icon: str
    description: str
    rules: list[MarketplaceRule]


class MarketplaceRuleOut(BaseModel):
    """A marketplace rule as returned to the frontend (normalized check dict)."""

    rule_key: str
    name: str
    description: str
    industries: list[str]
    regions: list[str]
    dimension: str
    severity: str
    check: dict[str, Any]


class MarketplacePackOut(BaseModel):
    id: str
    title: str
    icon: str
    description: str
    rules: list[MarketplaceRuleOut]


class MarketplacePacksOut(BaseModel):
    packs: list[MarketplacePackOut]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd app && uv run --group test pytest tests/test_marketplace_loader.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/marketplace/__init__.py \
        app/src/databricks_labs_dqx_app/backend/marketplace/models.py \
        app/tests/test_marketplace_loader.py
git commit -S -m "feat(marketplace): add pack pydantic models

Co-authored-by: Isaac"
```

---

## Task 2: Pack loader (discover + parse + normalize + cache)

**Files:**
- Create: `backend/marketplace/loader.py`
- Modify: `app/tests/test_marketplace_loader.py` (add loader tests)

**Interfaces:**
- Consumes: `MarketplacePack`, `MarketplaceRule`, `MarketplaceRuleOut`, `MarketplacePackOut`, `VALID_DIMENSIONS`, `VALID_SEVERITIES` (Task 1); `DQEngine.validate_checks` via `get_check_validator` (called by the route, Task 5 — the loader takes a validator callable as an argument so it stays unit-testable without DQX).
- Produces:
  - `PACKS_DIR: Path` — `Path(__file__).parent / "packs"`.
  - `slugify(name: str) -> str` — lowercase, non-alnum → `-`, collapse repeats, strip.
  - `normalize_check(rule: MarketplaceRule) -> dict[str, Any]` — returns `{"criticality": rule.criticality, "check": {"function": ..., "arguments": {...}}, "user_metadata": {"name": rule.name, "description": rule.description, "dimension": rule.dimension, "severity": rule.severity}, ["for_each_column": [...]]}` — mirrors `normalizeImportedCheck`'s output shape, preserving a top-level `for_each_column` if the YAML rule's `check` has one.
  - `load_packs(validate_fn: Callable[[list[dict]], ChecksValidationStatus]) -> list[MarketplacePackOut]` — reads every `*.yaml` in `PACKS_DIR` sorted A–Z by title, parses via `MarketplacePack`, validates every rule's normalized check through `validate_fn`, skips (log WARNING) a whole pack if it fails to parse or any of its rules fails validation / vocabulary check, and caches the result in a module global. `rule_key = f"{pack.id}:{slugify(rule.name)}"`.
  - `clear_cache() -> None` — reset the module cache (used by tests).

- [ ] **Step 1: Write the failing test** — append to `app/tests/test_marketplace_loader.py`:

```python
import logging
from pathlib import Path

from databricks.labs.dqx.checks_validator import ChecksValidationStatus

from databricks_labs_dqx_app.backend.marketplace import loader


def _real_validator(checks):
    from databricks.labs.dqx.engine import DQEngine
    return DQEngine.validate_checks(checks)


def test_slugify():
    assert loader.slugify("Valid credit card (Luhn)") == "valid-credit-card-luhn"


def test_normalize_check_shape():
    from databricks_labs_dqx_app.backend.marketplace.models import MarketplaceRule
    rule = MarketplaceRule(
        name="Must not be null",
        description="Value is present.",
        dimension="Completeness",
        severity="High",
        check={"function": "is_not_null", "arguments": {"column": "{{column}}"}},
    )
    out = loader.normalize_check(rule)
    assert out["criticality"] == "error"
    assert out["check"] == {"function": "is_not_null", "arguments": {"column": "{{column}}"}}
    assert out["user_metadata"] == {
        "name": "Must not be null",
        "description": "Value is present.",
        "dimension": "Completeness",
        "severity": "High",
    }


def test_load_packs_returns_sorted_nonempty(tmp_path):
    loader.clear_cache()
    packs = loader.load_packs(_real_validator)
    assert packs, "expected bundled packs to load"
    titles = [p.title for p in packs]
    assert titles == sorted(titles), "packs must be sorted A-Z by title"
    for p in packs:
        assert p.rules, f"{p.id}: empty pack"
        for r in p.rules:
            assert r.rule_key.startswith(f"{p.id}:")


def test_load_packs_skips_malformed_pack(monkeypatch, tmp_path, caplog):
    loader.clear_cache()
    # Point the loader at a temp dir with one good and one broken pack.
    good = tmp_path / "good.yaml"
    good.write_text(
        "id: good\ntitle: Good\nicon: SquareCheck\ndescription: ok\n"
        "rules:\n"
        "  - name: Must not be null\n"
        "    description: Value is present.\n"
        "    dimension: Completeness\n"
        "    severity: High\n"
        "    check:\n"
        "      function: is_not_null\n"
        "      arguments:\n"
        "        column: '{{column}}'\n"
    )
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "id: bad\ntitle: Bad\nicon: X\ndescription: broken\n"
        "rules:\n"
        "  - name: Broken\n"
        "    description: Bad function.\n"
        "    dimension: Validity\n"
        "    severity: Low\n"
        "    check:\n"
        "      function: not_a_real_function\n"
        "      arguments: {}\n"
    )
    monkeypatch.setattr(loader, "PACKS_DIR", tmp_path)
    with caplog.at_level(logging.WARNING):
        packs = loader.load_packs(_real_validator)
    ids = {p.id for p in packs}
    assert "good" in ids
    assert "bad" not in ids
    assert any("bad" in rec.message for rec in caplog.records)
    loader.clear_cache()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && uv run --group test pytest tests/test_marketplace_loader.py -v`
Expected: FAIL — `AttributeError: module ... has no attribute 'slugify'`.

- [ ] **Step 3: Write minimal implementation** — create `backend/marketplace/loader.py`:

```python
"""Discover, parse, validate and cache the bundled marketplace packs.

The loader reads every ``*.yaml`` in :data:`PACKS_DIR` on first request,
validates each rule's normalized check dict through the injected validator
(``DQEngine.validate_checks``), and caches the result. A pack that fails to
parse or whose rules fail validation / vocabulary is logged at WARNING and
skipped — a bad pack never crashes startup or the endpoint.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from databricks.labs.dqx.checks_validator import ChecksValidationStatus

from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.marketplace.models import (
    VALID_DIMENSIONS,
    VALID_SEVERITIES,
    MarketplacePack,
    MarketplacePackOut,
    MarketplaceRule,
    MarketplaceRuleOut,
)

PACKS_DIR = Path(__file__).parent / "packs"

_cache: list[MarketplacePackOut] | None = None

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(name: str) -> str:
    """Lowercase, replace non-alphanumerics with single hyphens, strip ends."""
    return _SLUG_RE.sub("-", name.lower()).strip("-")


def normalize_check(rule: MarketplaceRule) -> dict[str, Any]:
    """Produce the same normalized shape ``normalizeImportedCheck`` yields.

    ``dimension``/``severity``/``name``/``description`` land in reserved
    ``user_metadata`` keys. A ``for_each_column`` (if authored on the check) is
    preserved at the top level so the DQX validator sees the real check.
    """
    check_block = dict(rule.check)
    fn = str(check_block.get("function", ""))
    args = check_block.get("arguments")
    arguments = args if isinstance(args, dict) else {}
    result: dict[str, Any] = {
        "criticality": rule.criticality,
        "check": {"function": fn, "arguments": arguments},
        "user_metadata": {
            "name": rule.name,
            "description": rule.description,
            "dimension": rule.dimension,
            "severity": rule.severity,
        },
    }
    for_each = check_block.get("for_each_column")
    if isinstance(for_each, list):
        result["for_each_column"] = for_each
    return result


def _validate_rule(rule: MarketplaceRule, normalized: dict[str, Any], validate_fn) -> str | None:
    """Return an error string if the rule is invalid, else None."""
    if rule.dimension not in VALID_DIMENSIONS:
        return f"invalid dimension {rule.dimension!r}"
    if rule.severity not in VALID_SEVERITIES:
        return f"invalid severity {rule.severity!r}"
    status: ChecksValidationStatus = validate_fn([normalized])
    if status.has_errors:
        return status.to_string()
    return None


def _load_pack_file(path: Path, validate_fn) -> MarketplacePackOut | None:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        pack = MarketplacePack.model_validate(raw)
    except Exception as exc:  # noqa reason: best-effort — skip a bad pack, don't crash
        logger.warning("Skipping malformed marketplace pack %s: %s", path.name, exc)
        return None

    rules_out: list[MarketplaceRuleOut] = []
    for rule in pack.rules:
        normalized = normalize_check(rule)
        err = _validate_rule(rule, normalized, validate_fn)
        if err is not None:
            logger.warning("Skipping marketplace pack %s: rule %r invalid: %s", pack.id, rule.name, err)
            return None
        rules_out.append(
            MarketplaceRuleOut(
                rule_key=f"{pack.id}:{slugify(rule.name)}",
                name=rule.name,
                description=rule.description,
                industries=rule.industries,
                regions=rule.regions,
                dimension=rule.dimension,
                severity=rule.severity,
                check=normalized,
            )
        )
    return MarketplacePackOut(
        id=pack.id, title=pack.title, icon=pack.icon, description=pack.description, rules=rules_out
    )


def load_packs(validate_fn: Callable[[list[dict]], ChecksValidationStatus]) -> list[MarketplacePackOut]:
    """Load (and cache) all valid packs, sorted A-Z by title."""
    global _cache
    if _cache is not None:
        return _cache
    packs: list[MarketplacePackOut] = []
    for path in sorted(PACKS_DIR.glob("*.yaml")):
        pack = _load_pack_file(path, validate_fn)
        if pack is not None:
            packs.append(pack)
    packs.sort(key=lambda p: p.title)
    _cache = packs
    return _cache


def clear_cache() -> None:
    """Reset the module-level cache (tests / reload)."""
    global _cache
    _cache = None
```

- [ ] **Step 4: Run test to verify it passes** — Run: `cd app && uv run --group test pytest tests/test_marketplace_loader.py -v`. Expected: PASS (all). Note: `test_load_packs_returns_sorted_nonempty` requires at least one pack YAML on disk — if Task 4 hasn't run yet, create a temporary `packs/standard_checks.yaml` with the single "Must not be null" rule now (Task 4 will overwrite it with the full catalogue), OR reorder so Task 4 lands first. (Recommended: run Task 4 before this step's non-tmp assertion; the tmp_path test passes regardless.)

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/marketplace/loader.py app/tests/test_marketplace_loader.py
git commit -S -m "feat(marketplace): add pack loader with validation + caching

Co-authored-by: Isaac"
```

---

## Task 3: Author the pack YAML catalogue (all 7 packs, ~59 rules)

**Files:**
- Create: all 7 files under `backend/marketplace/packs/`.
- Create: `app/tests/test_marketplace_packs.py`

**Interfaces:**
- Consumes: `load_packs`, `clear_cache`, `PACKS_DIR` (Task 2); `DQEngine.validate_checks`.
- Produces: on-disk pack catalogue that every downstream task depends on.

**Authoring rules (apply to every rule):**
- `check.arguments` column args use `{{slot}}` placeholders. Scalar (non-column) args that DQX requires as a concrete type (e.g. `is_data_fresh.max_age_minutes` int, `is_in_range` bounds, `is_in_list.allowed`) are **baked as literals** — a `{{slot}}` there fails validation.
- Regex rules use `regex_match` with args `column` + `regex`. Keep quantifiers bounded.
- Cross-field / checksum / round-to logic uses `sql_expression` with `arguments.expression`; the expression must evaluate **true when the row is good** (DQX `sql_expression` fails on false).
- `dimension` ∈ {Validity, Completeness, Accuracy, Consistency, Uniqueness, Timeliness}; `severity` ∈ {Low, Medium, High, Critical}.
- `industries`/`regions` omitted ⇒ general/global. Use lowercase tags from the taxonomy: industries `banking, retail, healthcare, telco, insurance, logistics`; regions `global, us, uk, eu, canada, australia`.
- Rule `name` must be ≤ 80 chars and not start with an article; `description` is exactly one sentence ending in `.` and not starting with an article (matches the demo-manifest convention the packs test enforces).

- [ ] **Step 1: Write the failing test** — create `app/tests/test_marketplace_packs.py`:

```python
from databricks.labs.dqx.engine import DQEngine

from databricks_labs_dqx_app.backend.marketplace import loader
from databricks_labs_dqx_app.backend.marketplace.models import VALID_DIMENSIONS, VALID_SEVERITIES


def _validator(checks):
    return DQEngine.validate_checks(checks)


def _all_rules():
    loader.clear_cache()
    packs = loader.load_packs(_validator)
    return packs, [r for p in packs for r in p.rules]


def test_seven_packs_present():
    packs, _ = _all_rules()
    ids = {p.id for p in packs}
    assert ids == {
        "pricing-and-money",
        "contacts-and-people",
        "addresses-and-geo",
        "dates-and-freshness",
        "standard-checks",
        "codes-and-classifications",
        "transactions-and-amounts",
    }


def test_total_rule_count_in_expected_range():
    _, rules = _all_rules()
    assert 55 <= len(rules) <= 62, f"expected ~59 rules, got {len(rules)}"


def test_every_rule_validates_and_has_valid_vocab():
    _, rules = _all_rules()
    for r in rules:
        status = DQEngine.validate_checks([r.check])
        assert not status.has_errors, f"{r.rule_key}: {status.to_string()}"
        assert r.dimension in VALID_DIMENSIONS, f"{r.rule_key}: bad dimension {r.dimension}"
        assert r.severity in VALID_SEVERITIES, f"{r.rule_key}: bad severity {r.severity}"


def test_rule_keys_unique():
    _, rules = _all_rules()
    keys = [r.rule_key for r in rules]
    assert len(keys) == len(set(keys)), "duplicate rule_key"


def test_name_and_description_conventions():
    _, rules = _all_rules()
    for r in rules:
        assert r.name and len(r.name) <= 80, f"{r.rule_key}: name empty or >80"
        assert r.name.split()[0].lower() not in {"a", "an", "the"}, f"{r.rule_key}: article"
        assert r.description.endswith("."), f"{r.rule_key}: description not one sentence"
        assert r.description.count(".") == 1, f"{r.rule_key}: >1 sentence"


def test_tag_values_are_from_taxonomy():
    _, rules = _all_rules()
    ok_ind = {"banking", "retail", "healthcare", "telco", "insurance", "logistics"}
    ok_reg = {"global", "us", "uk", "eu", "canada", "australia"}
    for r in rules:
        assert set(r.industries) <= ok_ind, f"{r.rule_key}: bad industry {r.industries}"
        assert set(r.regions) <= ok_reg, f"{r.rule_key}: bad region {r.regions}"
```

- [ ] **Step 2: Run test to verify it fails** — Run: `cd app && uv run --group test pytest tests/test_marketplace_packs.py -v`. Expected: FAIL (no/partial packs on disk).

- [ ] **Step 3: Write the pack YAML files.**

**`packs/pricing_and_money.yaml` (full reference — write verbatim):**

```yaml
id: pricing-and-money
title: Pricing & Money
icon: DollarSign
description: >
  Money-column checks — non-negativity, decimal precision, currency codes,
  card and IBAN structure, and cost-vs-price consistency.
rules:
  - name: Amount must be non-zero
    description: Monetary amount is not exactly zero.
    industries: [banking, retail]
    dimension: Validity
    severity: Medium
    check:
      function: is_not_equal_to
      arguments:
        column: "{{amount}}"
        value: 0
  - name: Price cannot be negative
    description: Price is greater than or equal to zero.
    industries: [retail]
    dimension: Validity
    severity: High
    check:
      function: is_not_less_than
      arguments:
        column: "{{price}}"
        limit: 0
  - name: At most two decimal places
    description: Amount is rounded to at most two decimal places.
    dimension: Consistency
    severity: Low
    check:
      function: sql_expression
      arguments:
        expression: "{{amount}} = round({{amount}}, 2)"
  - name: Valid ISO-4217 currency code
    description: Currency is a three-letter alphabetic code.
    dimension: Validity
    severity: Medium
    check:
      function: regex_match
      arguments:
        column: "{{currency}}"
        regex: "^[A-Za-z]{3}$"
  - name: Valid numeric currency code
    description: Numeric currency code is exactly three digits.
    dimension: Validity
    severity: Low
    check:
      function: regex_match
      arguments:
        column: "{{currency_code}}"
        regex: "^[0-9]{3}$"
  - name: Cost cannot exceed price
    description: Cost is less than or equal to price.
    industries: [retail]
    dimension: Consistency
    severity: Medium
    check:
      function: sql_expression
      arguments:
        expression: "{{cost}} <= {{price}}"
  - name: Margin not extreme
    description: Gross margin falls between zero and ninety-five percent.
    industries: [retail]
    dimension: Accuracy
    severity: Medium
    check:
      function: sql_expression
      arguments:
        expression: "({{price}} - {{cost}}) / nullif({{price}}, 0) between 0 and 0.95"
  - name: Valid credit card (Luhn)
    description: Card number passes the Luhn checksum after non-digits are removed (requires DBR 13.3+).
    industries: [banking]
    dimension: Validity
    severity: High
    check:
      function: sql_expression
      arguments:
        expression: "luhn_check(regexp_replace({{card_number}}, '[^0-9]', ''))"
  - name: Valid IBAN format
    description: IBAN matches the two-letter country plus check-digit structure.
    industries: [banking]
    regions: [eu]
    dimension: Validity
    severity: Medium
    check:
      function: regex_match
      arguments:
        column: "{{iban}}"
        regex: "^[A-Za-z]{2}[0-9]{2}[A-Za-z0-9]{1,30}$"
```

**`packs/standard_checks.yaml` (full reference — write verbatim):**

```yaml
id: standard-checks
title: Standard checks
icon: SquareCheck
description: >
  The reusable baseline every table needs — presence, emptiness, whitespace,
  uniqueness, UUID format, and statistical outliers.
rules:
  - name: Must not be null
    description: Value is present.
    dimension: Completeness
    severity: High
    check:
      function: is_not_null
      arguments:
        column: "{{column}}"
  - name: Must not be empty
    description: String value is not the empty string.
    dimension: Completeness
    severity: High
    check:
      function: is_not_empty
      arguments:
        column: "{{column}}"
  - name: Must have no surrounding whitespace
    description: Value has no leading or trailing whitespace.
    dimension: Validity
    severity: Low
    check:
      function: sql_expression
      arguments:
        expression: "{{column}} = trim({{column}})"
  - name: Must be unique
    description: Value is unique across the dataset.
    dimension: Uniqueness
    severity: Critical
    check:
      function: is_unique
      arguments:
        columns:
          - "{{column}}"
  - name: Valid UUID
    description: Value matches the canonical 8-4-4-4-12 UUID format.
    dimension: Validity
    severity: Medium
    check:
      function: regex_match
      arguments:
        column: "{{uuid}}"
        regex: "^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$"
  - name: Must have no statistical outliers
    description: Value lies within the median-absolute-deviation bounds.
    dimension: Accuracy
    severity: Medium
    check:
      function: has_no_outliers
      arguments:
        column: "{{column}}"
```

**Remaining five packs — author each rule verbatim from the catalogue below.** Every rule follows the same YAML shape as the two reference packs (`name`, `description`, optional `industries`/`regions`, `dimension`, `severity`, `check.function`, `check.arguments`). Descriptions must be one sentence — draft the sentence as shown.

**`packs/contacts_and_people.yaml`** — `id: contacts-and-people`, `title: Contacts & People`, `icon: User`, description: "Person and contact-detail checks — email, phone in several formats, names, and telco subscriber identifiers.":
1. **Valid email** — Validity/High — `is_valid_email`, args `column: "{{email}}"`.
2. **Valid phone (E.164)** — Validity/High — `regex_match`, `column: "{{phone}}"`, `regex: "^\\+[1-9][0-9]{1,14}$"`.
3. **Phone must include country code** — Validity/Medium — `sql_expression`, `expression: "{{phone}} rlike '^\\\\+[1-9][0-9]{0,2}[0-9]{4,}$'"`. (SQL-string escaping: single backslash-plus inside the SQL literal.)
4. **Valid US phone (NANP)** — Validity/Low — region `[us]` — `regex_match`, `column: "{{phone}}"`, `regex: "^\\+?1?[2-9][0-9]{2}[2-9][0-9]{6}$"`.
5. **Full name must be present** — Completeness/Medium — `sql_expression`, `expression: "{{first_name}} is not null and trim({{first_name}}) <> '' and {{last_name}} is not null and trim({{last_name}}) <> ''"`. (Authored as sql_expression, NOT `for_each_column`, because `normalizeImportedCheck` drops `for_each_column` on import — see Key facts.)
6. **Valid MSISDN** — Validity/Medium — industry `[telco]` — `regex_match`, `column: "{{msisdn}}"`, `regex: "^\\+?[1-9][0-9]{1,14}$"`.
7. **Valid IMSI** — Validity/Medium — industry `[telco]` — `regex_match`, `column: "{{imsi}}"`, `regex: "^[0-9]{15}$"`.

**`packs/addresses_and_geo.yaml`** — `id: addresses-and-geo`, `title: Addresses & Geography`, `icon: MapPin`, description: "Postal, country, coordinate and administrative-region checks across several regions.":
1. **Valid UK postcode** — Validity/Medium — region `[uk]` — `regex_match`, `column: "{{postcode}}"`, `regex: "^[A-Za-z]{1,2}[0-9][0-9A-Za-z]? ?[0-9][A-Za-z]{2}$"`.
2. **Valid Canadian postal code** — Validity/Low — region `[canada]` — `regex_match`, `column: "{{postal_code}}"`, `regex: "^[A-Za-z][0-9][A-Za-z] ?[0-9][A-Za-z][0-9]$"`.
3. **Valid Netherlands postcode** — Validity/Low — region `[eu]` — `regex_match`, `column: "{{postcode}}"`, `regex: "^[0-9]{4} ?[A-Za-z]{2}$"`.
4. **Valid German postcode** — Validity/Low — region `[eu]` — `regex_match`, `column: "{{postcode}}"`, `regex: "^[0-9]{5}$"`.
5. **Valid French postcode** — Validity/Low — region `[eu]` — `regex_match`, `column: "{{postcode}}"`, `regex: "^[0-9]{5}$"`.
6. **Valid Australian postcode** — Validity/Low — region `[australia]` — `regex_match`, `column: "{{postcode}}"`, `regex: "^[0-9]{4}$"`.
7. **Valid postcode (generic)** — Validity/Low — region `[global]` — `regex_match`, `column: "{{postcode}}"`, `regex: "^[A-Za-z0-9][A-Za-z0-9 -]{1,10}[A-Za-z0-9]$"` (bounded, ReDoS-safe).
8. **Valid ISO-2 country code** — Validity/Medium — `regex_match`, `column: "{{country}}"`, `regex: "^[A-Za-z]{2}$"`.
9. **Valid ISO-3 country code** — Validity/Medium — `regex_match`, `column: "{{country}}"`, `regex: "^[A-Za-z]{3}$"`.
10. **Valid latitude** — Validity/Medium — `is_in_range`, `column: "{{lat}}"`, `min_limit: -90`, `max_limit: 90`.
11. **Valid longitude** — Validity/Medium — `is_in_range`, `column: "{{lon}}"`, `min_limit: -180`, `max_limit: 180`.
12. **Valid US state code** — Validity/Medium — region `[us]` — `is_in_list`, `column: "{{state}}"`, `allowed: [AL, AK, AZ, AR, CA, CO, CT, DE, FL, GA, HI, ID, IL, IN, IA, KS, KY, LA, ME, MD, MA, MI, MN, MS, MO, MT, NE, NV, NH, NJ, NM, NY, NC, ND, OH, OK, OR, PA, RI, SC, SD, TN, TX, UT, VT, VA, WA, WV, WI, WY, DC, PR, GU, VI, AS, MP]`.
13. **Valid Canadian province code** — Validity/Medium — region `[canada]` — `is_in_list`, `column: "{{province}}"`, `allowed: [AB, BC, MB, NB, NL, NS, NT, NU, ON, PE, QC, SK, YT]`.
14. **Valid Australian state code** — Validity/Medium — region `[australia]` — `is_in_list`, `column: "{{state}}"`, `allowed: [NSW, VIC, QLD, SA, WA, TAS, NT, ACT]`.
15. **Valid ISO-639 language code** — Validity/Low — `regex_match`, `column: "{{language}}"`, `regex: "^[a-z]{2}$"`.
16. **Valid continent code** — Validity/Low — `is_in_list`, `column: "{{continent}}"`, `allowed: [AF, AN, AS, EU, NA, OC, SA]`.

**`packs/dates_and_freshness.yaml`** — `id: dates-and-freshness`, `title: Dates & Freshness`, `icon: CalendarClock`, description: "Timestamp validity, ordering, freshness, and calendar-name checks.":
1. **Must not be in the future** — Validity/Medium — `is_not_in_future`, `column: "{{ts}}"`.
2. **End must not precede start** — Consistency/High — `sql_expression`, `expression: "{{end_ts}} >= {{start_ts}}"`.
3. **Must be fresh (within SLA)** — Timeliness/Medium — `is_data_fresh`, `column: "{{ts}}"`, `max_age_minutes: 1440` (literal int — a `{{slot}}` here fails validation; 1440 = 24h default, note in description).
   - description: "Timestamp is no older than one day from now."
4. **Admission before discharge** — Consistency/High — industry `[healthcare]` — `sql_expression`, `expression: "{{admission_ts}} <= {{discharge_ts}}"`.
5. **Valid day-of-week name** — Validity/Low — `is_in_list`, `column: "{{day}}"`, `allowed: [Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday]`.
6. **Valid month name** — Validity/Low — `is_in_list`, `column: "{{month}}"`, `allowed: [January, February, March, April, May, June, July, August, September, October, November, December]`.

**`packs/codes_and_classifications.yaml`** — `id: codes-and-classifications`, `title: Codes & Classifications`, `icon: FileBadge`, description: "Domain code-set checks — clinical codes, colour, and blood-type classifications.":
1. **Valid ICD-10-CM code** — Validity/High — industry `[healthcare]` — `regex_match`, `column: "{{icd10}}"`, `regex: "^[A-TV-Z][0-9][0-9A-Za-z]([.][0-9A-Za-z]{1,4})?$"`.
2. **Valid CPT code** — Validity/Medium — industry `[healthcare]` — `regex_match`, `column: "{{cpt}}"`, `regex: "^[0-9]{4}[0-9A-Za-z]$"`.
3. **Valid FHIR administrative gender** — Validity/Medium — industry `[healthcare]` — `is_in_list`, `column: "{{gender}}"`, `allowed: [male, female, other, unknown]`.
4. **Valid hex colour** — Validity/Low — industry `[retail]` — `regex_match`, `column: "{{hex}}"`, `regex: "^#[0-9A-Fa-f]{6}$"`.
5. **Valid blood type** — Validity/Medium — industry `[healthcare]` — `is_in_list`, `column: "{{blood_type}}"`, `allowed: ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]`.

**`packs/transactions_and_amounts.yaml`** — `id: transactions-and-amounts`, `title: Transactions & Amounts`, `icon: ShieldAlert`, description: "Banking transaction-integrity checks — structuring detection, sign-vs-type consistency, and duplicate detection.", all industry `[banking]`:
1. **Round-amount structuring** — Validity/High — `sql_expression`, `expression: "not ({{amount}} >= 1000 and {{amount}} = round({{amount}}, -3))"` (fails only on suspiciously round large amounts; true = clean).
2. **Amount just below reporting threshold** — Validity/High — `sql_expression`, `expression: "not ({{amount}} between 9950 and 9999)"` (true = clean).
3. **Credit must be positive** — Consistency/High — `sql_expression`, `expression: "{{type}} <> 'credit' or {{amount}} > 0"`.
4. **Debit must be negative** — Consistency/High — `sql_expression`, `expression: "{{type}} <> 'debit' or {{amount}} < 0"`.
5. **Duplicate transaction** — Uniqueness/High — `is_unique`, `columns: ["{{account}}", "{{amount}}", "{{reference}}"]`.

- [ ] **Step 4: Run test to verify it passes** — Run: `cd app && uv run --group test pytest tests/test_marketplace_packs.py tests/test_marketplace_loader.py -v`. Expected: PASS. If any rule reports a validation error, fix that rule's `check` (most likely: a scalar arg carrying a `{{slot}}`, or an unbalanced regex/SQL string) and re-run.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/marketplace/packs/ app/tests/test_marketplace_packs.py
git commit -S -m "feat(marketplace): author the 7-pack rule catalogue

Co-authored-by: Isaac"
```

---

## Task 4: Marketplace router + registration + wheel packaging

**Files:**
- Create: `backend/routes/v1/marketplace.py`
- Modify: `backend/routes/v1/__init__.py`
- Modify: `app/pyproject.toml`
- Create: `app/tests/test_marketplace_route.py`

**Interfaces:**
- Consumes: `load_packs` (Task 2); `MarketplacePacksOut` (Task 1); `get_check_validator`, `require_role`, `UserRole` (existing deps).
- Produces: `GET /api/v1/marketplace/packs` → `MarketplacePacksOut`, `operation_id="listMarketplacePacks"`, hard-gated to ADMIN. Orval will generate `useListMarketplacePacks` / `useListMarketplacePacksSuspense` and types `MarketplacePacksOut`, `MarketplacePackOut`, `MarketplaceRuleOut`.

- [ ] **Step 1: Write the failing test** — create `app/tests/test_marketplace_route.py`:

```python
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks.labs.dqx.engine import DQEngine

from databricks_labs_dqx_app.backend import dependencies as deps
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.marketplace import loader
from databricks_labs_dqx_app.backend.routes.v1.marketplace import router as marketplace_router


def _make_app(role: UserRole) -> FastAPI:
    loader.clear_cache()
    app = FastAPI()
    app.include_router(marketplace_router, prefix="/api/v1/marketplace")
    app.dependency_overrides[deps.get_current_user_role] = lambda: role
    app.dependency_overrides[deps.get_check_validator] = lambda: DQEngine.validate_checks
    return app


def test_admin_gets_catalogue():
    client = TestClient(_make_app(UserRole.ADMIN))
    resp = client.get("/api/v1/marketplace/packs")
    assert resp.status_code == 200
    data = resp.json()
    assert data["packs"], "expected packs"
    rule = data["packs"][0]["rules"][0]
    assert {"rule_key", "name", "dimension", "severity", "check", "industries", "regions"} <= set(rule)


def test_non_admin_rejected():
    client = TestClient(_make_app(UserRole.RULE_AUTHOR))
    resp = client.get("/api/v1/marketplace/packs")
    assert resp.status_code == 403
```

Note: confirm the exact dependency name `require_role` reads (`get_current_user_role` vs `CurrentUserRole`). Grep `dependencies.py` for the function `require_role` uses and override that symbol in the test.

- [ ] **Step 2: Run test to verify it fails** — Run: `cd app && uv run --group test pytest tests/test_marketplace_route.py -v`. Expected: FAIL — cannot import `marketplace` router.

- [ ] **Step 3: Write minimal implementation** — create `backend/routes/v1/marketplace.py`:

```python
"""Rules Marketplace routes — admin-only pack catalogue.

The whole router is hard-gated to :class:`UserRole.ADMIN`; the UI sidebar gate
and route redirect are conveniences, this is the real boundary. Packs are
bundled YAML, loaded + validated + cached by the marketplace loader.
"""

from collections.abc import Callable
from typing import Annotated, Any

from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from fastapi import APIRouter, Depends

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import get_check_validator, require_role
from databricks_labs_dqx_app.backend.marketplace import loader
from databricks_labs_dqx_app.backend.marketplace.models import MarketplacePacksOut

router = APIRouter(dependencies=[require_role(UserRole.ADMIN)])


@router.get("/packs", response_model=MarketplacePacksOut, operation_id="listMarketplacePacks")
def list_marketplace_packs(
    validate_fn: Annotated[Callable[[list[Any]], ChecksValidationStatus], Depends(get_check_validator)],
) -> MarketplacePacksOut:
    """Return the full marketplace pack catalogue (admin only)."""
    return MarketplacePacksOut(packs=loader.load_packs(validate_fn))
```

Modify `backend/routes/v1/__init__.py`: add import alongside the others (after line 35) and registration (after line 70):

```python
from .marketplace import router as marketplace_router
```
```python
v1_router.include_router(marketplace_router, prefix="/marketplace", tags=["marketplace"])
```

Modify `app/pyproject.toml` — under `[tool.hatch.build]` add the pack directory to `artifacts` so it always ships in the wheel (append to the existing `artifacts` list):

```toml
artifacts = [
    "src/databricks_labs_dqx_app/__dist__",
    "src/databricks_labs_dqx_app/_metadata.py",
    "src/databricks_labs_dqx_app/_version.py",
    "src/databricks_labs_dqx_app/backend/marketplace/packs/*.yaml",
]
```

- [ ] **Step 4: Run test to verify it passes** — Run: `cd app && uv run --group test pytest tests/test_marketplace_route.py -v`. Expected: PASS (both). Then run the full marketplace suite: `cd app && uv run --group test pytest tests/ -k marketplace -v`.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/routes/v1/marketplace.py \
        app/src/databricks_labs_dqx_app/backend/routes/v1/__init__.py \
        app/pyproject.toml app/tests/test_marketplace_route.py
git commit -S -m "feat(marketplace): add admin-gated GET /marketplace/packs

Co-authored-by: Isaac"
```

---

## Task 5: Regenerate the typed API client

**Files:**
- Modify (generated): `ui/lib/api.ts`

**Interfaces:**
- Consumes: the backend route + models (Task 4).
- Produces: orval hooks `useListMarketplacePacks`, `useListMarketplacePacksSuspense`, `getListMarketplacePacksQueryKey`, and TS types `MarketplacePacksOut`, `MarketplacePackOut`, `MarketplaceRuleOut` in `lib/api.ts`.

- [ ] **Step 1: Regenerate** — Run: `make app-regen-api`. Expected: dumps fresh OpenAPI + runs orval; `ui/lib/api.ts` now contains `useListMarketplacePacks`.

- [ ] **Step 2: Verify the new symbols exist** — Run: `cd app && grep -n "listMarketplacePacks\|MarketplaceRuleOut" src/databricks_labs_dqx_app/ui/lib/api.ts | head`. Expected: matches for the hook and the type.

- [ ] **Step 3: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/lib/api.ts
git commit -S -m "chore(marketplace): regenerate api.ts for /marketplace/packs

Co-authored-by: Isaac"
```

---

## Task 6: Polarity fix in RuleLogicDisclosure + i18n

**Files:**
- Modify: `ui/components/apply-rules/RuleConfigCard.tsx` (`RuleLogicBody`, ~L161-202; add a small pure helper)
- Modify: `ui/lib/i18n/locales/{en,pt-BR,it,es}.json`
- Create: `ui/lib/marketplace-selection.ts` (only the `polarityLineKey` helper for now; extended in Task 10) — OR add the helper inline and export it. To keep the regression test pure (no React render — matches repo test style), extract the mapping into a tiny exported pure function and test that.
- Test: `ui/lib/marketplace-selection.test.ts` (polarity mapping)

**Interfaces:**
- Consumes: `RegistryRuleOut.polarity` (`"pass" | "fail" | null`), i18n keys.
- Produces: exported `polarityLineKey(polarity: "pass" | "fail" | null | undefined): "monitoredTables.ruleLogicThenPasses" | "monitoredTables.ruleLogicThenFails" | null`. Rendered as a read-only line beneath the rule body in all three modes. `null`/`undefined` polarity ⇒ no line (dqx_native rules typically carry no polarity).

- [ ] **Step 1: Write the failing test** — create `ui/lib/marketplace-selection.test.ts`:

```typescript
import { describe, expect, it } from "vitest";
import { polarityLineKey } from "./marketplace-selection";

describe("polarityLineKey", () => {
  it("maps pass -> then-passes key", () => {
    expect(polarityLineKey("pass")).toBe("monitoredTables.ruleLogicThenPasses");
  });
  it("maps fail -> then-fails key", () => {
    expect(polarityLineKey("fail")).toBe("monitoredTables.ruleLogicThenFails");
  });
  it("returns null when polarity is absent", () => {
    expect(polarityLineKey(null)).toBeNull();
    expect(polarityLineKey(undefined)).toBeNull();
  });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/marketplace-selection.test.ts`. Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation.**

Create `ui/lib/marketplace-selection.ts`:

```typescript
import type { RegistryRuleOut } from "@/lib/api";

/**
 * Map a rule's polarity to the i18n key for its read-only "THEN THE RULE
 * PASSES/FAILS" line. Returns null when polarity is absent (dqx_native rules
 * carry no polarity, so no line is shown).
 */
export function polarityLineKey(
  polarity: RegistryRuleOut["polarity"] | null | undefined,
): "monitoredTables.ruleLogicThenPasses" | "monitoredTables.ruleLogicThenFails" | null {
  if (polarity === "pass") return "monitoredTables.ruleLogicThenPasses";
  if (polarity === "fail") return "monitoredTables.ruleLogicThenFails";
  return null;
}
```

In `RuleConfigCard.tsx`, import the helper (top of file with the other `@/lib` imports):

```typescript
import { polarityLineKey } from "@/lib/marketplace-selection";
```

Then wrap the render output of `RuleLogicBody` so the polarity line is appended in every mode. Replace the `RuleLogicBody` function body's returns with a shared wrapper. Concretely, change `RuleLogicBody` (L161-202) so both its lowcode branch and its native/sql branch render inside a fragment that also renders the polarity line:

```tsx
function PolarityLine({ registryRule }: { registryRule: RegistryRuleOut }) {
  const { t } = useTranslation();
  const key = polarityLineKey(registryRule.polarity);
  if (!key) return null;
  return (
    <div className="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">
      {t(key)}
    </div>
  );
}

function RuleLogicBody({ registryRule }: { registryRule: RegistryRuleOut }) {
  const { t } = useTranslation();
  const body = (registryRule.definition.body ?? {}) as Record<string, unknown>;
  const fn = typeof body.function === "string" ? body.function : undefined;
  const sql = typeof body.sql_query === "string" ? body.sql_query : undefined;
  const predicate = typeof body.predicate === "string" ? body.predicate : undefined;
  const parameters = registryRule.definition.parameters ?? [];

  const { data: fnData } = useListCheckFunctions();
  const fnLabel = fn
    ? (fnData?.data?.functions ?? []).find((f) => f.name === fn)?.label ?? fn
    : undefined;

  if (registryRule.mode === "lowcode") {
    return (
      <div className="space-y-3">
        <LowcodeLogicBody registryRule={registryRule} />
        <PolarityLine registryRule={registryRule} />
      </div>
    );
  }

  if (!fn && !sql && !predicate) {
    return (
      <div className="space-y-3">
        <p className="text-xs italic text-muted-foreground">{t("monitoredTables.ruleLogicUnavailable")}</p>
        <PolarityLine registryRule={registryRule} />
      </div>
    );
  }

  const sqlBody = sql ?? predicate;
  const text = fn ? (fnLabel ?? fn) : (sqlBody ?? t("monitoredTables.ruleLogicCustomSql"));

  return (
    <div className="space-y-3">
      <pre className="font-mono text-xs whitespace-pre-wrap rounded bg-muted/40 p-3 overflow-x-auto">
        {text}
      </pre>
      {fn && <RuleParametersView parameters={parameters} />}
      <PolarityLine registryRule={registryRule} />
    </div>
  );
}
```

Add the two i18n keys to each locale under the existing `monitoredTables` block (next to `ruleLogicLabel`):

- `en.json`: `"ruleLogicThenPasses": "THEN THE RULE PASSES"`, `"ruleLogicThenFails": "THEN THE RULE FAILS"`.
- `pt-BR.json`: `"ruleLogicThenPasses": "ENTÃO A REGRA PASSA"`, `"ruleLogicThenFails": "ENTÃO A REGRA FALHA"`.
- `it.json`: `"ruleLogicThenPasses": "ALLORA LA REGOLA PASSA"`, `"ruleLogicThenFails": "ALLORA LA REGOLA FALLISCE"`.
- `es.json`: `"ruleLogicThenPasses": "ENTONCES LA REGLA PASA"`, `"ruleLogicThenFails": "ENTONCES LA REGLA FALLA"`.

- [ ] **Step 4: Run tests to verify they pass** — Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/marketplace-selection.test.ts` (PASS). Then `cd app && bun run tsc -b --incremental` (no type errors in `RuleConfigCard.tsx`).

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/apply-rules/RuleConfigCard.tsx \
        app/src/databricks_labs_dqx_app/ui/lib/marketplace-selection.ts \
        app/src/databricks_labs_dqx_app/ui/lib/marketplace-selection.test.ts \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/en.json \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/pt-BR.json \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/it.json \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/es.json
git commit -S -m "fix(apply-rules): show rule polarity in read-only rule logic view

Co-authored-by: Isaac"
```

---

## Task 7: Extract DeployDemoRow (shared) + remove from settings

**Files:**
- Create: `ui/components/marketplace/DeployDemoRow.tsx`
- Modify: `ui/routes/_sidebar/settings.tsx` (remove `DeployDemoCard` function + `deployDemo` entry + now-unused imports)
- Modify i18n: add `marketplace.demoRow*` keys (4 locales) if new copy is needed; reuse existing `config.demo*` keys for the dialog to avoid churn.

**Interfaces:**
- Consumes: `useDeployDemoContent`, `useDemoContentStatus`, `getDemoContentStatusQueryKey` (from `@/lib/api`), `usePermissions`, `config.demo*` i18n keys.
- Produces: `export function DeployDemoRow(): JSX.Element` — an amber-tinted clickable row that opens the demo confirm dialog (wipe-first checkbox defaults checked); on success toasts `config.demoStarted` and invalidates the status query. No standalone Deploy button — the row is the trigger.

- [ ] **Step 1: Write the failing test** — this is a presentational component with network hooks; per repo convention (no React-render tests) verify via `tsc` + manual QA rather than a unit test. Skip a dedicated failing test; the gate is Step 4 (`tsc`) + Task 14 manual QA. (This task's "test" is type-checking the new module and the modified settings file.)

- [ ] **Step 2: Confirm baseline compiles** — Run: `cd app && bun run tsc -b --incremental`. Expected: PASS before changes.

- [ ] **Step 3: Implement.**

Create `ui/components/marketplace/DeployDemoRow.tsx` by lifting the logic from `settings.tsx`'s `DeployDemoCard`, restyled as a single clickable amber row:

```tsx
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import type { AxiosError } from "axios";
import { FlaskConical, Loader2 } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";
import { usePermissions } from "@/hooks/use-permissions";
import {
  useDeployDemoContent,
  useDemoContentStatus,
  getDemoContentStatusQueryKey,
} from "@/lib/api";

export function DeployDemoRow() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { isAdmin } = usePermissions();
  const [open, setOpen] = useState(false);
  const [wipeFirst, setWipeFirst] = useState(true);
  const deployMutation = useDeployDemoContent();

  const { data: statusResp } = useDemoContentStatus({
    query: {
      refetchInterval: (query) => (query.state.data?.data?.state === "running" ? 10000 : false),
    },
  });
  const status = statusResp?.data;
  const isRunning = status?.state === "running";

  const closeDialog = () => setOpen(false);

  const handleConfirm = () => {
    if (deployMutation.isPending) return;
    deployMutation.mutate(
      { data: { wipe_first: wipeFirst } },
      {
        onSuccess: () => {
          toast.success(t("config.demoStarted"));
          closeDialog();
          queryClient.invalidateQueries({ queryKey: getDemoContentStatusQueryKey() });
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.demoFailed"));
        },
      },
    );
  };

  return (
    <>
      <button
        type="button"
        disabled={!isAdmin || isRunning}
        onClick={() => {
          setWipeFirst(true);
          setOpen(true);
        }}
        className={cn(
          "w-full flex items-start gap-3 rounded-lg border border-amber-500/40 bg-amber-500/5 px-4 py-3 text-left transition-colors hover:bg-amber-500/10 disabled:opacity-60 disabled:cursor-not-allowed",
        )}
      >
        <FlaskConical className="h-5 w-5 shrink-0 text-amber-600" aria-hidden />
        <div className="space-y-1">
          <div className="text-sm font-medium flex items-center gap-2">
            {t("config.demoTitle")}
            {isRunning && <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />}
          </div>
          <p className="text-xs text-muted-foreground leading-relaxed">
            {isRunning ? t("config.demoRunningBanner", { phase: status?.phase ?? "" }) : t("config.demoBody")}
          </p>
        </div>
      </button>

      <Dialog
        open={open}
        onOpenChange={(o) => {
          if (deployMutation.isPending) return;
          if (o) setOpen(true);
          else closeDialog();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FlaskConical className="h-5 w-5" />
              {t("config.demoDialogTitle")}
            </DialogTitle>
            <DialogDescription>{t("config.demoWarning")}</DialogDescription>
          </DialogHeader>
          <div className="flex items-start gap-2">
            <Checkbox
              id="demo-wipe-first"
              checked={wipeFirst}
              onCheckedChange={(c) => setWipeFirst(c === true)}
              disabled={deployMutation.isPending}
            />
            <Label htmlFor="demo-wipe-first" className="text-xs leading-relaxed">
              {t("config.demoWipeLabel")}
            </Label>
          </div>
          <DialogFooter>
            <Button variant="ghost" size="sm" onClick={closeDialog} disabled={deployMutation.isPending}>
              {t("config.demoCancel")}
            </Button>
            <Button size="sm" onClick={handleConfirm} disabled={deployMutation.isPending} className="gap-1.5">
              {deployMutation.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {t("config.demoConfirm")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
```

In `settings.tsx`: delete the entire `DeployDemoCard` function (~L3023-3136) and delete the `deployDemo` entry object at L3200. Then remove any imports that are now unused **only if** nothing else in the file uses them (`FlaskConical`, `useDeployDemoContent`, `useDemoContentStatus`, `getDemoContentStatusQueryKey`, and possibly `Checkbox`/`Label` — verify each with a grep before removing). `tsc`'s `noUnusedLocals` will flag any left-over unused import, so rely on Step 4 to catch them.

- [ ] **Step 4: Run type check** — Run: `cd app && bun run tsc -b --incremental`. Expected: PASS. Fix any `noUnusedLocals` errors in `settings.tsx` by removing the now-dead imports.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/marketplace/DeployDemoRow.tsx \
        app/src/databricks_labs_dqx_app/ui/routes/_sidebar/settings.tsx
git commit -S -m "refactor(marketplace): extract DeployDemoRow and remove from settings

Co-authored-by: Isaac"
```

---

## Task 8: Client-side selection & filter helpers (+ tests)

**Files:**
- Modify: `ui/lib/marketplace-selection.ts` (add filtering/selection helpers)
- Modify: `ui/lib/marketplace-selection.test.ts`

**Interfaces:**
- Consumes: `MarketplacePackOut`, `MarketplaceRuleOut` (from `@/lib/api`).
- Produces:
  - `ruleMatchesFilters(rule, {industry, region, search}): boolean` — `industry === "all"` OR rule.industries empty OR includes industry; AND `region === "all"` OR rule.regions empty OR includes region; AND search empty OR name/description contains search (case-insensitive). Combine with AND.
  - `collectIndustries(packs): string[]` — sorted union of all rule.industries, always includes "all" first.
  - `collectRegions(packs): string[]` — sorted union, "all" first.
  - `packSelectionState(packRules, selected): "none" | "some" | "all"` — tri-state given a `Set<string>` of selected `rule_key`s.
  - `toggleRule(selected: Set<string>, key: string): Set<string>` — returns a new set.
  - `togglePack(selected: Set<string>, packRuleKeys: string[]): Set<string>` — if all selected → deselect all; else select all.
  - `selectedCheckDicts(packs, selected): Record<string, unknown>[]` — the `check` dicts of all selected rules, for import.

- [ ] **Step 1: Write the failing tests** — append to `ui/lib/marketplace-selection.test.ts`:

```typescript
import {
  ruleMatchesFilters,
  collectIndustries,
  collectRegions,
  packSelectionState,
  toggleRule,
  togglePack,
  selectedCheckDicts,
} from "./marketplace-selection";
import type { MarketplacePackOut, MarketplaceRuleOut } from "@/lib/api";

function rule(over: Partial<MarketplaceRuleOut>): MarketplaceRuleOut {
  return {
    rule_key: "p:r",
    name: "Rule",
    description: "Desc.",
    industries: [],
    regions: [],
    dimension: "Validity",
    severity: "Low",
    check: { criticality: "error", check: { function: "is_not_null", arguments: {} }, user_metadata: {} },
    ...over,
  } as MarketplaceRuleOut;
}

describe("ruleMatchesFilters", () => {
  it("general (no industry) always shows under any industry chip", () => {
    expect(ruleMatchesFilters(rule({ industries: [] }), { industry: "banking", region: "all", search: "" })).toBe(true);
  });
  it("industry chip narrows to tagged rules", () => {
    expect(ruleMatchesFilters(rule({ industries: ["retail"] }), { industry: "banking", region: "all", search: "" })).toBe(false);
    expect(ruleMatchesFilters(rule({ industries: ["banking"] }), { industry: "banking", region: "all", search: "" })).toBe(true);
  });
  it("global (no region) always shows under any region chip", () => {
    expect(ruleMatchesFilters(rule({ regions: [] }), { industry: "all", region: "uk", search: "" })).toBe(true);
  });
  it("industry AND region combine", () => {
    const r = rule({ industries: ["banking"], regions: ["eu"] });
    expect(ruleMatchesFilters(r, { industry: "banking", region: "eu", search: "" })).toBe(true);
    expect(ruleMatchesFilters(r, { industry: "banking", region: "uk", search: "" })).toBe(false);
  });
  it("search matches name or description case-insensitively", () => {
    expect(ruleMatchesFilters(rule({ name: "Valid email" }), { industry: "all", region: "all", search: "EMAIL" })).toBe(true);
    expect(ruleMatchesFilters(rule({ name: "Valid email" }), { industry: "all", region: "all", search: "phone" })).toBe(false);
  });
});

describe("collectIndustries / collectRegions", () => {
  const packs = [
    { rules: [rule({ industries: ["retail"] }), rule({ industries: ["banking"] })] },
    { rules: [rule({ regions: ["uk"] }), rule({ regions: ["eu"] })] },
  ] as unknown as MarketplacePackOut[];
  it("prepends all and unions/sorts", () => {
    expect(collectIndustries(packs)).toEqual(["all", "banking", "retail"]);
    expect(collectRegions(packs)).toEqual(["all", "eu", "uk"]);
  });
});

describe("selection", () => {
  it("tri-state reflects selected subset", () => {
    const keys = ["a", "b", "c"];
    expect(packSelectionState(keys, new Set())).toBe("none");
    expect(packSelectionState(keys, new Set(["a"]))).toBe("some");
    expect(packSelectionState(keys, new Set(["a", "b", "c"]))).toBe("all");
  });
  it("toggleRule adds/removes", () => {
    expect(toggleRule(new Set(), "a").has("a")).toBe(true);
    expect(toggleRule(new Set(["a"]), "a").has("a")).toBe(false);
  });
  it("togglePack selects all then clears", () => {
    const keys = ["a", "b"];
    const s1 = togglePack(new Set(), keys);
    expect([...s1].sort()).toEqual(["a", "b"]);
    const s2 = togglePack(s1, keys);
    expect(s2.size).toBe(0);
  });
  it("selectedCheckDicts returns check dicts of selected rules", () => {
    const packs = [{ rules: [rule({ rule_key: "p:a" }), rule({ rule_key: "p:b" })] }] as unknown as MarketplacePackOut[];
    const dicts = selectedCheckDicts(packs, new Set(["p:a"]));
    expect(dicts).toHaveLength(1);
  });
});
```

- [ ] **Step 2: Run to verify failure** — Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/marketplace-selection.test.ts`. Expected: FAIL (helpers undefined).

- [ ] **Step 3: Implement** — append to `ui/lib/marketplace-selection.ts`:

```typescript
import type { MarketplacePackOut, MarketplaceRuleOut } from "@/lib/api";

export interface MarketplaceFilters {
  industry: string; // "all" or a taxonomy value
  region: string; // "all" or a taxonomy value
  search: string;
}

export function ruleMatchesFilters(rule: MarketplaceRuleOut, f: MarketplaceFilters): boolean {
  const industryOk =
    f.industry === "all" || rule.industries.length === 0 || rule.industries.includes(f.industry);
  const regionOk = f.region === "all" || rule.regions.length === 0 || rule.regions.includes(f.region);
  const q = f.search.trim().toLowerCase();
  const searchOk =
    q === "" || rule.name.toLowerCase().includes(q) || rule.description.toLowerCase().includes(q);
  return industryOk && regionOk && searchOk;
}

function collectTag(packs: MarketplacePackOut[], pick: (r: MarketplaceRuleOut) => string[]): string[] {
  const set = new Set<string>();
  for (const p of packs) for (const r of p.rules) for (const tag of pick(r)) set.add(tag);
  return ["all", ...[...set].sort()];
}

export function collectIndustries(packs: MarketplacePackOut[]): string[] {
  return collectTag(packs, (r) => r.industries);
}

export function collectRegions(packs: MarketplacePackOut[]): string[] {
  return collectTag(packs, (r) => r.regions);
}

export function packSelectionState(
  packRuleKeys: string[],
  selected: Set<string>,
): "none" | "some" | "all" {
  const n = packRuleKeys.filter((k) => selected.has(k)).length;
  if (n === 0) return "none";
  if (n === packRuleKeys.length) return "all";
  return "some";
}

export function toggleRule(selected: Set<string>, key: string): Set<string> {
  const next = new Set(selected);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}

export function togglePack(selected: Set<string>, packRuleKeys: string[]): Set<string> {
  const next = new Set(selected);
  const allSelected = packRuleKeys.every((k) => next.has(k));
  if (allSelected) for (const k of packRuleKeys) next.delete(k);
  else for (const k of packRuleKeys) next.add(k);
  return next;
}

export function selectedCheckDicts(
  packs: MarketplacePackOut[],
  selected: Set<string>,
): Record<string, unknown>[] {
  const dicts: Record<string, unknown>[] = [];
  for (const p of packs)
    for (const r of p.rules) if (selected.has(r.rule_key)) dicts.push(r.check as Record<string, unknown>);
  return dicts;
}
```

- [ ] **Step 4: Run to verify pass** — Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/marketplace-selection.test.ts`. Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/lib/marketplace-selection.ts \
        app/src/databricks_labs_dqx_app/ui/lib/marketplace-selection.test.ts
git commit -S -m "feat(marketplace): client filtering + selection helpers

Co-authored-by: Isaac"
```

---

## Task 9: Build a synthetic RegistryRuleOut for preview (helper + test)

**Files:**
- Modify: `ui/lib/marketplace-selection.ts` (add `checkDictToPreviewRule`)
- Modify: `ui/lib/marketplace-selection.test.ts`

**Interfaces:**
- Consumes: normalized check dict (`MarketplaceRuleOut.check`), `parseDqxCheckJson` (`@/lib/registry-rule-conversion`), `CheckFunctionDef` (`@/lib/api`).
- Produces: `checkDictToPreviewRule(rule: MarketplaceRuleOut, checkFunctions: CheckFunctionDef[], t): RegistryRuleOut | undefined` — parses the normalized check into a `{mode, definition, polarity, user_metadata}` and wraps it in a minimal `RegistryRuleOut` shape (fill required fields with placeholders: `rule_id: rule.rule_key`, `status: "draft"`, `version: 1`, `display_status: "draft"`, `is_builtin: false`) so `RuleLogicDisclosure` renders it identically to Apply Rules. Returns `undefined` if parse throws.

- [ ] **Step 1: Write the failing test** — append to `ui/lib/marketplace-selection.test.ts`:

```typescript
import { checkDictToPreviewRule } from "./marketplace-selection";
import type { CheckFunctionDef } from "@/lib/api";

const T = (k: string) => k;
const FNS: CheckFunctionDef[] = [] as unknown as CheckFunctionDef[];

describe("checkDictToPreviewRule", () => {
  it("produces a RegistryRuleOut-shaped object for a native check", () => {
    const r = rule({
      rule_key: "standard-checks:must-not-be-null",
      check: {
        criticality: "error",
        check: { function: "is_not_null", arguments: { column: "{{column}}" } },
        user_metadata: { name: "Must not be null" },
      },
    });
    const preview = checkDictToPreviewRule(r, FNS, T);
    expect(preview).toBeDefined();
    expect(preview?.rule_id).toBe("standard-checks:must-not-be-null");
    expect(preview?.definition).toBeDefined();
  });
});
```

- [ ] **Step 2: Run to verify failure** — Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/marketplace-selection.test.ts -t checkDictToPreviewRule`. Expected: FAIL.

- [ ] **Step 3: Implement** — append to `ui/lib/marketplace-selection.ts`:

```typescript
import { parseDqxCheckJson } from "@/lib/registry-rule-conversion";
import type { CheckFunctionDef } from "@/lib/api";

const EMPTY_DEFINITION = { body: {}, slots: [], parameters: [] };

export function checkDictToPreviewRule(
  rule: MarketplaceRuleOut,
  checkFunctions: CheckFunctionDef[],
  t: (key: string, opts?: Record<string, unknown>) => string,
): RegistryRuleOut | undefined {
  try {
    const parsed = parseDqxCheckJson(
      JSON.stringify(rule.check),
      EMPTY_DEFINITION as never,
      {},
      checkFunctions,
      t,
    );
    return {
      rule_id: rule.rule_key,
      mode: parsed.mode,
      status: "draft",
      version: 1,
      polarity: parsed.polarity ?? null,
      author_kind: "human",
      definition: parsed.definition,
      user_metadata: parsed.userMetadata,
      is_builtin: false,
      modified_since_publish: false,
      display_status: "draft",
    } as RegistryRuleOut;
  } catch {
    return undefined;
  }
}
```

(Verify the exact field names on `ParsedCheckDefinition` — `mode`, `definition`, `polarity`, `userMetadata` — against `registry-rule-conversion.ts`; they match `parseChecksForImport`'s usage in `import-registry-rules.ts`.)

- [ ] **Step 4: Run to verify pass** — Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/marketplace-selection.test.ts` (all PASS) and `cd app && bun run tsc -b --incremental`.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/lib/marketplace-selection.ts \
        app/src/databricks_labs_dqx_app/ui/lib/marketplace-selection.test.ts
git commit -S -m "feat(marketplace): synthesize preview rule from pack check dict

Co-authored-by: Isaac"
```

---

## Task 10: PackGroup + MarketplaceRuleRow components

**Files:**
- Create: `ui/components/marketplace/MarketplaceRuleRow.tsx`
- Create: `ui/components/marketplace/PackGroup.tsx`
- Modify i18n: `marketplace.*` keys (added in Task 12 batch; use keys here and ensure they exist by Task 12).

**Interfaces:**
- Consumes: `MarketplaceRuleOut`, `MarketplacePackOut`, `CheckFunctionDef`; `RuleLogicDisclosure` (`@/components/apply-rules/RuleConfigCard`); `TagBadge`, `SeverityBadge` (`@/components/RegistryRuleBadges`); `checkDictToPreviewRule`, `packSelectionState` (Task 8/9); `Checkbox`, `FadeIn`; lucide icons dynamic by pack `icon` name.
- Produces:
  - `MarketplaceRuleRow({ rule, selected, onToggleSelect, open, onToggleOpen, checkFunctions })` — checkbox (stops propagation), name + dimension `TagBadge` + `SeverityBadge` + industry/region `TagBadge`s + description; row click (not checkbox) toggles inline `RuleLogicDisclosure` fed by `checkDictToPreviewRule`.
  - `PackGroup({ pack, selected, onToggleRule, onTogglePack, openRuleKey, onOpenRule, checkFunctions, expanded, onToggleExpanded })` — collapsible header with tri-state checkbox, dynamic lucide icon, title, "N / M selected" count, chevron; accordion: only `openRuleKey` within this pack is open.

- [ ] **Step 1..5:** Component files (presentational; no unit test per repo convention). Gate = `tsc` + Task 14 QA.

Implement `MarketplaceRuleRow.tsx`:

```tsx
import { useTranslation } from "react-i18next";
import { Checkbox } from "@/components/ui/checkbox";
import { cn } from "@/lib/utils";
import type { CheckFunctionDef, MarketplaceRuleOut } from "@/lib/api";
import { TagBadge, SeverityBadge } from "@/components/RegistryRuleBadges";
import { RuleLogicDisclosure } from "@/components/apply-rules/RuleConfigCard";
import { checkDictToPreviewRule } from "@/lib/marketplace-selection";

export function MarketplaceRuleRow({
  rule,
  selected,
  onToggleSelect,
  open,
  onToggleOpen,
  checkFunctions,
}: {
  rule: MarketplaceRuleOut;
  selected: boolean;
  onToggleSelect: () => void;
  open: boolean;
  onToggleOpen: () => void;
  checkFunctions: CheckFunctionDef[];
}) {
  const { t } = useTranslation();
  const previewRule = checkDictToPreviewRule(rule, checkFunctions, t);
  return (
    <div className={cn("rounded-md border transition-colors", selected && "border-primary/50 bg-primary/5")}>
      <div className="flex items-start gap-3 px-3 py-2">
        <Checkbox
          checked={selected}
          onCheckedChange={onToggleSelect}
          aria-label={t("marketplace.selectRule", { name: rule.name })}
          onClick={(e) => e.stopPropagation()}
          className="mt-0.5"
        />
        <button type="button" onClick={onToggleOpen} className="flex-1 text-left space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-sm font-medium">{rule.name}</span>
            <TagBadge label={rule.dimension} />
            <SeverityBadge severity={rule.severity} />
            {rule.industries.map((i) => (
              <TagBadge key={`i-${i}`} label={i} />
            ))}
            {rule.regions.map((r) => (
              <TagBadge key={`r-${r}`} label={r} />
            ))}
          </div>
          <p className="text-xs text-muted-foreground">{rule.description}</p>
        </button>
      </div>
      {open && (
        <div className="px-3 pb-3">
          <RuleLogicDisclosure open onToggle={onToggleOpen} registryRule={previewRule} />
        </div>
      )}
    </div>
  );
}
```

Implement `PackGroup.tsx` with the tri-state header (indeterminate checkbox via `checked === "indeterminate"`), dynamic lucide icon (`import * as Icons`; `const Icon = (Icons as Record<string, LucideIcon>)[pack.icon] ?? Icons.Package`), the "N / M selected" count using `packSelectionState` + count, chevron rotate, and grid-rows height transition. Accordion: pass `openRuleKey` and only render the open disclosure for the matching rule.

Gate: `cd app && bun run tsc -b --incremental` PASS; commit both files:

```bash
git add app/src/databricks_labs_dqx_app/ui/components/marketplace/MarketplaceRuleRow.tsx \
        app/src/databricks_labs_dqx_app/ui/components/marketplace/PackGroup.tsx
git commit -S -m "feat(marketplace): pack group + rule row components

Co-authored-by: Isaac"
```

---

## Task 11: MarketplacePage (toolbar, filters, demo row, import wiring)

**Files:**
- Create: `ui/components/marketplace/MarketplacePage.tsx`

**Interfaces:**
- Consumes: `useListMarketplacePacksSuspense` (Task 5), `useListCheckFunctions`, `selector`; `DeployDemoRow` (Task 7); `PackGroup` (Task 10); all `marketplace-selection` helpers; `importChecksAsRegistryDrafts` (`@/lib/import-registry-rules`); `FadeIn`; shadcn `Input`, `Button`, chip/badge components; `toast`.
- Produces: `export function MarketplacePage()` — the full page: `FadeIn` container; toolbar (search `Input` + "Import N selected" `Button` disabled at 0); two chip rows (Industry, Region) built from `collectIndustries`/`collectRegions`; `DeployDemoRow` first amber row; A–Z pack list of `PackGroup`s (auto-expand packs with search hits); selection state (`useState<Set<string>>`); import handler.

- [ ] **Step 1: Confirm baseline** — Run: `cd app && bun run tsc -b --incremental`. PASS.

- [ ] **Step 2: Implement** `MarketplacePage.tsx`. Key logic:

```tsx
const { data } = useListMarketplacePacksSuspense(selector());
const packs = data.packs;
const { data: fnResp } = useListCheckFunctions();
const checkFunctions = fnResp?.data?.functions ?? [];

const [selected, setSelected] = useState<Set<string>>(new Set());
const [filters, setFilters] = useState<MarketplaceFilters>({ industry: "all", region: "all", search: "" });
const [openRuleByPack, setOpenRuleByPack] = useState<Record<string, string | null>>({});

const visiblePacks = useMemo(
  () =>
    packs
      .map((p) => ({ ...p, rules: p.rules.filter((r) => ruleMatchesFilters(r, filters)) }))
      .filter((p) => p.rules.length > 0),
  [packs, filters],
);

const importMutation = ... // wrap importChecksAsRegistryDrafts in a useMutation or plain async handler

async function handleImport() {
  const dicts = selectedCheckDicts(packs, selected);
  const result = await importChecksAsRegistryDrafts({
    checks: dicts,
    checkFunctions,
    t,
    authorKind: "human",
    alsoSubmit: false,
  });
  if (result.failed > 0) toast.error(t("marketplace.importPartial", { saved: result.saved, reused: result.reused, failed: result.failed }));
  else toast.success(t("marketplace.importDone", { saved: result.saved, reused: result.reused }));
  setSelected(new Set());
}
```

- The Import button label uses a pluralized key: `t("marketplace.importSelected", { count: selected.size })` with `importSelected_one` / `importSelected_other`.
- Search auto-expands packs with hits: derive `expandedPacks` from `filters.search !== ""` ? all visible pack ids : user-toggled set.
- Chip rows: render each value from `collectIndustries`/`collectRegions` as a toggle button; label via `t("marketplace.industry." + value)` with a fallback to the raw value (or a generic label map). Keep it simple: display the capitalized tag; the "all" chip label is `t("marketplace.all")`.

- [ ] **Step 3: Run type check** — Run: `cd app && bun run tsc -b --incremental`. PASS (may fail on missing i18n keys only at runtime, not type-time; keys land in Task 12).

- [ ] **Step 4: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/marketplace/MarketplacePage.tsx
git commit -S -m "feat(marketplace): marketplace page with filters + import wiring

Co-authored-by: Isaac"
```

---

## Task 12: Marketplace i18n keys (4 locales)

**Files:**
- Modify: `ui/lib/i18n/locales/{en,pt-BR,it,es}.json`

**Interfaces:**
- Produces: a `marketplace` namespace object in each locale containing every key referenced by Tasks 7, 10, 11, 13. Minimum key set (en values shown; translate for others):
  - `marketplace.title` = "Marketplace"
  - `marketplace.subtitle` = "Browse curated packs of reusable data-quality rules and import them into your registry."
  - `marketplace.searchPlaceholder` = "Search rules by name or description"
  - `marketplace.all` = "All"
  - `marketplace.importSelected_one` = "Import {{count}} selected"
  - `marketplace.importSelected_other` = "Import {{count}} selected"
  - `marketplace.industryLabel` = "Industry"
  - `marketplace.regionLabel` = "Region"
  - `marketplace.selectRule` = "Select {{name}}"
  - `marketplace.selectPack` = "Select all rules in {{title}}"
  - `marketplace.packSelectedCount` = "{{selected}} / {{total}} selected"
  - `marketplace.importDone` = "Imported {{saved}} rule(s); {{reused}} already existed."
  - `marketplace.importPartial` = "Imported {{saved}}; {{reused}} reused; {{failed}} failed."
  - `marketplace.empty` = "No rules match the current filters."
  - (Reuse existing `config.demo*` keys for the demo dialog — no new demo keys required.)

- [ ] **Step 1: Add keys to `en.json`.** Insert a `"marketplace": { ... }` object (alphabetical placement near other top-level namespaces).

- [ ] **Step 2: Add the same keys with translated values to `pt-BR.json`, `it.json`, `es.json`.** Translate each value; do not leave English behind.

- [ ] **Step 3: Verify key parity** — Run:
```bash
cd app/src/databricks_labs_dqx_app/ui/lib/i18n/locales && \
for f in en pt-BR it es; do node -e "const o=require('./$f.json'); console.log('$f', Object.keys(o.marketplace||{}).length)"; done
```
Expected: identical counts across all four files.

- [ ] **Step 4: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/en.json \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/pt-BR.json \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/it.json \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/es.json
git commit -S -m "i18n(marketplace): add marketplace namespace in all 4 locales

Co-authored-by: Isaac"
```

---

## Task 13: Marketplace route (admin-gated) + sidebar item

**Files:**
- Create: `ui/routes/_sidebar/marketplace.tsx`
- Modify: `ui/routes/_sidebar/route.tsx`
- Regenerated: `ui/types/routeTree.gen.ts`

**Interfaces:**
- Consumes: `MarketplacePage` (Task 11), `usePermissions`, `useNavigate`; sidebar primitives.
- Produces: route `/marketplace` (admin-only, non-admins redirected to `/rules/active`), and a "Marketplace" sidebar item (icon `Store`) above Documentation, gated on `isAdmin`.

- [ ] **Step 1: Implement the route** — create `ui/routes/_sidebar/marketplace.tsx`:

```tsx
import { useEffect, Suspense } from "react";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { Skeleton } from "@/components/ui/skeleton";
import { usePermissions } from "@/hooks/use-permissions";
import { MarketplacePage } from "@/components/marketplace/MarketplacePage";

export const Route = createFileRoute("/_sidebar/marketplace")({
  component: () => <MarketplaceRoute />,
});

function MarketplaceRoute() {
  const { isAdmin } = usePermissions();
  const navigate = useNavigate();
  useEffect(() => {
    if (!isAdmin) navigate({ to: "/rules/active", replace: true });
  }, [isAdmin, navigate]);
  if (!isAdmin) return null;
  return (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} fallbackRender={() => null}>
          <Suspense fallback={<Skeleton className="h-96 w-full" />}>
            <MarketplacePage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  );
}
```

- [ ] **Step 2: Add the sidebar item** — in `route.tsx`, add `Store` to the lucide import, and insert a `<SidebarMenuItem>` inside the bottom `SidebarGroup`'s `SidebarMenu` (route.tsx:206) **above** the Documentation item, gated on `isAdmin`:

```tsx
// near top: const { isAdmin } = usePermissions();
{isAdmin && (
  <SidebarMenuItem>
    <SidebarMenuButton
      asChild
      isActive={location.pathname.startsWith("/marketplace")}
      tooltip={t("sidebar.marketplace")}
    >
      <Link to="/marketplace">
        <Store />
        <span>{t("sidebar.marketplace")}</span>
      </Link>
    </SidebarMenuButton>
  </SidebarMenuItem>
)}
```

Add `import { usePermissions } from "@/hooks/use-permissions";` and `Store` to the `lucide-react` import. Add `sidebar.marketplace` = "Marketplace" to all 4 locales (place in the existing `sidebar` namespace; translate).

- [ ] **Step 3: Regenerate route tree + type-check** — Run: `make app-regen-api` is not needed here; instead run `cd app && bun run tsc -b --incremental` (fails until routeTree regenerates). Trigger route-tree regeneration by running `make app-build` OR restarting `make app-start-dev`. Then re-run `cd app && bun run tsc -b --incremental`. Expected: PASS, `routeTree.gen.ts` includes `/marketplace`.

- [ ] **Step 4: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/routes/_sidebar/marketplace.tsx \
        app/src/databricks_labs_dqx_app/ui/routes/_sidebar/route.tsx \
        app/src/databricks_labs_dqx_app/ui/types/routeTree.gen.ts \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json
git commit -S -m "feat(marketplace): admin-gated route + sidebar item

Co-authored-by: Isaac"
```

---

## Task 14: Full check gates (app-check + app-test)

**Files:** none (verification only).

- [ ] **Step 1: Run backend tests** — Run: `make app-test`. Expected: all pass, including the three `test_marketplace_*` files.

- [ ] **Step 2: Run app-check** — Run: `make app-check`. Expected: `tsc -b` clean, `basedpyright --level error` clean, `bun test` all green (including `marketplace-selection.test.ts`).

- [ ] **Step 3: Fix anything red**, then re-run both. Do not suppress lints.

- [ ] **Step 4: Commit** (only if fixes were made) — group fixes into a single commit with the standard trailer.

---

## Task 15: What's-new entry

**Files:**
- Modify: `docs/dqx/docs/studio/whats-new/index.mdx`

- [ ] **Step 1: Add an entry** under an appropriate section (e.g. a new "Marketplace" heading or under Authoring):

```mdx
## Rules Marketplace

- **[Rules Marketplace](/docs/studio/).** Admins can browse curated packs of
  ready-made data-quality rules — grouped by domain and tagged by industry and
  region — pick the ones they want across packs, and import them into the Rules
  Registry as reusable templates in one click. The Deploy-demo-content action
  now lives here too.
- **Rule polarity in the rule-logic view.** The read-only rule-logic disclosure
  now states whether a matching condition means the rule passes or fails, so a
  rule's intent is unambiguous wherever it's previewed.
```

- [ ] **Step 2: Commit**

```bash
git add docs/dqx/docs/studio/whats-new/index.mdx
git commit -S -m "docs(marketplace): add whats-new entry

Co-authored-by: Isaac"
```

---

## Task 16: Manual QA on sandbox (English-only)

**Files:** none.

- [ ] **Step 1:** Deploy to the sandbox per project deploy conventions (real-exit-code capture; do not stage `uv.lock`).
- [ ] **Step 2:** As an admin, confirm: sidebar shows "Marketplace" above Documentation; page loads with amber demo row first, then A–Z packs; Industry/Region chips derived from tags narrow rules (general/global always show); search auto-expands hit packs; rule row expands to show rule logic incl. the polarity line; tri-state pack checkbox works; "Import N selected" disabled at 0 and reflects count; import toasts saved/reused; re-import dedupes (reused > 0). As a non-admin, confirm the sidebar item is hidden and `/marketplace` redirects to `/rules/active`. Confirm Settings no longer shows a Deploy-demo card.
- [ ] **Step 3:** Note any issues; fix in follow-up tasks if found.

---

## Self-review notes (gaps to watch during execution)

- **`for_each_column` import loss:** the "Full name present" rule is deliberately authored as `sql_expression` (not `is_not_null_and_not_empty` + `for_each_column`) because `normalizeImportedCheck` drops `for_each_column`. Do not "fix" it back to `for_each_column` — it would silently import an argument-less check.
- **`is_data_fresh` scalar:** `max_age_minutes` MUST be a literal int (1440) in the YAML; a `{{slot}}` fails `validate_checks`. Verified empirically.
- **`sql_expression` polarity:** expressions must be true-when-good (DQX fails on false). Structuring/threshold rules are written with `not (...)`.
- **Preview field names:** confirm `ParsedCheckDefinition` exposes `mode`/`definition`/`polarity`/`userMetadata` exactly (matches `parseChecksForImport`). If `RegistryRuleOut` gains required fields, `checkDictToPreviewRule` must fill them or `tsc` will fail — fill with the placeholders shown.
- **Test ordering:** Task 2's `test_load_packs_returns_sorted_nonempty` needs at least one real pack on disk; run Task 3 (or a stub pack) before asserting non-empty. The `tmp_path` malformed-skip test is order-independent.
- **Route-tree staleness:** after adding `marketplace.tsx`, regenerate `routeTree.gen.ts` via `make app-build`/dev-server restart, or the route silently 404s.
- **i18n parity:** every new `marketplace.*` and `sidebar.marketplace` and `monitoredTables.ruleLogicThen*` key must exist in all four locales (Task 6 + Task 12 + Task 13).

---

## Execution Handoff

**Plan complete. Save it to `docs/superpowers/plans/2026-07-31-rules-marketplace.md`. Two execution options:**

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration (superpowers:subagent-driven-development).
2. **Inline Execution** — execute tasks in this session with checkpoints (superpowers:executing-plans).

**Which approach?**

---

### One-paragraph summary

The plan delivers the admin-only Rules Marketplace in 16 bite-sized, TDD-structured tasks: (1-4) a backend `marketplace` package — Pydantic models, a cached YAML loader that validates every rule through `DQEngine.validate_checks`, the full 7-pack / ~59-rule catalogue authored verbatim from the spec (Pricing & Money and Standard checks given as complete YAML; the other five enumerated rule-by-rule with exact functions and regexes), and an admin-gated `GET /api/v1/marketplace/packs` router registered in `routes/v1/__init__.py`; (5) api.ts regeneration; (6) the shared `RuleLogicDisclosure` polarity fix ("THEN THE RULE PASSES/FAILS") with a pure-helper regression test and 4-locale keys; (7) extraction of the demo action into `DeployDemoRow` with removal of the settings `deployDemo` entry; (8-13) frontend selection/filter/preview helpers (unit-tested), `PackGroup`/`MarketplaceRuleRow`/`MarketplacePage` components reusing the existing `importChecksAsRegistryDrafts` → batch-import path, i18n, and an admin-gated route plus a `Store` sidebar item above Documentation; (14-16) full `make app-check`/`app-test` gates, the What's-new entry, and sandbox QA. Exploration confirmed the exact model fields, the `importChecksAsRegistryDrafts` signature, the `RuleLogicBody` code to modify, and two load-bearing validation facts baked into the catalogue design (scalar ints like `is_data_fresh.max_age_minutes` cannot be slots; `for_each_column` is dropped by `normalizeImportedCheck`, so the full-name rule uses `sql_expression`). Since I am in read-only mode with no file-writing tool, the plan document above must be saved by the parent agent to the target path.
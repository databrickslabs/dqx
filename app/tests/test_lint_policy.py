"""Locks down project-wide lint policy decisions.

These aren't *behaviour* tests — they're *convention* tests that
prevent the codebase from quietly drifting away from policies that
were decided on review (and documented in ``pyproject.toml``).

Currently locked down:

* **BLE001** (broad-except) — the project does NOT enforce BLE001
  because several backend surfaces require best-effort resilience
  (lifespan teardown, background-thread refresh loops, route-level
  per-item recovery). The documented policy lives in
  ``[tool.ruff.lint]`` in ``pyproject.toml`` with mandatory inline-
  comment justification at each site.

  This test guards both halves of that contract: the policy block
  exists in ``pyproject.toml`` AND no source file has re-introduced
  the rejected per-line ``# noqa: BLE001`` pattern. Per AGENTS.md
  rule #6 ("Never disable linting to silence issues"), the
  per-line escape hatch is the wrong place for this — fix the code
  or document the exemption project-wide.

* **reportCallIssue / reportArgumentType on psycopg ``execute``** —
  psycopg's PEP-675 stubs require :meth:`Cursor.execute`'s ``query``
  argument to be a :class:`typing.LiteralString`. Backend code that
  composes SQL programmatically (validated identifiers, escaped
  values) used to silence basedpyright at every call site with
  ``# pyright: ignore[reportCallIssue, reportArgumentType]``. That
  also violates AGENTS.md rule #6 — the documented fix is the
  ``run_trusted_sql`` helper in :mod:`backend.pg_executor`, which
  performs the cast once in a single auditable wrapper. This test
  pins the convention.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
TESTS_ROOT = REPO_ROOT / "tests"
PYPROJECT = REPO_ROOT / "pyproject.toml"
TASKS_PYPROJECT = REPO_ROOT / "tasks" / "pyproject.toml"

# Matches a ``databricks-labs-dqx`` requirement string (with or without
# an ``[extras]`` group) that carries an exact ``==`` pin, e.g.
# ``databricks-labs-dqx[llm,datacontract]==0.15.0``. Anchored at the
# start so it only matches the requirement for this exact package, and
# the version is captured up to the first whitespace / marker (``;``).
_DQX_EXACT_PIN = re.compile(
    r"^databricks-labs-dqx(?:\[[^\]]*\])?==(?P<version>[^\s;]+)\s*$",
)

# Matches the actual silencing pattern: an ``except`` clause carrying
# an inline noqa directive for the broad-except rule (with or without
# whitespace after the colon, with or without an exception alias).
# Deliberately anchored to the ``except`` keyword so docstrings,
# error messages, and *this very file's* policy-text references
# don't cause false positives. The regex literal below is the
# canonical form callers should look for.
_BLE_NOQA_ON_EXCEPT = re.compile(r"^\s*except\b.*#\s*noqa\s*:\s*BLE001\b")

# Matches the actual silencing pattern for psycopg's PEP-675
# constraint on ``Cursor.execute``: a line that calls ``.execute(``
# AND carries an inline pyright-ignore for ``reportCallIssue`` or
# ``reportArgumentType``. Anchored on ``.execute(`` so generic
# pyright-ignore directives elsewhere in the codebase aren't false-
# positive hits.
_EXECUTE_PYRIGHT_IGNORE = re.compile(
    r"\.execute\b.*#\s*pyright:\s*ignore\[[^\]]*" r"(?:reportCallIssue|reportArgumentType)"
)


class TestBleNoqaPolicy:
    """Per-line ``# noqa: BLE001`` is banned by the documented policy."""

    @pytest.mark.parametrize(
        "root,description",
        [
            (SRC_ROOT, "src/"),
            (TESTS_ROOT, "tests/"),
        ],
    )
    def test_no_per_line_noqa_ble001_in_source_or_tests(self, root, description):
        """Any ``except ...: # noqa: BLE001`` must be replaced by a
        substantive inline comment + the documented project-wide
        policy in ``pyproject.toml``.

        Adding one in source/tests means either (a) the code needs a
        substantive comment explaining the resilience contract
        instead, or (b) BLE001 needs to be enabled with a documented
        per-file ignore. Per-line silencing is rejected by both the
        reviewer comment that created this policy and by AGENTS.md
        rule #6.
        """
        offenders: list[tuple[Path, int, str]] = []
        for py in root.rglob("*.py"):
            # Skip any vendor/dist trees if a future build places .py
            # files under them. The src layout shouldn't ship any, but
            # the rglob is permissive.
            if any(part in {".venv", "__pycache__", "__dist__"} for part in py.parts):
                continue
            text = py.read_text(encoding="utf-8")
            for lineno, line in enumerate(text.splitlines(), start=1):
                if _BLE_NOQA_ON_EXCEPT.match(line):
                    offenders.append((py.relative_to(REPO_ROOT), lineno, line.strip()))

        assert not offenders, (
            f"Per-line BLE001 silencing re-introduced on except clauses in {description}. "
            f"Per the policy block in pyproject.toml, the broad-except "
            f"resilience pattern is documented project-wide; individual "
            f"sites must NOT silence the rule per-line. Offenders:\n"
            + "\n".join(f"  {p}:{n}  {snippet}" for p, n, snippet in offenders)
        )

    def test_pyproject_has_documented_ble001_policy_block(self):
        """The single source of truth for the broad-except policy.

        If someone removes the policy block from ``pyproject.toml``
        AND the codebase still has broad excepts, future contributors
        have no documented rationale for the pattern and the next
        reviewer will (correctly) flag it again. Keep the policy
        comment and the code aligned.
        """
        text = PYPROJECT.read_text(encoding="utf-8")
        assert "[tool.ruff.lint]" in text, (
            "pyproject.toml must declare a ``[tool.ruff.lint]`` block to anchor the documented BLE001 policy."
        )
        assert "BLE001" in text, (
            "pyproject.toml must document the project's BLE001 policy "
            "(it's the single source of truth that replaces the "
            "per-line ``# noqa: BLE001`` comments)."
        )
        # And the policy must explicitly forbid the per-line form so a
        # future reader looking at the policy block immediately sees
        # the prohibition rather than having to grep AGENTS.md.
        assert "noqa: BLE001" in text or "noqa:BLE001" in text, (
            "The pyproject.toml policy block must mention the rejected "
            "``# noqa: BLE001`` pattern explicitly so contributors see "
            "the prohibition in the same place as the policy."
        )


class TestPsycopgExecutePyrightIgnorePolicy:
    """``Cursor.execute(sql)`` must route through the trust-boundary helper."""

    @pytest.mark.parametrize(
        "root,description",
        [
            (SRC_ROOT, "src/"),
            (TESTS_ROOT, "tests/"),
        ],
    )
    def test_no_per_call_pyright_ignore_on_cursor_execute(self, root, description):
        """Forbid inline pyright-ignore on cursor execute calls.

        The rejected pattern is a cursor execute call that suppresses
        ``reportCallIssue`` or ``reportArgumentType`` on the same
        line. The replacement is
        :func:`backend.pg_executor.run_trusted_sql`, which performs
        the ``cast(LiteralString, sql)`` once and documents the
        trust boundary in one place.

        If you find yourself wanting to suppress the warning at a
        new call site, route through ``run_trusted_sql`` instead.
        If the SQL string contains untrusted input, parameterise
        the query (pass values via the ``params`` arg) rather than
        widening the helper's trust contract.
        """
        offenders: list[tuple[Path, int, str]] = []
        for py in root.rglob("*.py"):
            if any(part in {".venv", "__pycache__", "__dist__"} for part in py.parts):
                continue
            text = py.read_text(encoding="utf-8")
            for lineno, line in enumerate(text.splitlines(), start=1):
                if _EXECUTE_PYRIGHT_IGNORE.search(line):
                    offenders.append((py.relative_to(REPO_ROOT), lineno, line.strip()))

        assert not offenders, (
            f"Per-call pyright-ignore on ``.execute(...)`` re-introduced in {description}. "
            f"Route the call through ``run_trusted_sql`` in pg_executor.py "
            f"so the LiteralString cast lives in one auditable wrapper "
            f"rather than scattered across call sites. Offenders:\n"
            + "\n".join(f"  {p}:{n}  {snippet}" for p, n, snippet in offenders)
        )

    def test_run_trusted_sql_helper_is_importable_and_documented(self):
        """The helper that replaces the per-call ignores must exist
        and carry the trust-boundary docstring. If it gets renamed
        or deleted, this test fails loud rather than letting the
        per-call pattern silently creep back in.

        The helper now lives in :mod:`backend.pg_cursor_helpers` (a
        psycopg-free module — see that module's docstring), but is
        re-exported by :mod:`backend.pg_executor` for back-compat.
        Both import paths are pinned so a refactor that drops either
        re-export breaks loudly here.
        """
        from databricks_labs_dqx_app.backend.pg_cursor_helpers import (
            run_trusted_sql as run_trusted_sql_canonical,
        )
        from databricks_labs_dqx_app.backend.pg_executor import (
            run_trusted_sql as run_trusted_sql_reexport,
        )

        assert run_trusted_sql_canonical is run_trusted_sql_reexport, (
            "``backend.pg_executor.run_trusted_sql`` must be the SAME "
            "object as ``backend.pg_cursor_helpers.run_trusted_sql`` — "
            "a divergence means the re-export drifted from the canonical "
            "definition and callers will silently use the wrong version."
        )
        run_trusted_sql = run_trusted_sql_canonical
        assert callable(run_trusted_sql), "run_trusted_sql must be a callable wrapper"
        doc = (run_trusted_sql.__doc__ or "").lower()
        # The docstring is the trust-boundary contract — these tokens
        # are the load-bearing words. If any drift out, the next
        # reviewer can't tell why ``cast(LiteralString, ...)`` is
        # safe at the call sites.
        for token in ("literalstring", "trust", "untrusted"):
            assert token in doc, (
                f"run_trusted_sql.__doc__ must mention {token!r} "
                "so the trust boundary is explicit in the helper "
                "rather than implicit in the call sites."
            )
        # The narrowed contract (post-review) must explicitly point
        # callers with RUNTIME values at the parameterised sibling
        # helper / psycopg's native binding — otherwise a future
        # reader sees only the LiteralString cast and assumes the
        # helper sanitises everything.
        assert "run_parameterized_sql" in doc or "cur.execute(sql, params)" in doc, (
            "run_trusted_sql.__doc__ must point runtime-value callers "
            "at ``run_parameterized_sql`` (or psycopg's native "
            "``cur.execute(sql, params)`` binding) so the cast is not "
            "mistaken for a value sanitiser."
        )

    def test_run_parameterized_sql_helper_is_importable_and_documented(self):
        """Sibling of :func:`run_trusted_sql` for the "trusted
        template + runtime values" case. It centralises the same
        ``cast(LiteralString, ...)`` discipline (so call sites do
        NOT need ``# pyright: ignore``) while binding values through
        psycopg's native parameter binding so f-string value
        interpolation is structurally impossible at the call site.

        Same dual-import pin as :func:`run_trusted_sql`: the canonical
        definition lives in :mod:`backend.pg_cursor_helpers` and the
        executor re-exports it. Both paths and identity-equality are
        pinned."""
        from databricks_labs_dqx_app.backend.pg_cursor_helpers import (
            run_parameterized_sql as run_parameterized_sql_canonical,
        )
        from databricks_labs_dqx_app.backend.pg_executor import (
            run_parameterized_sql as run_parameterized_sql_reexport,
        )

        assert run_parameterized_sql_canonical is run_parameterized_sql_reexport, (
            "``backend.pg_executor.run_parameterized_sql`` must be the "
            "SAME object as the canonical definition in "
            "``backend.pg_cursor_helpers`` — re-export drift means "
            "callers silently bind through the wrong version."
        )
        run_parameterized_sql = run_parameterized_sql_canonical
        assert callable(run_parameterized_sql), "run_parameterized_sql must be a callable wrapper"
        doc = (run_parameterized_sql.__doc__ or "").lower()
        # Load-bearing tokens — explain the two halves of the
        # contract (the template is trusted like in run_trusted_sql,
        # the values go through psycopg's binder).
        for token in ("literalstring", "trusted", "psycopg", "binding"):
            assert token in doc, (
                f"run_parameterized_sql.__doc__ must mention {token!r} "
                "so the trust contract (trusted template + bound values) "
                "is explicit in the helper."
            )

    def test_helper_bodies_are_the_only_raw_cursor_execute_sites(self):
        """The two trust-boundary helper bodies in
        :mod:`backend.pg_cursor_helpers` are the ONLY places in the
        backend that may call ``cur.execute(...)`` directly. Every
        other caller — pg_executor's own methods, the migration
        runner, anything new — must route through the helpers so
        the ``cast(LiteralString, ...)`` happens in exactly one
        auditable place.

        This is stricter than the previous form of the test (which
        only checked ``pg_executor.py``): after the helpers moved
        to their own psycopg-free module the executor file now
        contains ZERO raw ``cur.execute`` calls, and the two
        canonical raw calls live in ``pg_cursor_helpers.py``.
        """
        backend = SRC_ROOT / "databricks_labs_dqx_app" / "backend"
        helpers_text = (backend / "pg_cursor_helpers.py").read_text(encoding="utf-8")
        executor_text = (backend / "pg_executor.py").read_text(encoding="utf-8")

        # The two helper bodies are the only legitimate raw call
        # sites. Counting matches (rather than substring) keeps a
        # comment that happens to mention ``cur.execute(`` from
        # blowing the test up.
        raw_in_helpers = sum(1 for line in helpers_text.splitlines() if re.match(r"^\s*(_ = )?cur\.execute\(", line))
        raw_in_executor = sum(1 for line in executor_text.splitlines() if re.match(r"^\s*(_ = )?cur\.execute\(", line))

        assert raw_in_helpers == 2, (
            f"Expected exactly 2 raw ``cur.execute(...)`` in "
            f"pg_cursor_helpers.py (the bodies of ``run_trusted_sql`` "
            f"and ``run_parameterized_sql``), found {raw_in_helpers}. "
            "If you added a third helper, update the policy test "
            "deliberately rather than letting the count drift."
        )
        assert raw_in_executor == 0, (
            f"Expected ZERO raw ``cur.execute(...)`` in pg_executor.py — "
            f"every cursor call must route through ``run_trusted_sql`` / "
            f"``run_parameterized_sql`` from ``backend.pg_cursor_helpers``. "
            f"Found {raw_in_executor}."
        )

        # The executor must still USE the helpers — at minimum on its
        # ``execute``, ``query``, and ``query_dicts`` data paths.
        # Counting helper invocations protects against a refactor
        # that silently routes a method through ``conn.execute`` or
        # ``conn.cursor().execute`` directly.
        helper_calls = executor_text.count("run_trusted_sql(cur,")
        assert helper_calls >= 3, (
            f"Expected at least 3 ``run_trusted_sql(cur, ...)`` call sites in "
            f"pg_executor.py (execute, query, query_dicts), found {helper_calls}."
        )

    def test_pg_cursor_helpers_is_psycopg_free_at_runtime(self):
        """The whole point of splitting :mod:`backend.pg_cursor_helpers`
        out of :mod:`backend.pg_executor` was to give consumers (notably
        :mod:`backend.migrations.postgres`) a way to import the trust-
        boundary helpers WITHOUT transitively pulling in :mod:`psycopg`.

        That property breaks the moment someone moves the ``Cursor``
        import out of the ``TYPE_CHECKING`` guard. This test pins both
        halves of the contract — the file must (a) import ``Cursor``
        from psycopg ONLY under the guard and (b) carry zero
        unconditional ``import psycopg`` / ``from psycopg import …``
        statements.

        If a future change actually needs a runtime psycopg dependency
        in this module, the helpers' purpose has changed and the
        caller graph (especially ``migrations.postgres``) needs to be
        re-audited; bumping this test deliberately at that point is
        the intended remediation.
        """
        backend = SRC_ROOT / "databricks_labs_dqx_app" / "backend"
        text = (backend / "pg_cursor_helpers.py").read_text(encoding="utf-8")

        # Walk lines, tracking whether we are inside the
        # ``if TYPE_CHECKING:`` block. Any ``import psycopg`` /
        # ``from psycopg import …`` line OUTSIDE that block breaks
        # the runtime-free promise.
        in_type_checking_block = False
        offenders: list[tuple[int, str]] = []
        # A new top-level statement (non-indented, non-blank, non-comment)
        # exits the ``if TYPE_CHECKING:`` indented block.
        for lineno, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("if TYPE_CHECKING") and stripped.endswith(":"):
                in_type_checking_block = True
                continue
            if in_type_checking_block:
                # Indented or blank/comment lines stay inside the guard;
                # a non-indented, non-blank, non-comment line leaves it.
                if line and not line.startswith((" ", "\t")) and not stripped.startswith("#"):
                    in_type_checking_block = False
            if in_type_checking_block:
                continue
            if re.match(r"^\s*(import\s+psycopg|from\s+psycopg(\s|\.))", line):
                offenders.append((lineno, stripped))

        assert not offenders, (
            "pg_cursor_helpers.py imported psycopg outside the "
            "``if TYPE_CHECKING:`` guard. That re-couples every "
            "caller of these helpers (including ``migrations.postgres`` "
            "and any future psycopg-free consumer) to psycopg, "
            "undoing the whole point of the split. Offenders:\n"
            + "\n".join(f"  line {n}: {snippet}" for n, snippet in offenders)
        )

        # And, defensively, confirm the guard exists at all — a
        # refactor that drops it entirely would still pass the
        # negative check above (no psycopg imports anywhere) but
        # would mean the module no longer carries the type hints
        # the call-sites rely on.
        assert "if TYPE_CHECKING:" in text, (
            "pg_cursor_helpers.py is expected to import ``Cursor`` "
            "for type hints under an ``if TYPE_CHECKING:`` guard. "
            "Dropping the guard means losing the type surface the "
            "helpers' signatures are written against."
        )

    def test_migrations_postgres_does_not_transitively_import_psycopg(self):
        """Companion of the previous test, from the consumer's side.

        :mod:`backend.migrations.postgres` MUST be importable in an
        environment that does NOT have :mod:`psycopg` installed.
        Before the helper split, importing it would crash with
        ``ModuleNotFoundError: psycopg`` because
        ``from ..pg_executor import run_trusted_sql`` pulled in
        psycopg via the executor's top-level imports.

        We can't simulate "psycopg is not installed" cleanly in
        unit tests, so we assert the structural precondition: the
        runner imports the helpers from :mod:`pg_cursor_helpers`
        (which is psycopg-free per the test above), not from
        :mod:`pg_executor`. This catches a regression as soon as
        anyone re-routes the import without realising the cost.
        """
        text = (SRC_ROOT / "databricks_labs_dqx_app" / "backend" / "migrations" / "postgres.py").read_text(
            encoding="utf-8"
        )

        # The forbidden pattern: importing the trust-boundary
        # helpers from ``pg_executor`` instead of ``pg_cursor_helpers``.
        # Matches both relative (``..pg_executor``) and absolute
        # (``databricks_labs_dqx_app.backend.pg_executor``) forms.
        forbidden = re.compile(
            r"^\s*from\s+(?:\.\.|databricks_labs_dqx_app\.backend\.)pg_executor\s+import\s+.*\b"
            r"(run_trusted_sql|run_parameterized_sql)\b"
        )
        offenders = [(n, line) for n, line in enumerate(text.splitlines(), 1) if forbidden.match(line)]
        assert not offenders, (
            "migrations/postgres.py imports a trust-boundary helper "
            "from ``backend.pg_executor`` — that re-couples the "
            "migration runner to ``psycopg`` and resurrects the lazy-"
            "import dance in ``app.py``. Import from "
            "``backend.pg_cursor_helpers`` instead. Offenders:\n"
            + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
        )

        # And confirm the migration runner DOES use the canonical
        # psycopg-free import path — symmetric assertion so a
        # refactor that deletes the helpers from the runner entirely
        # (replacing with raw cursor calls) fails loudly here.
        expected = re.compile(
            r"^\s*from\s+(?:\.\.|databricks_labs_dqx_app\.backend\.)pg_cursor_helpers\s+import\s+.*\b"
            r"(run_trusted_sql|run_parameterized_sql)\b"
        )
        assert any(expected.match(line) for line in text.splitlines()), (
            "migrations/postgres.py is expected to import the trust-"
            "boundary helpers from ``backend.pg_cursor_helpers``. "
            "If the runner stopped using them, route every cursor "
            "call through the helpers instead of dropping them."
        )


class TestDqxVersionLockstep:
    """The app and its task-runner must pin the SAME ``databricks-labs-dqx``.

    The app (``app/pyproject.toml``) authors and dry-runs rules; the
    serverless task-runner (``app/tasks/pyproject.toml``) executes the
    scheduled / async jobs against Databricks compute. Both pin the DQX
    engine from the registry with an exact ``==`` version, and each file
    carries a "Keep in lockstep with the pin in <the other>" comment.

    Nothing else enforces that coupling. A partial bump (e.g. moving the
    app to a new engine release but forgetting the task-runner) would
    silently ship a task-runner that resolves a *different* engine
    version than the one that authored the rules — the classic "passes
    in dry-run, behaves differently in the scheduled run" drift, since
    rule serialization / check semantics can change between engine
    versions. This test makes that mismatch fail at ``make app-test``
    instead of in production.
    """

    @staticmethod
    def _dqx_pin(pyproject_path: Path) -> str:
        """Return the exact ``==`` version the file pins DQX at.

        Fails the test (not merely returns ``None``) if the requirement
        is missing or isn't an exact pin — a range / unpinned dep would
        defeat the lockstep guarantee just as surely as a mismatch.
        """
        with pyproject_path.open("rb") as f:
            deps = tomllib.load(f).get("project", {}).get("dependencies", [])
        for dep in deps:
            match = _DQX_EXACT_PIN.match(dep.strip())
            if match:
                return match.group("version")
        raise AssertionError(
            f"{pyproject_path.relative_to(REPO_ROOT.parent)} has no exact "
            "``databricks-labs-dqx==<version>`` pin in [project].dependencies. "
            "The app and task-runner must both pin the engine to the SAME "
            "exact version (see the lockstep comments in both files); a "
            "range or unpinned dep breaks that guarantee."
        )

    def test_app_and_task_runner_pin_the_same_dqx_version(self):
        app_pin = self._dqx_pin(PYPROJECT)
        task_pin = self._dqx_pin(TASKS_PYPROJECT)
        assert app_pin == task_pin, (
            "``databricks-labs-dqx`` is pinned to different versions in the "
            f"app ({app_pin}, app/pyproject.toml) and the task-runner "
            f"({task_pin}, app/tasks/pyproject.toml). They MUST match — the "
            "task-runner executes the rules the app authors, so a version "
            "skew means the engine that validates rules at author-time "
            "differs from the one that runs them. Bump both pins together "
            "(that's what the 'Keep in lockstep' comments in both files mean)."
        )

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
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
TESTS_ROOT = REPO_ROOT / "tests"
PYPROJECT = REPO_ROOT / "pyproject.toml"

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
            "pyproject.toml must declare a ``[tool.ruff.lint]`` block " "to anchor the documented BLE001 policy."
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
        per-call pattern silently creep back in."""
        from databricks_labs_dqx_app.backend.pg_executor import run_trusted_sql

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

    def test_pg_executor_call_sites_use_helper_not_raw_execute(self):
        """Internal coverage: pg_executor.py's own ``execute``,
        ``query``, ``query_dicts`` methods must route through
        ``run_trusted_sql`` so the trust-boundary discipline starts
        at the file that owns the helper. If a future change adds
        a raw ``cur.execute(sql)`` here, the convention is already
        breaking down."""
        text = (SRC_ROOT / "databricks_labs_dqx_app" / "backend" / "pg_executor.py").read_text(encoding="utf-8")
        # The helper body itself is the one legitimate ``cur.execute``
        # site — every other call must use ``run_trusted_sql``.
        # Counting matches is more robust than substring checks
        # because someone could add a comment containing the literal.
        raw_execute_calls = sum(1 for line in text.splitlines() if re.match(r"^\s*(_ = )?cur\.execute\(", line))
        helper_calls = text.count("run_trusted_sql(cur,")
        assert raw_execute_calls == 1, (
            f"Expected exactly 1 raw ``cur.execute(...)`` in pg_executor.py "
            f"(the body of ``run_trusted_sql`` itself), found {raw_execute_calls}. "
            "New call sites must route through ``run_trusted_sql``."
        )
        assert helper_calls >= 3, (
            f"Expected at least 3 ``run_trusted_sql(cur, ...)`` call sites in "
            f"pg_executor.py (execute, query, query_dicts), found {helper_calls}."
        )

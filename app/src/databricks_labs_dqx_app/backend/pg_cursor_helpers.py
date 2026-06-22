"""Trust-boundary wrappers around :meth:`psycopg.Cursor.execute`.

Both helpers in this module exist for **one** reason: psycopg's PEP-675
stubs require :meth:`Cursor.execute`'s ``query`` argument to be a
:class:`typing.LiteralString`. Code that legitimately composes SQL
(validated identifiers, vendored DDL templates, parameter-binding
placeholders) cannot satisfy that constraint without a ``cast``.
Centralising the cast in named helpers — rather than sprinkling
``# pyright: ignore[reportCallIssue, reportArgumentType]`` at every
call site — is the documented project policy (AGENTS.md rule #6 + the
``TestPsycopgExecutePyrightIgnorePolicy`` test).

Why this lives in a separate module from :mod:`backend.pg_executor`
------------------------------------------------------------------
Per the reviewer note on the original ``app.py`` lazy-import comment:
``migrations.postgres`` only needs these helpers, but importing them
from :mod:`backend.pg_executor` transitively imports :mod:`psycopg`
(which the Delta-only test environment does not install). That made
the lazy-import dance in :func:`app.lifespan` necessary even though
:class:`PgMigrationRunner` itself doesn't touch psycopg directly.

This module imports ``Cursor`` **only under** :data:`typing.TYPE_CHECKING`,
so :mod:`psycopg` is never loaded at runtime. The actual ``execute``
call is duck-typed — any object with an ``execute(sql)`` /
``execute(sql, params)`` method works, which keeps mocks / fakes in
tests trivial.

Trust contract — read the per-function docstrings
-------------------------------------------------
:func:`run_trusted_sql` is for **static** SQL. :func:`run_parameterized_sql`
is for **trusted templates + runtime values**, where values flow through
psycopg's native parameter binder so they are NEVER spliced into the SQL
string. Manual ``str.replace("'", "''")`` "escaping" is explicitly out
of contract for both helpers — see :func:`run_trusted_sql` for the full
rationale.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, LiteralString, cast

if TYPE_CHECKING:
    # Type-only import keeps this module psycopg-free at runtime so
    # ``migrations.postgres`` (and any other caller that only needs the
    # trust-boundary helpers) can be imported in a Delta-only environment
    # without installing psycopg. The runtime call below is duck-typed.
    from psycopg import Cursor


def run_trusted_sql(cur: Cursor[Any], sql: str) -> None:
    """Execute a backend-composed STATIC SQL string against a psycopg cursor.

    psycopg's stubs (PEP 675) require :meth:`Cursor.execute`'s
    ``query`` argument to be a :class:`typing.LiteralString` — a
    string the type-checker can statically prove is not a runtime
    concatenation of untrusted input. That's a deliberate type-level
    defence against SQL injection; passing a plain ``str`` makes
    basedpyright raise ``reportCallIssue`` + ``reportArgumentType``.

    Casting to ``LiteralString`` in this *one* place concentrates
    that trust boundary into a single auditable wrapper instead of
    sprinkling ``# pyright: ignore[reportCallIssue,
    reportArgumentType]`` at every call site (which AGENTS.md rule
    #6 explicitly prohibits).

    Trust contract (narrow on purpose)
    ----------------------------------
    Every string reaching this helper MUST be built EXCLUSIVELY from:

    1. **Compile-time string literals** vendored alongside the code
       that consumes them — DDL templates, fixed SELECT/UPDATE
       skeletons, schema-bootstrap statements, etc.
    2. **Identifiers** (catalog / schema / table / column names)
       validated by :func:`backend.sql_utils.validate_fqn` and
       quoted via :meth:`PgExecutor.q` /
       :func:`backend.sql_utils.quote_fqn`. The quoting protects
       reserved-word identifiers; the validation rejects anything
       that doesn't look like an identifier in the first place.
    3. **Compile-time value literals** rendered by
       :func:`sql_executor._render_value` for the well-known set of
       safe sentinel values (``RawSql("current_timestamp()")``,
       integer / boolean constants used as flags, etc.).

    Explicitly OUT of contract
    --------------------------
    Runtime VALUE literals — anything that originates from a Python
    object's mutable state, a config file, a function argument, an
    `f"...{value}..."` interpolation, or a manual
    ``str.replace("'", "''")`` "escape" — must NOT reach this
    helper. Manual escaping is not a defence: it silently rubber-
    stamps every future change that drops a new f-string into the
    same template. The ``cast(LiteralString, ...)`` performed here
    is a type-checker silence, not a runtime sanitiser.

    For "trusted template + runtime value" callers, use the sibling
    helper :func:`run_parameterized_sql` (which routes values
    through psycopg's native ``cur.execute(sql, params)`` binding),
    or call ``cur.execute(template, params)`` directly. Either path
    keeps the value out of the SQL string entirely so the LiteralString
    cast on the template alone is sound.
    """
    _ = cur.execute(cast(LiteralString, sql))


def run_parameterized_sql(cur: Cursor[Any], sql: str, params: Sequence[Any]) -> None:
    """Execute a trusted SQL TEMPLATE with psycopg-bound runtime values.

    Sibling to :func:`run_trusted_sql` for the common pattern where
    the SQL structure (identifiers, clauses, the ``%s`` placeholders
    themselves) is trusted but the values bound to those placeholders
    originate at runtime — from migration metadata, an admin's
    config row, a method argument, anything that isn't a compile-time
    literal.

    The *template* is subject to the same trust contract as
    :func:`run_trusted_sql`: it MUST be composed entirely from
    compile-time literals plus identifiers routed through
    :meth:`PgExecutor.q` / :func:`quote_fqn`. The contract is
    enforced by the ``cast(LiteralString, sql)`` here, exactly like
    in :func:`run_trusted_sql`, so the same auditing discipline
    applies to the template surface.

    The *values* in ``params`` are passed through psycopg's native
    parameter binding (``cur.execute(template, params)``), which
    forwards them to libpq as binary parameters — never spliced into
    the SQL string. That makes value-side injection structurally
    impossible regardless of the value's provenance, which is the
    point: ``run_trusted_sql`` cannot offer that guarantee for
    f-string-interpolated values because it has no access to the
    distinction between "template" and "value" once the caller has
    concatenated them.

    Example::

        run_parameterized_sql(
            cur,
            f"INSERT INTO {self._meta_table} (version, description) "
            "VALUES (%s, %s)",
            (migration.version, migration.description),
        )

    The ``self._meta_table`` is an identifier built via
    :meth:`PgExecutor.q` so it satisfies the template contract; the
    integer and description go through psycopg's binder so neither
    needs manual escaping.
    """
    _ = cur.execute(cast(LiteralString, sql), params)

"""Unit tests for :mod:`backend.pg_executor`.

Strategy
--------
The real :class:`PgExecutor` opens a psycopg connection pool and spawns
a daemon thread inside ``__init__``. Both side-effects are off-limits in
a unit-test environment (we don't want a live Postgres dependency, a
background thread surviving the test, or a flaky pool open).

Two tactics keep the tests fast, hermetic, and focused:

1. **Pure helpers are tested as pure functions.** ``_to_text``,
   ``_pg_render_value``, and the SQL-shape produced by ``upsert`` need
   nothing from the surrounding class state, so we test them at the
   module level without instantiating ``PgExecutor``.

2. **Method-level tests build a half-initialised instance via
   ``__new__``** rather than going through ``__init__``. The
   ``_make_pg_executor`` helper below sets only the attributes a given
   test needs and mocks the pool / token holder / stop event, so we
   exercise the behaviour-under-test in isolation without touching the
   real psycopg pool or starting the refresher thread.

The :class:`PgExecutor` end-to-end happy path (real pool open, real
SCRAM handshake, real token rotation) is covered by integration tests
that run against a live Lakebase instance — out of scope here.
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from databricks_labs_dqx_app.backend.pg_executor import (
    PgExecutor,
    _generate_token,
    _pg_render_value,
    _to_text,
    _TokenHolder,
    build_pg_executor,
)
from databricks_labs_dqx_app.backend.sql_executor import RawSql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pg_executor(
    *,
    schema: str = "public",
    database: str = "dqx",
    instance_name: str = "test-instance",
    token_refresh_seconds: int = 0,
    token_refresh_retry_seconds: int = 10,
    token_refresh_retry_jitter: float = 0.3,
    token_refresh_max_failures: int = 12,
    initial_token: str = "tok-initial",
) -> PgExecutor:
    """Build a method-callable :class:`PgExecutor` without running ``__init__``.

    Skips the real ``ConnectionPool`` open, the bootstrap
    ``_generate_token`` call, and the refresher-thread spawn.

    *Why ``__new__`` here, unlike* ``conftest.make_scheduler`` *which uses
    the real constructor?* ``SchedulerService.__init__`` is pure
    attribute assignment, so injecting dependencies through the real
    constructor is straightforward. ``PgExecutor.__init__``, in
    contrast, performs three unavoidable side effects: it calls
    ``_generate_token`` (a network round-trip to the SDK), opens a
    real ``ConnectionPool`` (a TCP connect to Postgres), and starts a
    daemon thread. A unit test cannot perform any of these. Adding
    constructor kwargs to gate each side effect would expose
    test-only seams in production code — worse than the localised
    ``__new__`` here, which is contained to this single test helper.
    The ``_make_pg_executor`` factory makes the asymmetry explicit
    and keeps the boundary in test code.

    The returned instance has the minimum attribute surface every
    method under test reads:

    - ``_ws`` / ``_instance_name`` — used by the refresh loop and by
      :func:`build_pg_executor` smoke tests.
    - ``_schema`` / ``_database`` — used by ``schema`` / ``database`` /
      ``fqn`` / ``catalog`` properties.
    - ``_token_holder`` — real :class:`_TokenHolder` so the refresh-loop
      tests can read its mutated state.
    - ``_connect_kwargs`` — real ``dict`` so the refresh-loop tests can
      observe the mutated ``password`` key.
    - ``_pool`` — :class:`MagicMock` with a context-manager-shaped
      ``connection()`` so the executor methods that go through the
      pool can be exercised without psycopg.
    - ``_stop`` — real :class:`threading.Event` so the refresh-loop
      tests can terminate the loop deterministically.

    Tests that need to inspect the inner cursor build it inline (see
    ``_cursor_of`` below) rather than baking it into the helper to
    keep this factory uncluttered.
    """
    inst = PgExecutor.__new__(PgExecutor)
    inst._ws = MagicMock(name="WorkspaceClient")
    inst._instance_name = instance_name
    inst._database = database
    inst._schema = schema
    inst._username = "test-user"
    inst._host = "test-host"
    inst._port = 5432
    inst._token_refresh_seconds = token_refresh_seconds
    inst._token_refresh_retry_seconds = token_refresh_retry_seconds
    inst._token_refresh_retry_jitter = token_refresh_retry_jitter
    inst._token_refresh_max_failures = token_refresh_max_failures
    inst._token_holder = _TokenHolder(initial_token)
    inst._connect_kwargs = {"password": initial_token}
    inst._last_successful_refresh_at = None
    inst._consecutive_refresh_failures = 0

    # Pool is a MagicMock with the context-manager protocol wired so
    # ``with self._pool.connection() as conn:`` yields a deterministic
    # mock connection.
    pool = MagicMock(name="ConnectionPool")
    conn_cm = pool.connection.return_value
    conn_cm.__exit__.return_value = None  # don't swallow exceptions
    inst._pool = pool

    inst._stop = threading.Event()
    inst._refresher = MagicMock(name="Thread")  # never started in tests
    return inst


def _conn_of(executor: PgExecutor) -> MagicMock:
    """Return the connection mock the executor sees inside ``with self._pool.connection() as conn:``."""
    return executor._pool.connection.return_value.__enter__.return_value  # type: ignore[no-any-return]


def _cursor_of(executor: PgExecutor) -> MagicMock:
    """Return the cursor mock the executor uses inside ``with conn.cursor() as cur:``."""
    conn = _conn_of(executor)
    cur_cm = conn.cursor.return_value
    cur_cm.__exit__.return_value = None  # don't swallow exceptions
    return cur_cm.__enter__.return_value  # type: ignore[no-any-return]


# ===========================================================================
# Pure helpers
# ===========================================================================


class TestToText:
    """Coerces psycopg-typed cells to the Delta-compatible string output.

    The runtime type ordering matters: ``isinstance(True, int)`` is
    ``True`` in Python, so the ``bool`` branch MUST be checked before
    the int/float branch. A regression here would render booleans as
    ``"1"`` / ``"0"`` instead of ``"true"`` / ``"false"`` and break
    every service that round-trips a boolean through the API.
    """

    def test_none_passes_through(self) -> None:
        assert _to_text(None) is None

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ({"a": 1, "b": [2, 3]}, '{"a":1,"b":[2,3]}'),
            ([1, 2, 3], "[1,2,3]"),
            ({}, "{}"),
            ([], "[]"),
        ],
    )
    def test_dict_and_list_render_compact_json(self, value: object, expected: str) -> None:
        assert _to_text(value) == expected

    def test_datetime_renders_iso_8601(self) -> None:
        ts = dt.datetime(2026, 5, 28, 9, 30, 0, tzinfo=dt.timezone.utc)
        assert _to_text(ts) == "2026-05-28T09:30:00+00:00"

    def test_naive_datetime_renders_iso_without_offset(self) -> None:
        ts = dt.datetime(2026, 5, 28, 9, 30, 0)
        assert _to_text(ts) == "2026-05-28T09:30:00"

    def test_date_renders_iso(self) -> None:
        assert _to_text(dt.date(2026, 5, 28)) == "2026-05-28"

    def test_bool_true_renders_lowercase(self) -> None:
        """Critical: bool must match BEFORE int (bool is an int subclass)."""
        assert _to_text(True) == "true"

    def test_bool_false_renders_lowercase(self) -> None:
        assert _to_text(False) == "false"

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            # Decimals within the "normal" exponent range round-trip
            # as plain notation, byte-for-byte equal to what the user
            # originally wrote. This is the common case and the one
            # service code actually exercises.
            (Decimal("0"), "0"),
            (Decimal("123"), "123"),
            (Decimal("123.456"), "123.456"),
            (Decimal("-0.5"), "-0.5"),
            # Very small / very large magnitudes flip to scientific
            # notation because that's what Python's :meth:`Decimal.__str__`
            # produces once the exponent leaves the normal range. We
            # deliberately accept this output (see method docstring) —
            # downstream Postgres and CSV consumers parse ``1E-7`` and
            # ``1E+21`` exactly like the plain forms. Switching to
            # ``format(d, "f")`` would force plain notation but bloat
            # output for legitimately tiny / huge values and surprise
            # readers used to Decimal's canonical repr.
            (Decimal("0.0000001"), "1E-7"),
            (Decimal("1E+21"), "1E+21"),
        ],
    )
    def test_decimal_uses_decimal_str_repr(self, value: Decimal, expected: str) -> None:
        """Decimal values pass through ``str(value)`` verbatim.

        The function does NOT force plain notation; it relies on
        :meth:`Decimal.__str__`, which switches to scientific notation
        once the exponent is outside Decimal's "normal" range
        (configurable via :class:`decimal.Context`, default ±28). The
        test name documents this contract — an earlier version of this
        test claimed "no scientific notation" while asserting on
        ``"1E-7"``, which silently passed for the wrong reason.

        If a future caller cannot tolerate scientific notation (e.g.
        a strict CSV importer that doesn't recognise the ``E`` form),
        the fix is to apply ``format(d, "f")`` at the formatting layer
        — NOT to swap :func:`_to_text`'s implementation, which would
        bloat output for legitimately tiny / huge values.
        """
        assert _to_text(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (0, "0"),
            (42, "42"),
            (-7, "-7"),
            (1.5, "1.5"),
            (0.0, "0.0"),
        ],
    )
    def test_numeric_renders_as_str(self, value: float, expected: str) -> None:
        assert _to_text(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (b"\x00\x01\x02", "000102"),
            (bytearray(b"hello"), "68656c6c6f"),
            (memoryview(b"\xde\xad\xbe\xef"), "deadbeef"),
        ],
    )
    def test_bytes_like_renders_as_hex(self, value: object, expected: str) -> None:
        assert _to_text(value) == expected

    def test_plain_string_passes_through(self) -> None:
        assert _to_text("hello") == "hello"

    def test_arbitrary_object_falls_back_to_str(self) -> None:
        class Custom:
            def __str__(self) -> str:
                return "custom-repr"

        assert _to_text(Custom()) == "custom-repr"


class TestPgRenderValue:
    """Postgres-flavoured literal renderer for ``upsert``.

    Delegates non-:class:`RawSql` values to :func:`SqlExecutor._render_value`;
    the Postgres-specific responsibility is translating Spark-style
    ``current_timestamp()`` / ``now()`` to bare ``CURRENT_TIMESTAMP``
    that Postgres actually parses, while letting other ``RawSql``
    payloads through verbatim so callers can still inject Postgres-
    specific helpers.
    """

    @pytest.mark.parametrize("payload", ["current_timestamp()", "now()", "CURRENT_TIMESTAMP()", "Now()"])
    def test_spark_timestamp_idioms_become_postgres_current_timestamp(self, payload: str) -> None:
        assert _pg_render_value(RawSql(payload)) == "CURRENT_TIMESTAMP"

    @pytest.mark.parametrize("payload", ["  current_timestamp()  ", "\tnow()\n"])
    def test_spark_timestamp_idioms_tolerate_surrounding_whitespace(self, payload: str) -> None:
        assert _pg_render_value(RawSql(payload)) == "CURRENT_TIMESTAMP"

    @pytest.mark.parametrize(
        "payload",
        [
            "CURRENT_TIMESTAMP",  # already-Postgres
            "uuid_generate_v4()",  # postgres-specific function
            "(SELECT max(version) FROM other_table)",  # arbitrary subexpression
            "EXCLUDED.updated_at",  # ON CONFLICT helper
        ],
    )
    def test_other_raw_sql_passes_through_verbatim(self, payload: str) -> None:
        assert _pg_render_value(RawSql(payload)) == payload

    def test_bool_true_renders_as_TRUE(self) -> None:
        assert _pg_render_value(True) == "TRUE"

    def test_bool_false_renders_as_FALSE(self) -> None:
        assert _pg_render_value(False) == "FALSE"

    def test_none_renders_as_NULL(self) -> None:
        assert _pg_render_value(None) == "NULL"

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (42, "42"),
            (-7, "-7"),
            (1.5, "1.5"),
        ],
    )
    def test_numeric_renders_as_literal(self, value: float, expected: str) -> None:
        assert _pg_render_value(value) == expected

    def test_string_is_ansi_escaped(self) -> None:
        assert _pg_render_value("it's fine") == "'it''s fine'"


# ===========================================================================
# Identifier quoting (review item #8 already tightened the Delta side;
# here we cover the Postgres side directly).
# ===========================================================================


class TestPgQuoting:
    """Direct tests of :meth:`PgExecutor.q`."""

    @pytest.mark.parametrize(
        ("identifier", "expected"),
        [
            ("foo", '"foo"'),
            # Reserved-word columns the OLTP schema actually contains
            ("user", '"user"'),
            ("order", '"order"'),
            ("check", '"check"'),
            # Internal ``"`` must be doubled per ANSI SQL.
            ('weird"name', '"weird""name"'),
            # Pre-doubled (no over-escaping)
            ('a""b', '"a""""b"'),
            # Unicode survives (Postgres permits it in quoted identifiers)
            ("café", '"café"'),
            # Empty string — corner case for any escaping helper
            ("", '""'),
        ],
    )
    def test_quotes_identifier(self, identifier: str, expected: str) -> None:
        executor = _make_pg_executor()
        assert executor.q(identifier) == expected


# ===========================================================================
# upsert SQL shape
# ===========================================================================


class TestUpsertSqlShape:
    """Verifies the INSERT ... ON CONFLICT ... SQL shape.

    The reviewer's specific request: cover both the ``DO UPDATE``
    branch (when ``value_cols`` is populated) and the ``DO NOTHING``
    branch (when ``value_cols`` is empty — i.e. "ensure-this-row-
    exists" semantics). All column names must be quoted via
    :meth:`PgExecutor.q` so reserved words survive.
    """

    def _capture(self, executor: PgExecutor) -> list[str]:
        captured: list[str] = []
        # Monkey-patch the ``execute`` method so we read the rendered SQL
        # without needing a real pool. Bound to ``self`` via ``__get__``
        # so the method swap survives the call chain.
        executor.execute = lambda sql, **_: captured.append(sql)  # type: ignore[method-assign]
        return captured

    def test_do_update_branch_when_value_cols_populated(self) -> None:
        executor = _make_pg_executor()
        captured = self._capture(executor)

        executor.upsert(
            table='"dq"."dq_app_settings"',
            key_cols={"setting_key": "max_concurrent_runs"},
            value_cols={"setting_value": "10", "updated_by": "admin"},
        )

        assert len(captured) == 1
        sql = captured[0]
        assert 'INSERT INTO "dq"."dq_app_settings"' in sql
        assert 'ON CONFLICT ("setting_key") DO UPDATE SET' in sql
        # Both value cols must appear in the SET clause, each referencing EXCLUDED.
        assert '"setting_value" = EXCLUDED."setting_value"' in sql
        assert '"updated_by" = EXCLUDED."updated_by"' in sql
        # Column names in the INSERT list must be quoted (reserved-word safety).
        assert '("setting_key", "setting_value", "updated_by")' in sql
        # String literals must be ANSI-escaped.
        assert "'max_concurrent_runs'" in sql

    def test_do_nothing_branch_when_value_cols_empty(self) -> None:
        """Empty ``value_cols`` is the reviewer-flagged 'ensure-row-exists' shape."""
        executor = _make_pg_executor()
        captured = self._capture(executor)

        executor.upsert(
            table='"dq"."dq_role_mappings"',
            key_cols={"role": "admin", "group_name": "platform-admins"},
            value_cols={},
        )

        assert len(captured) == 1
        sql = captured[0]
        assert 'INSERT INTO "dq"."dq_role_mappings"' in sql
        # Both keys appear in the ON CONFLICT target.
        assert 'ON CONFLICT ("role", "group_name") DO NOTHING' in sql
        # No DO UPDATE / SET clause should leak in.
        assert "DO UPDATE" not in sql
        assert " SET " not in sql

    def test_raises_when_no_key_cols(self) -> None:
        executor = _make_pg_executor()
        self._capture(executor)
        with pytest.raises(ValueError, match="upsert requires at least one key column"):
            executor.upsert(table="t", key_cols={}, value_cols={"x": 1})

    def test_raw_sql_in_value_cols_translates_current_timestamp(self) -> None:
        """RawSql values flow through _pg_render_value, so Spark idioms are translated."""
        executor = _make_pg_executor()
        captured = self._capture(executor)

        executor.upsert(
            table='"dq"."dq_app_settings"',
            key_cols={"setting_key": "feature_flag"},
            value_cols={"setting_value": "on", "updated_at": RawSql("current_timestamp()")},
        )

        sql = captured[0]
        # The Spark idiom must NOT appear; the Postgres bare keyword should.
        assert "current_timestamp()" not in sql
        assert "CURRENT_TIMESTAMP" in sql

    def test_reserved_word_key_column_is_quoted(self) -> None:
        executor = _make_pg_executor()
        captured = self._capture(executor)
        executor.upsert(
            table="t",
            key_cols={"check": "1"},  # reserved-word column
            value_cols={"order": "2"},  # another reserved word
        )
        sql = captured[0]
        assert '("check", "order")' in sql
        assert 'ON CONFLICT ("check")' in sql
        assert '"order" = EXCLUDED."order"' in sql


# ===========================================================================
# upsert_with_audit — Protocol method that absorbs the dialect branch
# ===========================================================================


class TestUpsertWithAuditSqlShape:
    """Verifies the audit-aware upsert SQL shape on Postgres.

    The Protocol contract :meth:`OltpExecutorProtocol.upsert_with_audit`
    is responsible for two behaviours service code used to hand-roll
    via dialect branches:

    1. ``preserve_created=True`` excludes ``created_*`` columns from
       the DO UPDATE SET clause so the original creator/timestamp
       survive after the first INSERT.
    2. ``increment_on_update`` rewrites the named column's DO UPDATE
       expression to ``col = col + 1`` (bare reference, which Postgres
       resolves to the existing row inside ON CONFLICT DO UPDATE).

    These tests pin the rendered SQL so a regression to the old
    "include every value_cols in DO UPDATE" shape would fail loudly.
    """

    def _capture(self, executor: PgExecutor) -> list[str]:
        captured: list[str] = []
        executor.execute = lambda sql, **_: captured.append(sql)  # type: ignore[method-assign]
        return captured

    def test_preserve_created_excludes_created_cols_from_update_set(self) -> None:
        executor = _make_pg_executor()
        captured = self._capture(executor)

        executor.upsert_with_audit(
            table='"dq"."dq_role_mappings"',
            key_cols={"role": "admin", "group_name": "platform-admins"},
            value_cols={
                "created_by": "alice@x",
                "created_at": RawSql("now()"),
                "updated_by": "alice@x",
                "updated_at": RawSql("now()"),
            },
            preserve_created=True,
        )

        sql = captured[0]
        # INSERT carries ALL six cols (2 keys + 4 audit).
        assert '("role", "group_name", "created_by", "created_at", "updated_by", "updated_at")' in sql
        # DO UPDATE SET names ONLY the non-created cols.
        assert "DO UPDATE SET" in sql
        assert '"updated_by" = ' in sql
        assert '"updated_at" = ' in sql
        # created_* MUST be absent from the SET clause — the regression
        # we're guarding against is the executor clobbering the original
        # creator/timestamp on every upsert.
        set_clause = sql.split("DO UPDATE SET", 1)[1]
        assert '"created_by"' not in set_clause
        assert '"created_at"' not in set_clause

    def test_increment_on_update_uses_bare_column_self_reference(self) -> None:
        """On Postgres, ``col + 1`` inside DO UPDATE resolves to the existing row."""
        executor = _make_pg_executor()
        captured = self._capture(executor)

        executor.upsert_with_audit(
            table='"dq"."dq_schedule_config"',
            key_cols={"schedule_name": "main"},
            value_cols={
                "config_json": '{"k":"v"}',
                "version": 1,  # initial INSERT value
                "updated_by": "alice@x",
                "updated_at": RawSql("now()"),
            },
            preserve_created=False,
            increment_on_update="version",
        )

        sql = captured[0]
        # INSERT VALUES include the literal initial 1 (not the increment expr).
        assert "VALUES ('main', '{\"k\":\"v\"}', 1, 'alice@x', CURRENT_TIMESTAMP)" in sql
        # DO UPDATE rewrites version to the bare-column self-reference
        # — no table prefix, no EXCLUDED reference.
        assert '"version" = "version" + 1' in sql
        # And specifically NOT the EXCLUDED form (which would set it to
        # the proposed 1 every time, defeating the increment).
        assert '"version" = EXCLUDED."version"' not in sql

    def test_combined_preserve_created_and_increment_for_schedule_config_shape(self) -> None:
        """The exact call ``ScheduleConfigService.save`` makes — locked
        down end-to-end so a service refactor that drops either kwarg
        fails here."""
        executor = _make_pg_executor()
        captured = self._capture(executor)

        executor.upsert_with_audit(
            table='"dq"."dq_schedule_config"',
            key_cols={"schedule_name": "main"},
            value_cols={
                "config_json": '{}',
                "version": 1,
                "created_by": "alice@x",
                "created_at": RawSql("now()"),
                "updated_by": "alice@x",
                "updated_at": RawSql("now()"),
            },
            preserve_created=True,
            increment_on_update="version",
        )

        sql = captured[0]
        set_clause = sql.split("DO UPDATE SET", 1)[1]
        # SET clause has updated_*, config_json, and the version self-
        # reference — but NOT created_*.
        assert '"created_by"' not in set_clause
        assert '"created_at"' not in set_clause
        assert '"config_json"' in set_clause
        assert '"updated_by"' in set_clause
        assert '"updated_at"' in set_clause
        assert '"version" = "version" + 1' in set_clause

    def test_all_value_cols_are_created_renders_do_nothing(self) -> None:
        """Edge case: insert-only audit row with no updatable cols."""
        executor = _make_pg_executor()
        captured = self._capture(executor)

        executor.upsert_with_audit(
            table='"dq"."ledger"',
            key_cols={"id": "1"},
            value_cols={"created_by": "system", "created_at": RawSql("now()")},
            preserve_created=True,
        )

        sql = captured[0]
        # Nothing to update → DO NOTHING (not a malformed empty SET).
        assert "DO NOTHING" in sql
        assert "DO UPDATE" not in sql
        assert " SET " not in sql

    def test_raises_when_no_key_cols(self) -> None:
        executor = _make_pg_executor()
        self._capture(executor)
        with pytest.raises(ValueError, match="upsert_with_audit requires at least one key column"):
            executor.upsert_with_audit(table="t", key_cols={}, value_cols={"x": 1})

    def test_raises_when_increment_col_missing_from_value_cols(self) -> None:
        """Guard against silently dropping a version bump."""
        executor = _make_pg_executor()
        self._capture(executor)
        with pytest.raises(ValueError, match="increment_on_update='version' must be present"):
            executor.upsert_with_audit(
                table="t",
                key_cols={"k": "1"},
                value_cols={"updated_by": "u"},  # no version!
                increment_on_update="version",
            )

    def test_reserved_word_increment_column_is_quoted(self) -> None:
        executor = _make_pg_executor()
        captured = self._capture(executor)
        executor.upsert_with_audit(
            table="t",
            key_cols={"k": "1"},
            value_cols={"order": 1},  # reserved word
            preserve_created=False,
            increment_on_update="order",
        )
        sql = captured[0]
        # Increment must use quoted identifier on BOTH sides of the +.
        assert '"order" = "order" + 1' in sql


# ===========================================================================
# select_json_text / interval_days_expr — Protocol render helpers
# ===========================================================================


class TestProtocolRenderHelpers:
    """The two pure-string helpers that absorb dialect rendering.

    Both are trivial but the *exact* string shape matters — a typo
    becomes a SQL syntax error against a live Postgres at the worst
    possible moment (a migration window). Lock them down here so any
    accidental rewrite trips the test, not the customer's Lakebase.
    """

    def test_select_json_text_returns_column_verbatim(self) -> None:
        # Postgres JSONB → text coercion happens in _to_text on the way
        # out, so the projection is just the bare column. No to_json(),
        # no CAST — a regression to either would re-introduce the
        # dialect branch the Protocol absorbs.
        executor = _make_pg_executor()
        assert executor.select_json_text('"check"') == '"check"'
        assert executor.select_json_text("config_json") == "config_json"

    def test_interval_days_expr_uses_quoted_lowercase_plural(self) -> None:
        # Postgres syntax: INTERVAL '<N> days' (single-quoted literal,
        # lowercase plural). The Delta form (INTERVAL N DAY) would
        # produce ``ERROR: syntax error at or near "DAY"`` against a
        # real Postgres.
        executor = _make_pg_executor()
        assert executor.interval_days_expr(90) == "INTERVAL '90 days'"
        assert executor.interval_days_expr(1) == "INTERVAL '1 days'"
        # int() coercion is part of the contract — defends against a
        # ``float`` slipping in from a config bug.
        assert executor.interval_days_expr(7.0) == "INTERVAL '7 days'"  # type: ignore[arg-type]


# ===========================================================================
# connection() context manager
# ===========================================================================


class TestConnectionContextManager:
    """The wrapper around ``self._pool.connection()``.

    The contract documented on :meth:`PgExecutor.connection`:

    - The caller is responsible for ``conn.commit()``.
    - If the ``with`` block exits without committing (exception or
      otherwise) psycopg-pool rolls back and returns the connection.

    These tests verify our wrapper is a *true* pass-through: it
    neither commits nor rolls back implicitly, and it propagates
    exceptions raised inside the ``with`` block.
    """

    def test_yields_the_pool_connection(self) -> None:
        executor = _make_pg_executor()
        with executor.connection() as conn:
            # The yielded object is the same conn the pool produced.
            assert conn is _conn_of(executor)

    def test_does_not_implicitly_commit_or_rollback(self) -> None:
        """Caller-managed transaction semantics — we don't sneak commits in."""
        executor = _make_pg_executor()
        with executor.connection() as conn:
            pass
        # Our wrapper never calls commit() or rollback() on the conn —
        # the caller (or psycopg-pool's __exit__) owns that decision.
        conn.commit.assert_not_called()  # type: ignore[attr-defined]
        conn.rollback.assert_not_called()  # type: ignore[attr-defined]

    def test_exception_inside_block_propagates(self) -> None:
        """A user error inside ``with executor.connection()`` must not be swallowed."""
        executor = _make_pg_executor()
        with pytest.raises(RuntimeError, match="boom"):
            with executor.connection():
                raise RuntimeError("boom")


# ===========================================================================
# _token_refresh_loop — happy / sad / permanent-failure paths
# ===========================================================================


class TestTokenRefreshLoop:
    """Background-thread refresh logic, tested without spawning the thread.

    We invoke ``_token_refresh_loop`` directly on a half-built executor
    so the test runs synchronously. ``_stop`` is a real
    :class:`threading.Event` so we can wake the loop deterministically.
    All ``_escalate_refresh_failure`` calls are patched out — the
    real implementation hard-exits the process via ``os._exit``, which
    we cover separately in :class:`TestRefreshFailureEscalation`.
    """

    def _stop_after(self, executor: PgExecutor, n: int) -> None:
        """Patch ``_stop.wait`` so the loop runs exactly ``n`` iterations.

        Each ``wait`` call increments a counter; on the ``n``-th call
        we set the stop event so the next ``while not self._stop.is_set()``
        check exits the loop. ``wait`` always returns ``False`` so the
        loop's "stop fired during sleep" early-return path doesn't fire
        — that's covered separately.
        """
        counter = {"n": 0}

        def fake_wait(timeout: float | None = None) -> bool:  # noqa: ARG001
            counter["n"] += 1
            if counter["n"] >= n:
                executor._stop.set()
            return False

        executor._stop.wait = fake_wait  # type: ignore[method-assign]

    def test_happy_path_updates_token_kwargs_and_metric(self) -> None:
        executor = _make_pg_executor(initial_token="old-token")
        self._stop_after(executor, 1)

        with patch(
            "databricks_labs_dqx_app.backend.pg_executor._generate_token",
            return_value="fresh-token",
        ) as gen:
            executor._token_refresh_loop()

        gen.assert_called_with(executor._ws, executor._instance_name)
        assert executor._token_holder.token == "fresh-token"
        assert executor._connect_kwargs["password"] == "fresh-token"
        # Success path updates the observability surface that the
        # health endpoint relies on.
        assert executor.consecutive_refresh_failures == 0
        assert executor.last_successful_refresh_at is not None

    def test_stop_event_terminates_loop_without_generating_token(self) -> None:
        """``wait`` returning True means the stop event fired during the sleep."""
        executor = _make_pg_executor()
        executor._stop.set()  # already set before the loop runs

        with patch("databricks_labs_dqx_app.backend.pg_executor._generate_token") as gen:
            executor._token_refresh_loop()

        gen.assert_not_called()

    def test_failure_then_success_increments_then_resets_counter(self, caplog) -> None:
        """Transient failure → counter increments; recovery → counter resets to 0."""
        executor = _make_pg_executor(initial_token="old-token", token_refresh_max_failures=10)
        self._stop_after(executor, 2)

        with patch(
            "databricks_labs_dqx_app.backend.pg_executor._generate_token",
            side_effect=[RuntimeError("transient SDK error"), "recovered-token"],
        ) as gen:
            with caplog.at_level(logging.WARNING, logger="databricks_labs_dqx_app.backend.pg_executor"):
                executor._token_refresh_loop()

        assert gen.call_count == 2
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("Failed to refresh Lakebase token" in r.message for r in warnings)
        # The warning carries the attempt counter so operators can
        # tell "first failure" from "approaching escalation".
        assert any("1/10" in r.message for r in warnings)
        # Recovery clears the failure counter and updates the token.
        assert executor._token_holder.token == "recovered-token"
        assert executor._connect_kwargs["password"] == "recovered-token"
        assert executor.consecutive_refresh_failures == 0

    def test_permanent_failure_below_threshold_does_not_escalate(self) -> None:
        """Sustained failures *under* ``max_failures`` keep spinning, don't escalate."""
        # Raise the threshold so 3 failures don't trip escalation —
        # we want to verify the loop keeps trying without dying or
        # exiting the process.
        executor = _make_pg_executor(initial_token="old-token", token_refresh_max_failures=99)
        self._stop_after(executor, 3)

        with (
            patch(
                "databricks_labs_dqx_app.backend.pg_executor._generate_token",
                side_effect=RuntimeError("Lakebase is down"),
            ) as gen,
            patch.object(type(executor), "_escalate_refresh_failure") as escalate,
        ):
            executor._token_refresh_loop()

        assert gen.call_count >= 3
        # Existing connections keep their original credential until
        # ``max_lifetime`` recycles them — we never mutated the
        # token holder.
        assert executor._token_holder.token == "old-token"
        assert executor._connect_kwargs["password"] == "old-token"
        # Failure counter accumulated across all attempts.
        assert executor.consecutive_refresh_failures >= 3
        # And — critically — we did NOT escalate; the escalation
        # path is reserved for ``>= max_failures``.
        escalate.assert_not_called()


# ===========================================================================
# _next_wait_seconds — back-off cadence + jitter bounds
# ===========================================================================


class TestNextWaitSeconds:
    """The back-off chooser is the load-bearing piece of the redesign.

    Healthy → wait the full scheduled interval (don't burn SDK quota).
    Failing → wait the short retry interval, jittered, so we recover
    inside the pool's ``max_lifetime`` window instead of letting it
    drain silently.
    """

    def test_healthy_returns_scheduled_interval(self) -> None:
        """``consecutive_refresh_failures == 0`` ⇒ full scheduled wait."""
        executor = _make_pg_executor(
            token_refresh_seconds=3000,
            token_refresh_retry_seconds=10,
        )
        executor._consecutive_refresh_failures = 0
        assert executor._next_wait_seconds() == 3000.0

    def test_failing_returns_retry_interval_not_scheduled(self) -> None:
        """One failure flips to the short retry path — this is the whole point.

        The original loop slept 60s after a failure and *then* waited
        the full scheduled interval (50 min by default) before retrying,
        which silently drained the pool. Verify the new loop instead
        picks the retry budget, not the scheduled one.
        """
        executor = _make_pg_executor(
            token_refresh_seconds=3000,
            token_refresh_retry_seconds=10,
            token_refresh_retry_jitter=0.0,  # no jitter for a deterministic check
        )
        executor._consecutive_refresh_failures = 1
        wait = executor._next_wait_seconds()
        assert wait == 10.0, (
            f"After a failure the loop must pick the retry interval (10s), "
            f"not the scheduled interval (3000s); got {wait}s. "
            f"That regression is what silently drains the pool on sustained failure."
        )

    def test_jitter_stays_within_documented_bounds(self) -> None:
        """Jitter must be uniform within ``base ± base*jitter``.

        Operators tune ``retry_jitter`` to spread thunder-herd risk —
        if the jitter ever exceeded the documented bound, multiple
        workers' back-offs could synchronise (worse: a bug could
        produce wait=0, busy-looping the SDK). Sample a chunk of
        iterations and check the empirical bounds.
        """
        executor = _make_pg_executor(
            token_refresh_retry_seconds=10,
            token_refresh_retry_jitter=0.3,
        )
        executor._consecutive_refresh_failures = 1
        samples = [executor._next_wait_seconds() for _ in range(200)]
        lower = 10 - (10 * 0.3)  # 7.0
        upper = 10 + (10 * 0.3)  # 13.0
        assert all(lower <= s <= upper for s in samples), (
            f"Jitter escaped ±30% bound. lower={min(samples)}, upper={max(samples)}, "
            f"expected within [{lower}, {upper}]."
        )
        # And we actually observed some spread — not a single fixed
        # value (which would mean ``random.uniform`` was patched out
        # or the jitter math collapsed to a constant).
        assert len(set(samples)) > 1, (
            "Jitter produced a single repeated value across 200 samples; "
            "the randomisation is missing and workers will thunder-herd."
        )

    def test_jitter_zero_returns_exact_retry_seconds(self) -> None:
        """``jitter=0`` ⇒ deterministic, returns ``retry_seconds`` exactly.

        Documented escape hatch for tests / deterministic env where
        a flapping back-off would make logs hard to read.
        """
        executor = _make_pg_executor(
            token_refresh_retry_seconds=7,
            token_refresh_retry_jitter=0.0,
        )
        executor._consecutive_refresh_failures = 5
        for _ in range(10):
            assert executor._next_wait_seconds() == 7.0


# ===========================================================================
# _escalate_refresh_failure — process exit on sustained failure
# ===========================================================================


class TestRefreshFailureEscalation:
    """After ``max_failures`` consecutive failures the loop hands off to
    the supervisor by exiting the process.

    The escalation path is the user-facing fix for the silent-pool-drain
    risk: rather than continuing to retry while ``max_lifetime`` quietly
    expires every pooled connection, crash loud and let the supervisor
    restart us with a fresh token.
    """

    def test_loop_calls_escalate_after_max_failures(self, caplog) -> None:
        """Reaching the threshold triggers escalation, not another retry."""
        executor = _make_pg_executor(
            initial_token="old-token",
            token_refresh_max_failures=3,
        )
        # Force the loop to keep iterating; the escalation path is
        # what stops it, not the wait counter.
        executor._stop.wait = lambda timeout=None: False  # type: ignore[method-assign,assignment]

        with (
            patch(
                "databricks_labs_dqx_app.backend.pg_executor._generate_token",
                side_effect=RuntimeError("Lakebase is down"),
            ),
            patch.object(type(executor), "_escalate_refresh_failure") as escalate,
        ):
            with caplog.at_level(logging.WARNING, logger="databricks_labs_dqx_app.backend.pg_executor"):
                executor._token_refresh_loop()

        # Escalation fired exactly once — on the 3rd consecutive
        # failure, the loop calls it and then ``return``s.
        escalate.assert_called_once()
        assert executor.consecutive_refresh_failures == 3

    def test_escalate_logs_critical_with_context_then_exits(self, caplog) -> None:
        """Escalation logs CRITICAL with the failure count + last-success
        + instance name BEFORE exiting, so operators have something to
        page on / search for in log aggregators.

        ``os._exit`` is patched out so the test runs synchronously.
        """
        executor = _make_pg_executor(instance_name="lakebase-prod-us-east-1")
        executor._consecutive_refresh_failures = 12
        before = executor._last_successful_refresh_at

        with patch("databricks_labs_dqx_app.backend.pg_executor.os._exit") as exit_call:
            with caplog.at_level(logging.CRITICAL, logger="databricks_labs_dqx_app.backend.pg_executor"):
                executor._escalate_refresh_failure()

        exit_call.assert_called_once_with(1)
        criticals = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert criticals, "Escalation must log CRITICAL for ops paging."
        msg = criticals[0].getMessage()
        assert "12" in msg, "Log must include the failure count for context."
        assert "lakebase-prod-us-east-1" in msg, (
            "Log must include the instance name so multi-instance deployments " "know which Lakebase failed."
        )
        if before is not None:
            assert before.isoformat() in msg, (
                "Log must include the last-successful-refresh timestamp "
                "so operators know how long the failure window is."
            )

    def test_loop_invokes_os_exit_after_exactly_max_failures(self) -> None:
        """End-to-end: loop → escalate → ``os._exit(1)`` with NOTHING
        between ``os._exit`` and the loop patched out.

        The earlier ``test_loop_calls_escalate_after_max_failures``
        patches ``_escalate_refresh_failure`` itself, so it only
        verifies the loop *reaches* the escalation hand-off. That
        leaves the load-bearing claim — "the supervisor actually sees
        a non-zero exit after max_failures consecutive failures" —
        untested end-to-end. If a future refactor changed
        ``os._exit(1)`` to ``sys.exit(1)`` (which a daemon thread
        silently swallows), the existing test would still pass and
        the pool would silently drain in production exactly like the
        original 60s-sleep bug.

        Here we drive the loop with ``_generate_token`` raising every
        time, leave ``_escalate_refresh_failure`` UN-patched, and
        verify the chain reaches ``os._exit(1)`` after exactly
        ``max_failures`` token-generation attempts — not earlier
        (escalates one short of the budget) and not later
        (escalates one past the budget).
        """
        executor = _make_pg_executor(token_refresh_max_failures=4)
        # Never auto-stop via the sleep counter — the loop must stop
        # on its own via the ``return`` after ``_escalate_refresh_failure``.
        executor._stop.wait = lambda timeout=None: False  # type: ignore[method-assign,assignment]

        with (
            patch(
                "databricks_labs_dqx_app.backend.pg_executor._generate_token",
                side_effect=RuntimeError("Lakebase is down"),
            ) as gen,
            patch("databricks_labs_dqx_app.backend.pg_executor.os._exit") as exit_call,
        ):
            executor._token_refresh_loop()

        # Exactly ``max_failures`` token-generation attempts before
        # escalation fired. Anything less means escalation fires too
        # early (operators lose retries the policy promised them);
        # anything more means escalation fires too late (the pool's
        # ``max_lifetime`` may start expiring connections without a
        # valid replacement token, which is the original bug).
        assert gen.call_count == 4, (
            f"Expected escalation after exactly 4 consecutive failures, "
            f"got {gen.call_count} ``_generate_token`` calls. The "
            f"threshold check in ``_token_refresh_loop`` is off-by-one "
            f"or the counter increment landed on the wrong branch."
        )
        exit_call.assert_called_once_with(1)
        assert executor.consecutive_refresh_failures == 4

    def test_success_grants_a_fresh_threshold_window_before_escalation(self) -> None:
        """A single successful refresh resets the ENTIRE failure budget,
        not just decrements the counter by one.

        Drive ``max_failures=3`` with the sequence::

            fail, fail,                (counter = 2; one short of escalate)
            success,                   (counter must reset to 0)
            fail, fail,                (counter = 2; one short of escalate)
            fail                       (counter = 3 → escalate)

        Escalation must fire on the **6th** ``_generate_token`` call
        (the 3rd post-recovery failure), not the 4th (which would mean
        the success only suppressed the immediate escalate but the
        threshold was already poisoned), and not the 5th (which would
        mean the success only decremented by one).

        This catches subtle refactors of the reset path:
        - ``self._consecutive_refresh_failures -= 1`` instead of
          ``= 0`` → escalation would fire on the 5th call.
        - Forgetting the reset entirely (e.g. only updating
          ``last_successful_refresh_at``) → escalation would fire
          on the 4th call.
        Without this test, either bug ships green.
        """
        executor = _make_pg_executor(token_refresh_max_failures=3)
        executor._stop.wait = lambda timeout=None: False  # type: ignore[method-assign,assignment]

        # 6 attempts: 2 fail, 1 success, 3 fail (escalate). The list
        # is sized to the *expected* count so an early escalation
        # leaves entries unused (gen.call_count < 6, test fails) and
        # a late one would attempt a 7th call (StopIteration → still
        # caught by the loop's ``except Exception`` but escalation
        # would by then have fired off the wrong counter value).
        token_seq: list[object] = [
            RuntimeError("transient #1"),
            RuntimeError("transient #2"),
            "recovered-token",
            RuntimeError("transient #3"),
            RuntimeError("transient #4"),
            RuntimeError("transient #5"),
        ]

        with (
            patch(
                "databricks_labs_dqx_app.backend.pg_executor._generate_token",
                side_effect=token_seq,
            ) as gen,
            patch("databricks_labs_dqx_app.backend.pg_executor.os._exit") as exit_call,
        ):
            executor._token_refresh_loop()

        assert gen.call_count == 6, (
            f"Expected 6 ``_generate_token`` calls (2 fail + 1 success + "
            f"3 fail to re-reach the threshold of 3), got {gen.call_count}. "
            f"A count of 4 means success only suppressed the immediate "
            f"escalate (no counter reset). A count of 5 means success "
            f"only decremented the counter by one instead of resetting it."
        )
        exit_call.assert_called_once_with(1)
        # Counter at escalation time is exactly the threshold — the
        # success path fully reset it, the post-recovery failures
        # rebuilt it from zero.
        assert executor.consecutive_refresh_failures == 3
        # And the success in the middle DID update the token + the
        # observability surface — proving the reset is genuine (the
        # success branch ran in full), not a side-effect of some
        # other branch suppressing escalation.
        assert executor._token_holder.token == "recovered-token"
        assert executor._connect_kwargs["password"] == "recovered-token"


# ===========================================================================
# Observability surfaces — last_successful_refresh_at + counter
# ===========================================================================


class TestRefreshObservability:
    """``last_successful_refresh_at`` + ``consecutive_refresh_failures``
    are the metrics health endpoints scrape to catch the silent-drain
    failure before it cascades.
    """

    def test_initial_state_after_init_is_healthy(self) -> None:
        """A freshly-built executor reads as 'just refreshed, no failures'.

        The bootstrap token mint IS a successful refresh from the
        pool's perspective, so the metric must start in the healthy
        state — not as ``None`` / unknown, which would falsely page
        every operator at t=0.
        """
        # We can't run the real __init__ in unit tests (no real pool),
        # so the fixture sets these to ``None`` / 0; verify the *real*
        # __init__ sets the bootstrap timestamp by patching out the
        # external surfaces and constructing for real.
        with (
            patch(
                "databricks_labs_dqx_app.backend.pg_executor._generate_token",
                return_value="bootstrap-token",
            ),
            patch("databricks_labs_dqx_app.backend.pg_executor.ConnectionPool") as Pool,
            patch("databricks_labs_dqx_app.backend.pg_executor.threading.Thread"),
        ):
            Pool.check_connection = MagicMock()
            executor = PgExecutor(
                ws=MagicMock(),
                instance_name="instance",
                database="dqx",
                schema="public",
                username="sp",
                host="host",
            )

        assert executor.consecutive_refresh_failures == 0
        assert executor.last_successful_refresh_at is not None

    def test_success_updates_timestamp_monotonic(self) -> None:
        """Each successful refresh advances ``last_successful_refresh_at``."""
        executor = _make_pg_executor()
        before = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        executor._last_successful_refresh_at = before
        executor._stop.wait = lambda timeout=None: (executor._stop.set() or False)  # type: ignore[method-assign,assignment,func-returns-value]

        with patch(
            "databricks_labs_dqx_app.backend.pg_executor._generate_token",
            return_value="fresh",
        ):
            executor._token_refresh_loop()

        assert executor.last_successful_refresh_at is not None
        assert executor.last_successful_refresh_at > before, (
            "Successful refresh must advance the timestamp so a stalled "
            "loop is detectable via ``now() - last_successful_refresh_at``."
        )

    def test_failure_does_not_update_timestamp(self) -> None:
        """Last-success only moves on success — never on failure.

        Otherwise a refresh attempt that raised would silently reset
        the metric and hide the failure window from operators.
        """
        executor = _make_pg_executor()
        before = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        executor._last_successful_refresh_at = before
        executor._stop.wait = lambda timeout=None: (executor._stop.set() or False)  # type: ignore[method-assign,assignment,func-returns-value]

        with (
            patch(
                "databricks_labs_dqx_app.backend.pg_executor._generate_token",
                side_effect=RuntimeError("boom"),
            ),
            patch.object(type(executor), "_escalate_refresh_failure"),
        ):
            executor._token_refresh_loop()

        assert executor.last_successful_refresh_at == before
        assert executor.consecutive_refresh_failures >= 1


# ===========================================================================
# Constructor clamps — defend against mis-configured env vars
# ===========================================================================


class TestRefreshTuningClamps:
    """Mis-configured env vars can't degenerate the loop into a tight
    busy-loop or an infinite failure window."""

    def test_retry_seconds_floors_at_one(self) -> None:
        with (
            patch(
                "databricks_labs_dqx_app.backend.pg_executor._generate_token",
                return_value="t",
            ),
            patch("databricks_labs_dqx_app.backend.pg_executor.ConnectionPool"),
            patch("databricks_labs_dqx_app.backend.pg_executor.threading.Thread"),
        ):
            executor = PgExecutor(
                ws=MagicMock(),
                instance_name="i",
                database="d",
                schema="s",
                username="u",
                host="h",
                token_refresh_retry_seconds=0,
            )
        assert executor._token_refresh_retry_seconds == 1

    def test_jitter_clamps_to_unit_interval(self) -> None:
        with (
            patch(
                "databricks_labs_dqx_app.backend.pg_executor._generate_token",
                return_value="t",
            ),
            patch("databricks_labs_dqx_app.backend.pg_executor.ConnectionPool"),
            patch("databricks_labs_dqx_app.backend.pg_executor.threading.Thread"),
        ):
            high = PgExecutor(
                ws=MagicMock(),
                instance_name="i",
                database="d",
                schema="s",
                username="u",
                host="h",
                token_refresh_retry_jitter=5.0,
            )
            low = PgExecutor(
                ws=MagicMock(),
                instance_name="i",
                database="d",
                schema="s",
                username="u",
                host="h",
                token_refresh_retry_jitter=-0.5,
            )
        assert high._token_refresh_retry_jitter == 1.0
        assert low._token_refresh_retry_jitter == 0.0

    def test_max_failures_floors_at_one(self) -> None:
        """``max_failures=0`` would never escalate, the opposite of safe."""
        with (
            patch(
                "databricks_labs_dqx_app.backend.pg_executor._generate_token",
                return_value="t",
            ),
            patch("databricks_labs_dqx_app.backend.pg_executor.ConnectionPool"),
            patch("databricks_labs_dqx_app.backend.pg_executor.threading.Thread"),
        ):
            executor = PgExecutor(
                ws=MagicMock(),
                instance_name="i",
                database="d",
                schema="s",
                username="u",
                host="h",
                token_refresh_max_failures=0,
            )
        assert executor._token_refresh_max_failures == 1


# ===========================================================================
# build_pg_executor — error paths (the only ones exercisable without a
# real Postgres). The happy-path call sequence is exercised in
# integration tests.
# ===========================================================================


class TestBuildPgExecutor:
    """Cover the two early-validation RuntimeErrors at lines 436-444."""

    def test_raises_when_instance_has_no_read_write_dns(self) -> None:
        ws = MagicMock(name="WorkspaceClient")
        # get_database_instance returns an object whose .read_write_dns is empty.
        ws.database.get_database_instance.return_value = MagicMock(read_write_dns=None)

        with pytest.raises(RuntimeError, match="has no read_write_dns"):
            build_pg_executor(
                ws,
                instance_name="missing-dns-instance",
                database="dqx",
                schema="dqx_studio",
            )
        # Error fires *before* we ever ask for the calling identity.
        ws.current_user.me.assert_not_called()

    def test_raises_when_calling_identity_has_no_username_or_id(self) -> None:
        ws = MagicMock(name="WorkspaceClient")
        ws.database.get_database_instance.return_value = MagicMock(read_write_dns="some.host:5432")
        # Both user_name AND id come back falsy — Lakebase has nothing
        # to authenticate as.
        ws.current_user.me.return_value = MagicMock(user_name=None, id=None)

        with pytest.raises(RuntimeError, match="Could not determine workspace identity"):
            build_pg_executor(
                ws,
                instance_name="ok-instance",
                database="dqx",
                schema="dqx_studio",
            )

    def test_uses_id_as_fallback_when_user_name_is_empty(self) -> None:
        """``me.user_name or me.id`` fallback — proves we don't bail on user_name=None alone."""
        ws = MagicMock(name="WorkspaceClient")
        ws.database.get_database_instance.return_value = MagicMock(read_write_dns="ok.host")
        ws.current_user.me.return_value = MagicMock(user_name=None, id="sp-1234")

        # We stop before the real PgExecutor opens its pool — patch
        # PgExecutor.__init__ to a no-op and assert it was called with
        # the resolved username and host.
        with patch(
            "databricks_labs_dqx_app.backend.pg_executor.PgExecutor.__init__",
            return_value=None,
        ) as init_mock:
            build_pg_executor(
                ws,
                instance_name="ok-instance",
                database="dqx",
                schema="dqx_studio",
            )

        # __init__ is called as PgExecutor.__new__(cls).__init__(ws=..., ...)
        # so the kwargs are what we want to inspect.
        kwargs = init_mock.call_args.kwargs
        assert kwargs["username"] == "sp-1234", "fell through user_name=None to id"
        assert kwargs["host"] == "ok.host"
        assert kwargs["instance_name"] == "ok-instance"


# ===========================================================================
# Data-path methods (execute / query / query_dicts / close / execute_no_schema)
# ===========================================================================


class TestDataPathMethods:
    """The pool/cursor plumbing methods.

    These all share the same shape: ``with pool.connection() as conn: with
    conn.cursor() as cur:`` — so the mock fixtures already in
    :func:`_make_pg_executor` are enough to drive them and observe the
    SQL passed in and the rows coerced out.
    """

    def test_execute_commits_after_running(self) -> None:
        executor = _make_pg_executor()
        executor.execute("UPDATE t SET x = 1")
        cur = _cursor_of(executor)
        # Now TWO cursor.execute calls: SET LOCAL guard + user SQL.
        # The user SQL is the *second* call; the first is the
        # statement_timeout preamble verified in TestStatementTimeout.
        assert cur.execute.call_count == 2
        assert cur.execute.call_args_list[-1].args == ("UPDATE t SET x = 1",)
        # One-shot statements MUST auto-commit (parity with SqlExecutor).
        _conn_of(executor).commit.assert_called_once()

    def test_execute_no_schema_delegates_to_execute(self) -> None:
        executor = _make_pg_executor()
        executor.execute_no_schema("CREATE SCHEMA IF NOT EXISTS s")
        cur = _cursor_of(executor)
        # Same two-call shape as execute(): SET LOCAL + user SQL.
        assert cur.execute.call_count == 2
        assert cur.execute.call_args_list[-1].args == ("CREATE SCHEMA IF NOT EXISTS s",)
        _conn_of(executor).commit.assert_called_once()

    def test_query_returns_string_coerced_rows(self) -> None:
        executor = _make_pg_executor()
        cur = _cursor_of(executor)
        cur.fetchall.return_value = [
            (1, True, dt.date(2026, 5, 28), None),
            (2, False, dt.date(2026, 5, 29), "literal"),
        ]
        rows = executor.query("SELECT id, flag, day, note FROM t")
        # Each cell is coerced via _to_text → strings (or None for NULL).
        assert rows == [
            ["1", "true", "2026-05-28", None],
            ["2", "false", "2026-05-29", "literal"],
        ]

    def test_query_dicts_zips_column_names(self) -> None:
        executor = _make_pg_executor()
        cur = _cursor_of(executor)
        # ``MagicMock(name=...)`` is reserved for the mock identity, not
        # the synthesised attribute — set ``.name`` explicitly so the
        # implementation's ``d.name`` lookup returns the column label.
        col_a, col_b = MagicMock(), MagicMock()
        col_a.name = "a"
        col_b.name = "b"
        cur.description = [col_a, col_b]
        cur.fetchall.return_value = [(1, True), (2, None)]
        rows = executor.query_dicts("SELECT a, b FROM t")
        assert rows == [{"a": "1", "b": "true"}, {"a": "2", "b": None}]

    def test_query_dicts_handles_empty_description(self) -> None:
        """``cur.description`` can be ``None`` for DDL — must not raise."""
        executor = _make_pg_executor()
        cur = _cursor_of(executor)
        cur.description = None
        cur.fetchall.return_value = []
        assert executor.query_dicts("SELECT 1 WHERE FALSE") == []

    # ----- statement_timeout enforcement (per-call) -----------------------

    def _execute_calls(self, executor: PgExecutor) -> list[str]:
        """Return the raw SQL strings ``cur.execute`` was called with."""
        cur = _cursor_of(executor)
        return [call.args[0] for call in cur.execute.call_args_list]

    def test_execute_emits_set_local_statement_timeout_before_user_sql(self) -> None:
        """Default 120s timeout ⇒ ``SET LOCAL statement_timeout = 120000`` first."""
        executor = _make_pg_executor()
        executor.execute("UPDATE t SET x = 1")
        calls = self._execute_calls(executor)
        assert len(calls) == 2, (
            "Expected exactly 2 cursor.execute calls (SET LOCAL guard + user SQL); " f"got {len(calls)}: {calls}"
        )
        assert calls[0] == "SET LOCAL statement_timeout = 120000", (
            f"First call must be the SET LOCAL guard, got {calls[0]!r}. "
            "Without this, a runaway query pins the pool connection indefinitely."
        )
        assert calls[1] == "UPDATE t SET x = 1"

    def test_execute_respects_custom_timeout_seconds(self) -> None:
        """Per-call ``timeout_seconds`` overrides the default cleanly."""
        executor = _make_pg_executor()
        executor.execute("DELETE FROM t WHERE id < 100", timeout_seconds=5)
        calls = self._execute_calls(executor)
        assert calls[0] == "SET LOCAL statement_timeout = 5000"
        assert calls[1] == "DELETE FROM t WHERE id < 100"

    def test_query_emits_set_local_before_user_sql(self) -> None:
        """``query`` must guard its statement too — a misbehaving SELECT
        with a missing predicate is just as capable of pinning a pool
        connection as a runaway UPDATE."""
        executor = _make_pg_executor()
        cur = _cursor_of(executor)
        cur.fetchall.return_value = []
        executor.query("SELECT * FROM t WHERE id > 0", timeout_seconds=30)
        calls = self._execute_calls(executor)
        assert calls[0] == "SET LOCAL statement_timeout = 30000"
        assert calls[1] == "SELECT * FROM t WHERE id > 0"

    def test_query_dicts_emits_set_local_before_user_sql(self) -> None:
        """``query_dicts`` must guard its statement too."""
        executor = _make_pg_executor()
        cur = _cursor_of(executor)
        cur.description = None
        cur.fetchall.return_value = []
        executor.query_dicts("SELECT * FROM t", timeout_seconds=45)
        calls = self._execute_calls(executor)
        assert calls[0] == "SET LOCAL statement_timeout = 45000"
        assert calls[1] == "SELECT * FROM t"

    def test_timeout_uses_set_local_not_set(self) -> None:
        """``SET`` (no LOCAL) would leak the timeout to the next pool
        caller because connections are reused. ``SET LOCAL`` scopes
        the GUC to the transaction and resets on COMMIT/ROLLBACK —
        the only correct primitive for a pooled executor.
        """
        executor = _make_pg_executor()
        executor.execute("UPDATE t SET x = 1", timeout_seconds=10)
        calls = self._execute_calls(executor)
        timeout_call = calls[0]
        assert timeout_call.startswith("SET LOCAL "), (
            f"Timeout must use SET LOCAL (transaction-scoped); got {timeout_call!r}. "
            "A plain SET would leak the previous caller's timeout to the next "
            "checkout — a healthcheck's 1s cap would kill the next migration."
        )

    def test_timeout_seconds_floors_at_one_millisecond(self) -> None:
        """Defends against ``timeout_seconds=0`` degenerating into 'no
        timeout' (Postgres treats ``statement_timeout=0`` as unlimited).
        """
        executor = _make_pg_executor()
        executor.execute("UPDATE t SET x = 1", timeout_seconds=0)
        calls = self._execute_calls(executor)
        assert calls[0] == "SET LOCAL statement_timeout = 1", (
            f"timeout_seconds=0 must NOT translate to statement_timeout=0 "
            f"(Postgres treats 0 as unlimited, which is the failure mode "
            f"we're guarding against). Got {calls[0]!r}."
        )

    def test_timeout_helper_routes_through_trust_boundary(self) -> None:
        """The SET LOCAL statement is dispatched via ``run_trusted_sql``
        (the documented :class:`LiteralString` cast) — proves we don't
        bypass the trust-boundary helper for internally-generated SQL.
        """
        from databricks_labs_dqx_app.backend import pg_executor as mod

        with patch.object(mod, "run_trusted_sql", wraps=mod.run_trusted_sql) as helper:
            executor = _make_pg_executor()
            executor.execute("UPDATE t SET x = 1", timeout_seconds=60)
            # Two routed calls: SET LOCAL + the user SQL.
            assert helper.call_count == 2
            sqls = [c.args[1] for c in helper.call_args_list]
            assert sqls[0] == "SET LOCAL statement_timeout = 60000"
            assert sqls[1] == "UPDATE t SET x = 1"

    def test_connection_context_manager_does_not_auto_emit_timeout(self) -> None:
        """``connection()`` deliberately leaves timeout to the caller —
        documented in the docstring because the migration runner uses
        it for DDL whose appropriate timeout depends on the work being
        done (a 30s query vs a 10-minute index rebuild).
        """
        executor = _make_pg_executor()
        with executor.connection():
            pass
        # No cursor was opened, so no execute calls fired at all.
        # We're verifying the manager itself doesn't smuggle a SET
        # LOCAL onto the connection behind the caller's back.
        cur = _cursor_of(executor)
        assert cur.execute.call_count == 0

    def test_close_signals_stop_and_closes_pool(self) -> None:
        executor = _make_pg_executor()
        assert not executor._stop.is_set()
        executor.close()
        assert executor._stop.is_set(), "stop event must be set so refresher loop exits"
        executor._pool.close.assert_called_once()

    def test_close_does_not_raise_on_pool_close_error(self) -> None:
        """``close`` is best-effort — pool close failure must be swallowed and logged."""
        executor = _make_pg_executor()
        executor._pool.close.side_effect = RuntimeError("pool already shut down")
        # Must not propagate — Apps platform calls this on shutdown
        # and a raise here would mask the real shutdown reason.
        executor.close()
        assert executor._stop.is_set()


# ===========================================================================
# Misc — sanity coverage for adjacent surface area used by callers
# ===========================================================================


class TestSimpleProperties:
    """The trivial property getters that callers rely on for portability."""

    def test_dialect_is_postgres(self) -> None:
        assert PgExecutor.dialect == "postgres"

    def test_schema_property(self) -> None:
        e = _make_pg_executor(schema="my_schema")
        assert e.schema == "my_schema"

    def test_database_property(self) -> None:
        e = _make_pg_executor(database="my_db")
        assert e.database == "my_db"

    def test_catalog_property_returns_database(self) -> None:
        """``catalog`` aliases ``database`` so portable 3-part FQN code works."""
        e = _make_pg_executor(database="my_db")
        assert e.catalog == "my_db"

    def test_warehouse_id_is_empty_string_for_type_parity(self) -> None:
        """Kept on the class for SqlExecutor type-compatibility — Lakebase has no warehouse."""
        e = _make_pg_executor()
        assert e.warehouse_id == ""

    def test_fqn_returns_two_part_name(self) -> None:
        """Postgres has one catalog per connection — fqn drops the catalog part."""
        e = _make_pg_executor(schema="my_schema")
        assert e.fqn("dq_quality_rules") == "my_schema.dq_quality_rules"

    def test_ts_text_is_identity(self) -> None:
        """Postgres lets ``_to_text`` ISO-format the timestamp on the way out."""
        e = _make_pg_executor()
        assert e.ts_text("created_at") == "created_at"

    def test_json_literal_expr_renders_jsonb_cast(self) -> None:
        e = _make_pg_executor()
        assert e.json_literal_expr('{"k":"v"}') == "'{\"k\":\"v\"}'::jsonb"

    def test_json_literal_expr_escapes_apostrophes(self) -> None:
        e = _make_pg_executor()
        assert e.json_literal_expr("it's") == "'it''s'::jsonb"


class TestTokenHolder:
    """Thread-safe accessor for the rotating OAuth credential."""

    def test_get_returns_initial(self) -> None:
        h = _TokenHolder("initial")
        assert h.token == "initial"

    def test_set_updates_value(self) -> None:
        h = _TokenHolder("initial")
        h.token = "updated"
        assert h.token == "updated"

    def test_concurrent_read_write_does_not_corrupt(self) -> None:
        """Smoke test: the lock prevents torn reads on the str pointer.

        Strings are atomically assigned on CPython, so this is more of
        a contract demonstration than a true thread-safety check —
        but it verifies the lock doesn't deadlock under contention,
        which is what would actually break a long-running app.
        """
        h = _TokenHolder("tok-0")
        # Worker threads can't propagate exceptions to the main test
        # thread automatically — anything they raise becomes a silent
        # ``Thread._invoke_excepthook`` log and the test would falsely
        # pass. We catch ``BaseException`` (the widest catchable) so
        # even KeyboardInterrupt / SystemExit from a buggy lock impl
        # surfaces as a real assertion failure below. See the BLE001
        # policy block in ``pyproject.toml`` — test thread-bridge
        # collectors are the same resilience pattern as production
        # background-thread loops.
        errors: list[BaseException] = []

        def reader() -> None:
            try:
                for _ in range(1000):
                    _ = h.token
            except BaseException as exc:
                errors.append(exc)

        def writer() -> None:
            try:
                for i in range(1000):
                    h.token = f"tok-{i}"
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(3)] + [
            threading.Thread(target=writer) for _ in range(2)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)
            assert not t.is_alive(), "Token holder deadlocked under contention"
        assert not errors, f"Concurrent access raised: {errors}"


class TestGenerateToken:
    """Thin wrapper around ``ws.database.generate_database_credential``."""

    def test_returns_token_from_credential_response(self) -> None:
        ws = MagicMock(name="WorkspaceClient")
        ws.database.generate_database_credential.return_value = MagicMock(token="fresh-tok")
        assert _generate_token(ws, "my-instance") == "fresh-tok"
        # Request ID should be a UUID-shaped string and instance_names a single-item list.
        call_kwargs = ws.database.generate_database_credential.call_args.kwargs
        assert call_kwargs["instance_names"] == ["my-instance"]
        assert isinstance(call_kwargs["request_id"], str)
        assert len(call_kwargs["request_id"]) >= 32

    def test_raises_when_credential_response_has_no_token(self) -> None:
        ws = MagicMock(name="WorkspaceClient")
        ws.database.generate_database_credential.return_value = MagicMock(token=None)
        with pytest.raises(RuntimeError, match="no token"):
            _generate_token(ws, "my-instance")

"""Unit tests for :mod:`backend.sql_executor`.

Focus: the reviewer-requested CRUD-builder helpers (:class:`WhereIn`,
:func:`_build_where`, :func:`_build_insert`, :func:`_build_update`,
:func:`_build_delete`, :func:`_build_count`) and the :class:`SqlExecutor`
methods that delegate to them.

The dialect-specific Postgres shape (``CURRENT_TIMESTAMP`` translation,
ANSI identifier quoting, JSONB casts, etc.) is covered by
``test_pg_executor.py::TestPgCrudBuilders``. The tests here run against
the Delta-flavoured renderer :func:`_render_value` and Delta's backtick
:meth:`SqlExecutor.q`, so they double as regression coverage for the
default value-encoding contract that both dialects inherit.
"""

from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from databricks_labs_dqx_app.backend.sql_executor import (
    RawSql,
    SqlExecutor,
    WhereIn,
    bind_list,
    _build_count,
    _build_delete,
    _build_insert,
    _build_select,
    _build_update,
    _build_where,
    _render_value,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _delta_quote(identifier: str) -> str:
    """Delta backtick quoting, matches :meth:`SqlExecutor.q`."""
    return "`" + identifier.replace("`", "``") + "`"


def _make_sql_executor() -> SqlExecutor:
    """Half-initialised :class:`SqlExecutor` for method-level delegation tests.

    Same rationale as :func:`test_pg_executor._make_pg_executor` — the
    constructor is pure attribute assignment, but going through it
    requires a :class:`WorkspaceClient` we don't need for the SQL-shape
    tests. Skipping it via ``__new__`` keeps these tests hermetic.
    """
    executor = SqlExecutor.__new__(SqlExecutor)
    executor._ws = MagicMock(name="WorkspaceClient")  # noqa: SLF001
    executor._warehouse_id = "test-wh"  # noqa: SLF001
    executor._catalog = "dqx"  # noqa: SLF001
    executor._schema = "public"  # noqa: SLF001
    return executor


def test_query_binds_runtime_value_without_inlining_it() -> None:
    workspace = MagicMock()
    executor = SqlExecutor(workspace, "test-wh", "dqx", "public")
    response = workspace.statement_execution.execute_statement.return_value
    response.status.state = StatementState.SUCCEEDED
    response.result.data_array = [["b1"]]
    value = "x\\' OR 1=1 --"

    rows = executor.query("SELECT id FROM bindings WHERE owner = :owner", parameters={"owner": value})

    assert rows == [["b1"]]
    request = workspace.statement_execution.execute_statement.call_args.kwargs
    assert request["statement"] == "SELECT id FROM bindings WHERE owner = :owner"
    assert request["parameters"] == [StatementParameterListItem(name="owner", type="STRING", value=value)]


def test_fqn_quotes_exotic_identifier_parts() -> None:
    executor = SqlExecutor(MagicMock(), "test-wh", "prod-east", "main;DROP SCHEMA x")

    assert executor.fqn("dq_resolved_rules") == "`prod-east`.`main;DROP SCHEMA x`.dq_resolved_rules"
    assert executor.fqn("dq;DELETE FROM users") == "`prod-east`.`main;DROP SCHEMA x`.`dq;DELETE FROM users`"


def test_execute_binds_runtime_value_without_inlining_it() -> None:
    workspace = MagicMock()
    executor = SqlExecutor(workspace, "test-wh", "dqx", "public")
    workspace.statement_execution.execute_statement.return_value.status.state = StatementState.SUCCEEDED
    value = "x\\' OR 1=1 --"

    executor.execute("UPDATE bindings SET owner = :owner", parameters={"owner": value})

    request = workspace.statement_execution.execute_statement.call_args.kwargs
    assert request["statement"] == "UPDATE bindings SET owner = :owner"
    assert request["parameters"] == [StatementParameterListItem(name="owner", type="STRING", value=value)]


def test_query_dicts_binds_typed_runtime_values() -> None:
    workspace = MagicMock()
    executor = SqlExecutor(workspace, "test-wh", "dqx", "public")
    response = workspace.statement_execution.execute_statement.return_value
    response.status.state = StatementState.SUCCEEDED
    response.result.data_array = [["b1"]]
    response.manifest.schema.columns = [MagicMock(name="id")]
    response.manifest.schema.columns[0].name = "id"

    rows = executor.query_dicts(
        "SELECT id FROM bindings WHERE version = :version AND enabled = :enabled",
        parameters={"version": 3, "enabled": True},
    )

    assert rows == [{"id": "b1"}]
    request = workspace.statement_execution.execute_statement.call_args.kwargs
    assert request["parameters"] == [
        StatementParameterListItem(name="version", type="BIGINT", value="3"),
        StatementParameterListItem(name="enabled", type="BOOLEAN", value="true"),
    ]


def test_bind_list_keeps_values_out_of_sql_markers() -> None:
    executor = SqlExecutor(MagicMock(), "test-wh", "dqx", "public")
    value = "x\\' OR 1=1 --"

    markers, parameters = bind_list(executor.param, "rule_id", [value, "safe"])

    assert markers == ":rule_id_0, :rule_id_1"
    assert value not in markers
    assert parameters == {"rule_id_0": value, "rule_id_1": "safe"}


def test_json_parameter_expression_uses_bound_marker() -> None:
    executor = SqlExecutor(MagicMock(), "test-wh", "dqx", "public")
    assert executor.json_parameter_expr("definition_json") == "parse_json(:definition_json)"


# ===========================================================================
# _build_where
# ===========================================================================


class TestBuildWhere:
    """Predicate rendering — equality, NULL-safety, IN, refusal on empty."""

    def test_single_equality_predicate(self) -> None:
        sql = _build_where({"id": "abc"}, _delta_quote, _render_value)
        assert sql == "`id` = 'abc'"

    def test_multiple_predicates_and_joined(self) -> None:
        sql = _build_where(
            {"id": "abc", "status": "active"},
            _delta_quote,
            _render_value,
        )
        assert sql == "`id` = 'abc' AND `status` = 'active'"

    def test_none_becomes_is_null(self) -> None:
        """None in WHERE must render ``IS NULL`` — ``= NULL`` never matches."""
        sql = _build_where({"deleted_at": None}, _delta_quote, _render_value)
        assert sql == "`deleted_at` IS NULL"

    def test_wherein_expands_to_in_clause(self) -> None:
        sql = _build_where(
            {"rule_id": WhereIn(["r1", "r2", "r3"])},
            _delta_quote,
            _render_value,
        )
        assert sql == "`rule_id` IN ('r1', 'r2', 'r3')"

    def test_wherein_empty_renders_false_to_stay_syntactically_valid(self) -> None:
        """Empty ``IN ()`` is a parse error on both dialects; ``FALSE`` matches nothing safely."""
        sql = _build_where({"rule_id": WhereIn([])}, _delta_quote, _render_value)
        assert sql == "FALSE"

    def test_wherein_accepts_generators_via_materialisation(self) -> None:
        """``WhereIn`` materialises its input so a generator renders deterministically."""
        sql = _build_where(
            {"rule_id": WhereIn(f"r{i}" for i in range(2))},
            _delta_quote,
            _render_value,
        )
        assert sql == "`rule_id` IN ('r0', 'r1')"

    def test_reserved_word_column_is_quoted(self) -> None:
        sql = _build_where({"check": "1"}, _delta_quote, _render_value)
        assert sql == "`check` = '1'"

    def test_string_values_are_ansi_escaped(self) -> None:
        """Single quotes in values must be doubled to prevent injection."""
        sql = _build_where({"name": "O'Brien"}, _delta_quote, _render_value)
        assert sql == "`name` = 'O''Brien'"

    def test_raw_sql_passes_through_verbatim(self) -> None:
        """RawSql values in WHERE flow through the renderer unquoted."""
        sql = _build_where(
            {"created_at": RawSql("current_timestamp()")},
            _delta_quote,
            _render_value,
        )
        assert sql == "`created_at` = current_timestamp()"

    def test_empty_where_is_refused(self) -> None:
        with pytest.raises(ValueError, match="WHERE clause required"):
            _build_where({}, _delta_quote, _render_value)


# ===========================================================================
# _build_insert
# ===========================================================================


class TestBuildInsert:
    """INSERT INTO ... (cols) VALUES (vals) — the plain single-row insert."""

    def test_single_column(self) -> None:
        sql = _build_insert("dq.t", {"id": "abc"}, _delta_quote, _render_value)
        assert sql == "INSERT INTO dq.t (`id`) VALUES ('abc')"

    def test_multiple_columns_preserves_dict_order(self) -> None:
        """Column order must match the dict iteration order so review diffs stay stable."""
        sql = _build_insert(
            "dq.t",
            {"id": "abc", "name": "foo", "count": 3},
            _delta_quote,
            _render_value,
        )
        assert sql == "INSERT INTO dq.t (`id`, `name`, `count`) VALUES ('abc', 'foo', 3)"

    def test_none_becomes_null_literal(self) -> None:
        sql = _build_insert("dq.t", {"id": "abc", "note": None}, _delta_quote, _render_value)
        assert sql == "INSERT INTO dq.t (`id`, `note`) VALUES ('abc', NULL)"

    def test_bool_renders_uppercase(self) -> None:
        sql = _build_insert("dq.t", {"id": "abc", "flag": True}, _delta_quote, _render_value)
        assert "TRUE" in sql
        sql = _build_insert("dq.t", {"id": "abc", "flag": False}, _delta_quote, _render_value)
        assert "FALSE" in sql

    def test_raw_sql_is_inlined_verbatim(self) -> None:
        """RawSql lets callers inject SQL expressions like ``now()`` / ``parse_json(...)``."""
        sql = _build_insert(
            "dq.t",
            {"id": "abc", "created_at": RawSql("now()")},
            _delta_quote,
            _render_value,
        )
        assert "VALUES ('abc', now())" in sql

    def test_empty_values_is_refused(self) -> None:
        with pytest.raises(ValueError, match="insert requires at least one column"):
            _build_insert("dq.t", {}, _delta_quote, _render_value)


# ===========================================================================
# _build_update
# ===========================================================================


class TestBuildUpdate:
    """UPDATE ... SET ... WHERE ... — the reviewer's motivating shape."""

    def test_single_column_single_key(self) -> None:
        """The reviewer's exact example: pin one column on one row."""
        sql = _build_update(
            "dq.applied_rules",
            updates={"pinned_version": 3},
            where={"id": "rule-1"},
            quote=_delta_quote,
            render=_render_value,
        )
        assert sql == "UPDATE dq.applied_rules SET `pinned_version` = 3 WHERE `id` = 'rule-1'"

    def test_multiple_updates_are_comma_joined(self) -> None:
        sql = _build_update(
            "dq.t",
            updates={"status": "approved", "updated_by": "alice"},
            where={"id": "row-1"},
            quote=_delta_quote,
            render=_render_value,
        )
        assert "SET `status` = 'approved', `updated_by` = 'alice'" in sql

    def test_multiple_where_predicates_are_and_joined(self) -> None:
        sql = _build_update(
            "dq.t",
            updates={"status": "approved"},
            where={"binding_id": "b1", "rule_id": "r1"},
            quote=_delta_quote,
            render=_render_value,
        )
        assert "WHERE `binding_id` = 'b1' AND `rule_id` = 'r1'" in sql

    def test_none_in_updates_becomes_null_literal(self) -> None:
        """UPDATE uses ``= NULL`` (which the DB accepts as a valid write)."""
        sql = _build_update(
            "dq.t",
            updates={"note": None},
            where={"id": "row-1"},
            quote=_delta_quote,
            render=_render_value,
        )
        assert "SET `note` = NULL WHERE" in sql

    def test_none_in_where_becomes_is_null(self) -> None:
        """WHERE uses ``IS NULL`` because ``= NULL`` never matches in SQL."""
        sql = _build_update(
            "dq.t",
            updates={"status": "done"},
            where={"deleted_at": None},
            quote=_delta_quote,
            render=_render_value,
        )
        assert "WHERE `deleted_at` IS NULL" in sql

    def test_raw_sql_expression_in_updates(self) -> None:
        """The classic ``updated_at = now()`` audit stamp."""
        sql = _build_update(
            "dq.t",
            updates={"updated_at": RawSql("now()")},
            where={"id": "row-1"},
            quote=_delta_quote,
            render=_render_value,
        )
        assert "SET `updated_at` = now()" in sql

    def test_empty_updates_is_refused(self) -> None:
        with pytest.raises(ValueError, match="update requires at least one column in `updates`"):
            _build_update(
                "dq.t",
                updates={},
                where={"id": "row-1"},
                quote=_delta_quote,
                render=_render_value,
            )

    def test_empty_where_is_refused(self) -> None:
        """Full-table UPDATE is the exact footgun the builder exists to prevent."""
        with pytest.raises(ValueError, match="WHERE clause required"):
            _build_update(
                "dq.t",
                updates={"status": "done"},
                where={},
                quote=_delta_quote,
                render=_render_value,
            )


# ===========================================================================
# _build_delete
# ===========================================================================


class TestBuildDelete:
    """DELETE FROM ... WHERE ... — the most common CRUD shape in the codebase."""

    def test_single_predicate(self) -> None:
        sql = _build_delete(
            "dq.t",
            where={"id": "row-1"},
            quote=_delta_quote,
            render=_render_value,
        )
        assert sql == "DELETE FROM dq.t WHERE `id` = 'row-1'"

    def test_wherein_bulk_delete(self) -> None:
        """The ``WHERE id IN (...)`` shape used by :meth:`RegistryService.delete_builtin_rules`."""
        sql = _build_delete(
            "dq.t",
            where={"rule_id": WhereIn(["r1", "r2"])},
            quote=_delta_quote,
            render=_render_value,
        )
        assert sql == "DELETE FROM dq.t WHERE `rule_id` IN ('r1', 'r2')"

    def test_empty_where_is_refused(self) -> None:
        """Full-table DELETE is the exact footgun the builder exists to prevent."""
        with pytest.raises(ValueError, match="WHERE clause required"):
            _build_delete(
                "dq.t",
                where={},
                quote=_delta_quote,
                render=_render_value,
            )

    def test_multiple_predicates_are_and_joined(self) -> None:
        sql = _build_delete(
            "dq.t",
            where={"binding_id": "b1", "rule_id": "r1"},
            quote=_delta_quote,
            render=_render_value,
        )
        assert sql == "DELETE FROM dq.t WHERE `binding_id` = 'b1' AND `rule_id` = 'r1'"


# ===========================================================================
# _build_count
# ===========================================================================


class TestBuildCount:
    """SELECT COUNT(*) — the homepage-stat pattern."""

    def test_no_where(self) -> None:
        sql = _build_count("dq.t", None, _delta_quote, _render_value)
        assert sql == "SELECT COUNT(*) FROM dq.t"

    def test_empty_where_treated_as_no_where(self) -> None:
        """Empty dict is a legitimate 'no predicate' — the builder must not raise."""
        sql = _build_count("dq.t", {}, _delta_quote, _render_value)
        assert sql == "SELECT COUNT(*) FROM dq.t"

    def test_with_where(self) -> None:
        sql = _build_count(
            "dq.t",
            {"status": "active"},
            _delta_quote,
            _render_value,
        )
        assert sql == "SELECT COUNT(*) FROM dq.t WHERE `status` = 'active'"


# ===========================================================================
# _build_select
# ===========================================================================


class TestBuildSelect:
    """SELECT <cols> FROM <table> [WHERE <where>] — simple projection helper."""

    def test_no_where(self) -> None:
        sql = _build_select("dq.t", ["id", "name"], None, _delta_quote, _render_value)
        assert sql == "SELECT `id`, `name` FROM dq.t"

    def test_with_where(self) -> None:
        sql = _build_select(
            "dq.t",
            ["id"],
            {"status": "active"},
            _delta_quote,
            _render_value,
        )
        assert sql == "SELECT `id` FROM dq.t WHERE `status` = 'active'"

    def test_columns_are_quoted(self) -> None:
        """Reserved-word columns like ``order`` must round-trip through :func:`quote`."""
        sql = _build_select("dq.t", ["order", "check"], None, _delta_quote, _render_value)
        assert sql == "SELECT `order`, `check` FROM dq.t"

    def test_empty_columns_is_refused(self) -> None:
        with pytest.raises(ValueError, match="select requires at least one column"):
            _build_select("dq.t", [], None, _delta_quote, _render_value)


# ===========================================================================
# SqlExecutor method-level delegation
# ===========================================================================


class TestSqlExecutorCrudDelegation:
    """The class methods delegate to the free-function builders correctly.

    We intercept :meth:`execute` / :meth:`query` on the executor and
    verify the SQL string produced by the builder + the delegation
    plumbing (identifier quoting, dialect renderer). Deep behavioural
    coverage lives on the free-function tests above.
    """

    def _capture_execute(self, executor: SqlExecutor) -> list[str]:
        captured: list[str] = []
        executor.execute = lambda sql, **_: captured.append(sql)  # type: ignore[method-assign]
        return captured

    def test_insert_delegates_with_delta_quoting(self) -> None:
        executor = _make_sql_executor()
        captured = self._capture_execute(executor)
        executor.insert("dq.t", values={"id": "abc", "count": 3})
        assert captured == ["INSERT INTO dq.t (`id`, `count`) VALUES (:value_0, :value_1)"]

    def test_insert_binds_untrusted_text(self) -> None:
        executor = _make_sql_executor()
        executor.execute = MagicMock()
        value = "x\\' OR 1=1 --"

        executor.insert("dq.t", values={"owner": value})

        statement = executor.execute.call_args.args[0]
        assert value not in statement
        assert executor.execute.call_args.kwargs["parameters"] == {"value_0": value}

    def test_insert_binds_timestamp_with_cast(self) -> None:
        from databricks_labs_dqx_app.backend import sql_executor

        executor = _make_sql_executor()
        executor.execute = MagicMock()
        value = "2026-07-10 08:00:00' OR 1=1 --"

        executor.insert("dq.t", values={"run_time": sql_executor.TimestampValue(value)})

        call = executor.execute.call_args
        assert "CAST(:value_0 AS TIMESTAMP)" in call.args[0]
        assert value not in call.args[0]
        assert call.kwargs["parameters"] == {"value_0": value}

    def test_update_delegates_with_delta_quoting(self) -> None:
        executor = _make_sql_executor()
        captured = self._capture_execute(executor)
        executor.update("dq.t", updates={"status": "done"}, where={"id": "r1"})
        assert captured == ["UPDATE dq.t SET `status` = :value_0 WHERE `id` = :value_1"]

    def test_delete_delegates_with_delta_quoting(self) -> None:
        executor = _make_sql_executor()
        captured = self._capture_execute(executor)
        executor.delete("dq.t", where={"id": "r1"})
        assert captured == ["DELETE FROM dq.t WHERE `id` = :value_0"]

    def test_update_and_delete_bind_untrusted_text(self) -> None:
        executor = _make_sql_executor()
        executor.execute = MagicMock()
        value = "x\\' OR 1=1 --"

        executor.update("dq.t", updates={"status": value}, where={"id": value})
        update_call = executor.execute.call_args
        assert value not in update_call.args[0]
        assert update_call.kwargs["parameters"] == {"value_0": value, "value_1": value}

        executor.delete("dq.t", where={"id": value})
        delete_call = executor.execute.call_args
        assert value not in delete_call.args[0]
        assert delete_call.kwargs["parameters"] == {"value_0": value}

    def test_upsert_binds_untrusted_text(self) -> None:
        executor = _make_sql_executor()
        executor.execute = MagicMock()
        value = "x\\' OR 1=1 --"

        executor.upsert("dq.t", {"id": value}, {"status": value})

        call = executor.execute.call_args
        assert value not in call.args[0]
        assert value in call.kwargs["parameters"].values()

    def test_upsert_with_audit_binds_untrusted_text(self) -> None:
        executor = _make_sql_executor()
        executor.execute = MagicMock()
        value = "x\\' OR 1=1 --"

        executor.upsert_with_audit("dq.t", {"id": value}, {"updated_by": value})

        call = executor.execute.call_args
        assert value not in call.args[0]
        assert value in call.kwargs["parameters"].values()

    def test_count_delegates_and_parses_query_result(self) -> None:
        executor = _make_sql_executor()
        # ``query`` is what count() calls; return the row-shape the API
        # ships (list-of-list of stringified cells).
        executor.query = MagicMock(return_value=[["42"]])  # type: ignore[method-assign]
        assert executor.count("dq.t", where={"status": "active"}) == 42
        assert executor.query.call_args.args[0] == "SELECT COUNT(*) FROM dq.t WHERE `status` = :where_0"
        assert executor.query.call_args.kwargs["parameters"] == {"where_0": "active"}

    def test_count_returns_zero_on_empty_result(self) -> None:
        """Defensive: COUNT should always return a row, but shield the caller if it doesn't."""
        executor = _make_sql_executor()
        executor.query = MagicMock(return_value=[])  # type: ignore[method-assign]
        assert executor.count("dq.t") == 0

    def test_select_rows_delegates_to_query(self) -> None:
        executor = _make_sql_executor()
        executor.query = MagicMock(return_value=[["r1", "one"], ["r2", "two"]])  # type: ignore[method-assign]
        rows = executor.select_rows("dq.t", ["id", "name"], where={"status": "active"})
        assert rows == [["r1", "one"], ["r2", "two"]]
        assert executor.query.call_args.args[0] == "SELECT `id`, `name` FROM dq.t WHERE `status` = :where_0"
        assert executor.query.call_args.kwargs["parameters"] == {"where_0": "active"}

    def test_select_rows_binds_untrusted_where_value(self) -> None:
        executor = _make_sql_executor()
        executor.query = MagicMock(return_value=[])  # type: ignore[method-assign]
        value = "x\\' OR 1=1 --"
        executor.select_rows("dq.t", ["id"], where={"name": value})
        assert value not in executor.query.call_args.args[0]
        assert executor.query.call_args.kwargs["parameters"] == {"where_0": value}

    def test_select_dicts_delegates_to_query_dicts(self) -> None:
        executor = _make_sql_executor()
        executor.query_dicts = MagicMock(return_value=[{"id": "r1"}])  # type: ignore[method-assign]
        rows = executor.select_dicts("dq.t", ["id"])
        assert rows == [{"id": "r1"}]
        assert executor.query_dicts.call_args.args[0] == "SELECT `id` FROM dq.t"

    def test_count_returns_zero_on_null_cell(self) -> None:
        executor = _make_sql_executor()
        executor.query = MagicMock(return_value=[[None]])  # type: ignore[method-assign]
        assert executor.count("dq.t") == 0

    def test_update_refuses_empty_where(self) -> None:
        executor = _make_sql_executor()
        self._capture_execute(executor)
        with pytest.raises(ValueError, match="WHERE clause required"):
            executor.update("dq.t", updates={"status": "done"}, where={})

    def test_delete_refuses_empty_where(self) -> None:
        executor = _make_sql_executor()
        self._capture_execute(executor)
        with pytest.raises(ValueError, match="WHERE clause required"):
            executor.delete("dq.t", where={})

    def test_insert_refuses_empty_values(self) -> None:
        executor = _make_sql_executor()
        self._capture_execute(executor)
        with pytest.raises(ValueError, match="insert requires at least one column"):
            executor.insert("dq.t", values={})

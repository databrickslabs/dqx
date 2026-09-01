"""Schema tests for monitored tables, applied rules, and data products.

Layer 2 of the Rules Registry design plus the Data Products grouping on top
of it — see docs/superpowers/specs/2026-07-02-rules-registry-design.md
§3.1/§7 and docs/superpowers/specs/2026-07-07-data-products-design.md §3.

Every table below ships on both baselines: the Postgres v1 baseline and the
Delta OLTP fallback. The services that read these tables are backend-agnostic,
so each table's column set is asserted exactly *and* compared across the two
backends — a column added to one baseline only would break whichever deploy
shape didn't get it.
"""

import pytest

from databricks_labs_dqx_app.backend.migrations import _V2_OLTP_FALLBACK
from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS

_PG = PG_MIGRATIONS[0].sql
_DELTA = _V2_OLTP_FALLBACK

# Both baselines, for assertions that must hold on either backend.
_BOTH = [pytest.param(_PG, id="postgres"), pytest.param(_DELTA, id="delta")]

_NON_COLUMN_KEYWORDS = {"CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"}


def _create_stmt(sql: str, table: str) -> str:
    """The one CREATE TABLE statement for ``table``, whitespace-normalized."""
    stmts = [" ".join(s.split()) for s in sql.split(";")]
    matches = [s for s in stmts if s.startswith("CREATE TABLE") and f".{table} (" in s]
    assert len(matches) == 1, f"expected one CREATE TABLE for {table}, found {len(matches)}"
    return matches[0]


def _paren_body(ddl: str) -> str:
    """The text inside a CREATE TABLE's outermost parentheses.

    Scanning for the *matching* close paren rather than the last one matters
    on Delta, where the statement continues past it with ``CLUSTER BY (...)``.
    """
    start = ddl.index("(")
    depth = 0
    for offset in range(start, len(ddl)):
        if ddl[offset] == "(":
            depth += 1
        elif ddl[offset] == ")":
            depth -= 1
            if depth == 0:
                return ddl[start + 1 : offset]
    raise AssertionError(f"unbalanced parentheses in: {ddl[:80]}")


def _columns(sql: str, table: str) -> set[str]:
    """Column names ``table`` declares, ignoring table-level constraints.

    Quoting is stripped so a column that has to be escaped in one dialect
    (``"check"`` on Postgres, ```check``` on Delta) compares equal across
    backends.
    """
    names: set[str] = set()
    depth = 0
    field = ""
    for char in _paren_body(_create_stmt(sql, table)):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            _collect_column(field, names)
            field = ""
        else:
            field += char
    _collect_column(field, names)
    return names


def _collect_column(field: str, names: set[str]) -> None:
    tokens = field.split()
    if tokens and tokens[0].upper() not in _NON_COLUMN_KEYWORDS:
        names.add(tokens[0].strip('"`'))


class TestDqMonitoredTables:
    """The thin binding recording that a table is under active governance."""

    EXPECTED = {
        "binding_id",
        "table_fqn",
        "owner",
        "owner_display_name",
        "status",
        "version",
        "schedule_cron",
        "schedule_tz",
        "schedule_kind",
        "schedule_sample_size",
        "last_profiled_at",
        "last_run_at",
        "pending_rationale",
        "last_decision_rationale",
        "created_by",
        "created_at",
        "updated_by",
        "updated_at",
    }

    @pytest.mark.parametrize("baseline", _BOTH)
    def test_columns(self, baseline: str) -> None:
        assert _columns(baseline, "dq_monitored_tables") == self.EXPECTED

    def test_table_fqn_unique_on_postgres(self) -> None:
        # Delta has no UNIQUE support — the service enforces one binding per
        # table_fqn there. Postgres gets the real constraint.
        ddl = _create_stmt(_PG, "dq_monitored_tables")
        assert "uq_dq_monitored_tables_table_fqn" in ddl
        assert "UNIQUE (table_fqn)" in ddl

    def test_status_check_is_the_four_state_review_set(self) -> None:
        four_state = "CHECK (status IN ('draft','pending_approval','approved','rejected'))"
        # Inline on Postgres; a following ALTER on Delta (only PK/FK go inline).
        assert four_state in _create_stmt(_PG, "dq_monitored_tables")
        assert "chk_dq_monitored_tables_status" in _DELTA
        assert four_state in " ".join(_DELTA.split())


class TestDqAppliedRules:
    """The live link between a published registry rule and a table's mapping."""

    EXPECTED = {
        "id",
        "binding_id",
        "rule_id",
        "pinned_version",
        "severity_override",
        "row_filter",
        "pass_threshold",
        "column_mapping",
        "user_metadata",
        "mapping_hash",
        "created_by",
        "created_at",
    }

    @pytest.mark.parametrize("baseline", _BOTH)
    def test_columns(self, baseline: str) -> None:
        assert _columns(baseline, "dq_applied_rules") == self.EXPECTED

    def test_unique_binding_rule_mapping_hash_on_postgres(self) -> None:
        ddl = _create_stmt(_PG, "dq_applied_rules")
        assert "uq_dq_applied_rules_binding_rule_mapping" in ddl
        assert "UNIQUE (binding_id, rule_id, mapping_hash)" in ddl


class TestDqQualityRulesProvenance:
    @pytest.mark.parametrize("baseline", _BOTH)
    def test_provenance_columns_present(self, baseline: str) -> None:
        cols = _columns(baseline, "dq_quality_rules")
        assert {"registry_rule_id", "registry_version", "applied_rule_id"} <= cols


class TestDqMonitoredTableVersions:
    """Frozen snapshot of a binding, taken on publish."""

    EXPECTED = {
        "id",
        "binding_id",
        "version",
        "state_json",
        "created_by",
        "created_at",
        "refrozen_at",
    }

    @pytest.mark.parametrize("baseline", _BOTH)
    def test_columns(self, baseline: str) -> None:
        cols = _columns(baseline, "dq_monitored_table_versions")
        assert cols == self.EXPECTED
        # Reference-based snapshot: no frozen copy of the rendered rule set.
        assert "checks_json" not in cols

    def test_unique_binding_version_on_postgres(self) -> None:
        assert "UNIQUE (binding_id, version)" in _create_stmt(_PG, "dq_monitored_table_versions")


class TestDqDataProducts:
    """A named grouping of monitored tables, reviewed and run as one unit."""

    EXPECTED = {
        "product_id",
        "name",
        "description",
        "owner",
        "owner_display_name",
        "schedule_cron",
        "schedule_tz",
        "schedule_kind",
        "schedule_sample_size",
        "status",
        "version",
        "pending_rationale",
        "last_decision_rationale",
        "created_by",
        "created_at",
        "updated_by",
        "updated_at",
    }

    @pytest.mark.parametrize("baseline", _BOTH)
    def test_columns(self, baseline: str) -> None:
        assert _columns(baseline, "dq_data_products") == self.EXPECTED

    def test_name_unique_on_postgres(self) -> None:
        assert "UNIQUE (name)" in _create_stmt(_PG, "dq_data_products")

    def test_status_check_is_the_four_state_review_set(self) -> None:
        four_state = "CHECK (status IN ('draft','pending_approval','approved','rejected'))"
        assert four_state in _create_stmt(_PG, "dq_data_products")
        assert "chk_dq_data_products_status" in _DELTA
        assert four_state in " ".join(_DELTA.split())


class TestDqDataProductMembers:
    EXPECTED = {"id", "product_id", "binding_id", "pinned_version"}

    @pytest.mark.parametrize("baseline", _BOTH)
    def test_columns(self, baseline: str) -> None:
        assert _columns(baseline, "dq_data_product_members") == self.EXPECTED

    def test_unique_product_binding_on_postgres(self) -> None:
        assert "UNIQUE (product_id, binding_id)" in _create_stmt(_PG, "dq_data_product_members")


class TestDqRunSets:
    """One row per product-level run, grouping the per-binding runs."""

    EXPECTED = {
        "run_set_id",
        "product_id",
        "product_version",
        "source",
        "trigger",
        "created_by",
        "created_at",
    }

    @pytest.mark.parametrize("baseline", _BOTH)
    def test_columns(self, baseline: str) -> None:
        # ``trigger`` is a reserved word on Postgres and needs quoting there;
        # _columns strips the quoting so both backends compare equal.
        assert _columns(baseline, "dq_run_sets") == self.EXPECTED

    @pytest.mark.parametrize("baseline", _BOTH)
    def test_source_and_trigger_checks(self, baseline: str) -> None:
        sql = " ".join(baseline.split())
        assert "CHECK (source IN ('approved','draft'))" in sql
        assert "'manual'" in sql
        assert "'scheduled'" in sql


class TestDqRunSetMembers:
    EXPECTED = {"id", "run_set_id", "run_id", "binding_id", "binding_version"}

    @pytest.mark.parametrize("baseline", _BOTH)
    def test_columns(self, baseline: str) -> None:
        assert _columns(baseline, "dq_run_set_members") == self.EXPECTED

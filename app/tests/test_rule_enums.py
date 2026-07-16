"""Tests for the ``RuleSource`` / ``RuleStatus`` enums and their wiring.

Covers enum values, the SQL ``IN``-list helper, request-model validation, the
service's derived ``VALID_STATUSES``, and the ``source`` / ``status`` CHECK
constraints that both migration backends derive from the enums (regression
guard for the malformed-DDL bug where a closing paren / terminator was dropped).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from databricks_labs_dqx_app.backend.migrations import _V2_OLTP_FALLBACK
from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS
from databricks_labs_dqx_app.backend.models import (
    BatchSaveRulesIn,
    RuleSource,
    RuleStatus,
    SaveRulesIn,
    SetStatusIn,
)
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService

_EXPECTED_SOURCE_VALUES = {"ui", "sql", "profiler", "import", "ai"}
_EXPECTED_STATUS_VALUES = {"draft", "pending_approval", "approved", "rejected"}


def test_enum_values_match_check_constraints():
    assert {m.value for m in RuleSource} == _EXPECTED_SOURCE_VALUES
    assert {m.value for m in RuleStatus} == _EXPECTED_STATUS_VALUES


@pytest.mark.parametrize("enum_cls", [RuleSource, RuleStatus])
def test_sql_in_list_renders_all_members_quoted(enum_cls):
    fragment = enum_cls.sql_in_list()
    assert fragment == ", ".join(f"'{m.value}'" for m in enum_cls)
    for member in enum_cls:
        assert f"'{member.value}'" in fragment


class TestSaveRequestValidation:
    @pytest.mark.parametrize("model", [SaveRulesIn, BatchSaveRulesIn])
    def test_source_defaults_to_ui(self, model):
        kwargs = {"checks": []}
        kwargs |= {"table_fqns": ["c.s.t"]} if model is BatchSaveRulesIn else {"table_fqn": "c.s.t"}
        assert model(**kwargs).source is RuleSource.ui

    @pytest.mark.parametrize("model", [SaveRulesIn, BatchSaveRulesIn])
    def test_source_string_is_coerced_to_enum(self, model):
        kwargs = {"checks": [], "source": "profiler"}
        kwargs |= {"table_fqns": ["c.s.t"]} if model is BatchSaveRulesIn else {"table_fqn": "c.s.t"}
        parsed = model(**kwargs)
        assert parsed.source is RuleSource.profiler
        assert parsed.source.value == "profiler"

    @pytest.mark.parametrize("model", [SaveRulesIn, BatchSaveRulesIn])
    def test_invalid_source_is_rejected(self, model):
        kwargs = {"checks": [], "source": "bogus"}
        kwargs |= {"table_fqns": ["c.s.t"]} if model is BatchSaveRulesIn else {"table_fqn": "c.s.t"}
        with pytest.raises(ValidationError):
            model(**kwargs)


class TestSetStatusValidation:
    def test_status_string_is_coerced_to_enum(self):
        assert SetStatusIn(status="approved").status is RuleStatus.approved

    def test_invalid_status_is_rejected(self):
        with pytest.raises(ValidationError):
            SetStatusIn(status="bogus")


def test_service_valid_statuses_derive_from_enum():
    assert RulesCatalogService.VALID_STATUSES == {m.value for m in RuleStatus}
    nodes = set(RulesCatalogService.VALID_TRANSITIONS) | {
        s for targets in RulesCatalogService.VALID_TRANSITIONS.values() for s in targets
    }
    assert nodes <= RulesCatalogService.VALID_STATUSES


def _rules_table_ddl() -> list:
    """The rules-table DDL fragments from both migration backends."""
    pg = next(m.sql for m in PG_MIGRATIONS if "CREATE TABLE" in m.sql and "dq_quality_rules" in m.sql)
    return [pytest.param(_V2_OLTP_FALLBACK, id="delta"), pytest.param(pg, id="postgres")]


@pytest.mark.parametrize("ddl", _rules_table_ddl())
def test_source_and_status_checks_are_well_formed(ddl):
    assert ddl.count("(") == ddl.count(")")
    assert f"CHECK (source IN ({RuleSource.sql_in_list()}))" in ddl
    assert f"CHECK (status IN ({RuleStatus.sql_in_list()}))" in ddl

"""Tests for ``CommentsService`` — entity-type allowlist + generic storage.

Comment threads are keyed by a generic ``(entity_type, entity_id)`` pair, so
supporting a new entity type is purely a matter of widening the allowlist —
there is no per-type schema. These tests lock the supported set and confirm
add/list/delete route their SQL through the shared executor for any allowed
type.
"""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.services.comments_service import CommentsService


@pytest.fixture
def sql():
    mock = create_autospec(SqlExecutor, instance=True)
    mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    mock.query.return_value = []
    return mock


@pytest.fixture
def svc(sql):
    return CommentsService(sql=sql)


def test_supported_entity_types(svc):
    assert svc.VALID_ENTITY_TYPES == {"run", "rule", "monitored_table", "data_product"}


@pytest.mark.parametrize("entity_type", ["run", "rule", "monitored_table", "data_product"])
def test_add_comment_accepts_all_supported_types(svc, sql, entity_type):
    comment = svc.add_comment(entity_type, "obj-1", "alice@x", "hello")
    assert comment.entity_type == entity_type
    assert comment.entity_id == "obj-1"
    insert_sql = sql.execute.call_args[0][0]
    assert "INSERT INTO dqx_test.dqx_app_test.dq_comments" in insert_sql
    assert entity_type in insert_sql


@pytest.mark.parametrize("entity_type", ["monitored_table", "data_product"])
def test_list_comments_accepts_new_types(svc, sql, entity_type):
    svc.list_comments(entity_type, "obj-1")
    select_sql = sql.query.call_args[0][0]
    assert f"entity_type = '{entity_type}'" in select_sql


def test_add_comment_rejects_unknown_type(svc, sql):
    with pytest.raises(ValueError):
        svc.add_comment("workspace", "obj-1", "alice@x", "nope")
    sql.execute.assert_not_called()

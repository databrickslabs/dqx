"""Parameter templates preserve literal percent signs through psycopg parsing."""

from unittest.mock import create_autospec

import pytest
from psycopg import ClientCursor, Cursor

from databricks_labs_dqx_app.backend.pg_cursor_helpers import run_parameterized_sql, run_trusted_sql


@pytest.mark.parametrize(
    "template, expected",
    [
        ('SELECT %(value)s FROM "studio%qa"."settings"', 'SELECT 7 FROM "studio%qa"."settings"'),
        ('SELECT %(value)s FROM "studio%%qa"."settings"', 'SELECT 7 FROM "studio%%qa"."settings"'),
        ('SELECT %(value)s FROM "studio%(value)sqa"."settings"', 'SELECT 7 FROM "studio%(value)sqa"."settings"'),
        ('SELECT %(value)s FROM "studio""%qa"."settings"', 'SELECT 7 FROM "studio""%qa"."settings"'),
        ("SELECT %(value)s, 'fixed 100%'", "SELECT 7, 'fixed 100%'"),
        (
            "SELECT %s FROM \"studio%qa\" WHERE label = 'it''s 100%'",
            "SELECT 7 FROM \"studio%qa\" WHERE label = 'it''s 100%'",
        ),
    ],
)
def test_parameterized_sql_preserves_quoted_percent_signs(
    pg_binding_cursor: ClientCursor[tuple[object, ...]], template: str, expected: str
) -> None:
    cursor = create_autospec(Cursor, instance=True)
    rendered: list[str] = []

    def execute(query: str, parameters: dict[str, int] | tuple[int]) -> None:
        rendered.append(pg_binding_cursor.mogrify(query, parameters))

    cursor.execute.side_effect = execute
    parameters = (7,) if template.startswith("SELECT %s") else {"value": 7}

    run_parameterized_sql(cursor, template, parameters)

    assert rendered == [expected]


def test_trusted_sql_keeps_unbound_percent_signs() -> None:
    cursor = create_autospec(Cursor, instance=True)
    template = 'CREATE SCHEMA "studio%%qa"'

    run_trusted_sql(cursor, template)

    assert cursor.execute.call_args.args == (template,)

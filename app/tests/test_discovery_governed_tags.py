"""Tests for ``DiscoveryService.list_governed_tags``.

Covers: parsing ``SHOW GOVERNED TAGS`` rows into ``GovernedTag(tag, description)``
including value-bearing tags (bare key + key=value entries), empty description →
``None``, empty/whitespace key skipping, defensive ``Values`` JSON parsing, dedup
+ sort, and defensive behaviour when the SQL query raises (returns ``[]``, never
raises) or no SQL executor is injected.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService, GovernedTag


def _svc(rows: list[list[object]] | Exception | None) -> DiscoveryService:
    ws = MagicMock()
    if rows is None:
        return DiscoveryService(ws=ws, user_id="tester", sql=None)
    sql = MagicMock()
    if isinstance(rows, Exception):
        sql.query.side_effect = rows
    else:
        sql.query.return_value = rows
    return DiscoveryService(ws=ws, user_id="tester", sql=sql)


class TestListGovernedTags:
    def test_parses_rows_into_governed_tags_with_values(self):
        """Emits the bare key plus key=value per value, carrying the description."""
        svc = _svc(
            [
                ["class.age", "id1", "Age band.", '["minor", "adult"]', None, None],
                ["cost_center", "id2", "Cost center.", "[]", None, None],
                ["pii", "id3", "", None, None, None],
            ]
        )
        result = svc.list_governed_tags()
        assert result == [
            GovernedTag(tag="class.age", description="Age band."),
            GovernedTag(tag="class.age=adult", description="Age band."),
            GovernedTag(tag="class.age=minor", description="Age band."),
            GovernedTag(tag="cost_center", description="Cost center."),
            GovernedTag(tag="pii", description=None),
        ]

    def test_empty_description_becomes_none(self):
        svc = _svc([["t", "id", "   ", "[]", None, None]])
        assert svc.list_governed_tags() == [GovernedTag(tag="t", description=None)]

    def test_skips_empty_and_whitespace_keys(self):
        svc = _svc(
            [
                ["  ", "id1", "d", '["x"]', None, None],
                ["", "id2", "d", None, None, None],
                ["ok", "id3", "desc", '["v"]', None, None],
            ]
        )
        assert svc.list_governed_tags() == [
            GovernedTag(tag="ok", description="desc"),
            GovernedTag(tag="ok=v", description="desc"),
        ]

    def test_defensive_values_parsing(self):
        """Unparseable or non-list ``Values`` yields only the bare key."""
        svc = _svc(
            [
                ["a", "id1", "da", "not-json", None, None],
                ["b", "id2", "db", '{"not": "a list"}', None, None],
                ["c", "id3", "dc", None, None, None],
            ]
        )
        assert svc.list_governed_tags() == [
            GovernedTag(tag="a", description="da"),
            GovernedTag(tag="b", description="db"),
            GovernedTag(tag="c", description="dc"),
        ]

    def test_deduplicates_first_description_wins_and_sorts(self):
        svc = _svc(
            [
                ["z", "id1", "dz", '["1"]', None, None],
                ["a", "id2", "first", '["1", "1"]', None, None],
                ["a", "id3", "second", None, None, None],
            ]
        )
        assert svc.list_governed_tags() == [
            GovernedTag(tag="a", description="first"),
            GovernedTag(tag="a=1", description="first"),
            GovernedTag(tag="z", description="dz"),
            GovernedTag(tag="z=1", description="dz"),
        ]

    def test_returns_empty_and_does_not_raise_when_query_fails(self):
        svc = _svc(RuntimeError("SHOW GOVERNED TAGS unavailable"))
        assert svc.list_governed_tags() == []

    def test_returns_empty_when_no_sql_executor(self):
        svc = _svc(None)
        assert svc.list_governed_tags() == []

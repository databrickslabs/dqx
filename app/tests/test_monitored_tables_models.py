"""Tests for the monitored-tables / applied-rules domain model (Phase 3A).

Covers the Pydantic 2 domain models ``MonitoredTable`` and ``AppliedRule``
plus the ``compute_mapping_hash`` dedup helper. No DB, no Spark — pure model
construction/validation, mirroring ``test_registry_models.py``'s style.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# MonitoredTable
# ---------------------------------------------------------------------------


class TestMonitoredTable:
    @pytest.fixture
    def MonitoredTable(self):
        from databricks_labs_dqx_app.backend.registry_models import MonitoredTable

        return MonitoredTable

    def test_valid_construction(self, MonitoredTable):
        table = MonitoredTable(
            binding_id="b1",
            table_fqn="cat.schema.tbl",
            steward="alice@example.com",
            status="draft",
        )
        assert table.binding_id == "b1"
        assert table.table_fqn == "cat.schema.tbl"
        assert table.steward == "alice@example.com"
        assert table.status == "draft"
        assert table.last_profiled_at is None

    def test_defaults(self, MonitoredTable):
        table = MonitoredTable(binding_id="b1", table_fqn="cat.schema.tbl")
        assert table.status == "draft"
        assert table.steward is None
        assert table.created_by is None
        assert table.updated_at is None

    @pytest.mark.parametrize("status", ["draft", "pending_approval", "approved", "rejected"])
    def test_accepts_valid_statuses(self, MonitoredTable, status):
        assert MonitoredTable(binding_id="b1", table_fqn="t", status=status).status == status

    def test_rejects_invalid_status(self, MonitoredTable):
        with pytest.raises(ValidationError):
            MonitoredTable(binding_id="b1", table_fqn="t", status="live")

    def test_requires_table_fqn(self, MonitoredTable):
        with pytest.raises(ValidationError):
            MonitoredTable(binding_id="b1")


# ---------------------------------------------------------------------------
# AppliedRule
# ---------------------------------------------------------------------------


class TestAppliedRule:
    @pytest.fixture
    def AppliedRule(self):
        from databricks_labs_dqx_app.backend.registry_models import AppliedRule

        return AppliedRule

    def test_valid_construction(self, AppliedRule):
        rule = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            pinned_version=3,
            severity_override="High",
            column_mapping=[{"column": "id"}],
            user_metadata={"note": "hello"},
            mapping_hash="deadbeef",
        )
        assert rule.id == "ar1"
        assert rule.binding_id == "b1"
        assert rule.rule_id == "r1"
        assert rule.pinned_version == 3
        assert rule.severity_override == "High"
        assert rule.column_mapping == [{"column": "id"}]
        assert rule.user_metadata == {"note": "hello"}
        assert rule.mapping_hash == "deadbeef"

    def test_defaults(self, AppliedRule):
        rule = AppliedRule(binding_id="b1", rule_id="r1")
        assert rule.id is None
        assert rule.pinned_version is None
        assert rule.severity_override is None
        assert rule.column_mapping == []
        assert rule.user_metadata == {}
        assert rule.mapping_hash is None
        assert rule.created_by is None

    def test_pinned_version_none_means_follow_latest(self, AppliedRule):
        rule = AppliedRule(binding_id="b1", rule_id="r1", pinned_version=None)
        assert rule.pinned_version is None

    def test_multiple_mapping_groups(self, AppliedRule):
        rule = AppliedRule(
            binding_id="b1",
            rule_id="r1",
            column_mapping=[{"column": "a"}, {"column": "b"}],
        )
        assert len(rule.column_mapping) == 2

    def test_requires_rule_id(self, AppliedRule):
        with pytest.raises(ValidationError):
            AppliedRule(binding_id="b1")


# ---------------------------------------------------------------------------
# compute_mapping_hash
# ---------------------------------------------------------------------------


class TestComputeMappingHash:
    @pytest.fixture
    def compute_mapping_hash(self):
        from databricks_labs_dqx_app.backend.registry_models import compute_mapping_hash

        return compute_mapping_hash

    def test_deterministic(self, compute_mapping_hash):
        mapping = [{"column": "id"}, {"reference_column": "ref_id"}]
        assert compute_mapping_hash(mapping) == compute_mapping_hash(mapping)

    def test_hex_sha256_shape(self, compute_mapping_hash):
        digest = compute_mapping_hash([{"column": "id"}])
        assert len(digest) == 64
        int(digest, 16)  # raises ValueError if not valid hex

    def test_empty_mapping_is_stable(self, compute_mapping_hash):
        assert compute_mapping_hash([]) == compute_mapping_hash([])

    def test_order_insensitive_within_group(self, compute_mapping_hash):
        group_a = {"column": "id", "reference_column": "ref_id"}
        group_b = {"reference_column": "ref_id", "column": "id"}
        assert compute_mapping_hash([group_a]) == compute_mapping_hash([group_b])

    def test_order_insensitive_across_groups(self, compute_mapping_hash):
        mapping_a = [{"column": "a"}, {"column": "b"}]
        mapping_b = [{"column": "b"}, {"column": "a"}]
        assert compute_mapping_hash(mapping_a) == compute_mapping_hash(mapping_b)

    def test_sensitive_to_value_change(self, compute_mapping_hash):
        assert compute_mapping_hash([{"column": "a"}]) != compute_mapping_hash([{"column": "b"}])

    def test_sensitive_to_key_change(self, compute_mapping_hash):
        assert compute_mapping_hash([{"column": "a"}]) != compute_mapping_hash([{"other_slot": "a"}])

    def test_sensitive_to_group_count(self, compute_mapping_hash):
        assert compute_mapping_hash([{"column": "a"}]) != compute_mapping_hash([{"column": "a"}, {"column": "a"}])

    def test_distinguishes_duplicate_groups_from_single(self, compute_mapping_hash):
        # Two identical groups (rule applied twice to the same column) must
        # NOT collapse into one — group count is semantically meaningful.
        single = compute_mapping_hash([{"column": "a"}])
        doubled = compute_mapping_hash([{"column": "a"}, {"column": "a"}])
        assert single != doubled

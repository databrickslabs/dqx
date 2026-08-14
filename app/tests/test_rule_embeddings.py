"""Tests for ``services/rule_embeddings.py`` — Rules Registry Phase 4B.

Covers ``build_rule_embed_text`` (pure function) and ``RuleEmbeddingsService``
(deploy-safe: every method is a documented no-op when
``embedding_endpoint_name`` is unconfigured).
"""

import json
from types import SimpleNamespace
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.registry_models import RegistryRule, RuleDefinition
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.rule_embeddings import (
    RuleEmbeddingsService,
    build_rule_embed_text,
)


def _rule(
    rule_id: str = "r1",
    slot_names: list[str] | None = None,
    slot_families: list[str] | None = None,
    slot_cardinalities: list[str] | None = None,
    body: dict | None = None,
    user_metadata: dict | None = None,
    version: int = 1,
) -> RegistryRule:
    slot_names = slot_names if slot_names is not None else ["column"]
    slot_families = slot_families if slot_families is not None else ["any"] * len(slot_names)
    slot_cardinalities = slot_cardinalities if slot_cardinalities is not None else ["one"] * len(slot_names)
    definition = RuleDefinition.model_validate(
        {
            "body": body or {"function": "is_not_null", "arguments": {n: f"{{{{{n}}}}}" for n in slot_names}},
            "slots": [
                {"name": n, "family": f, "position": i, "cardinality": c}
                for i, (n, f, c) in enumerate(zip(slot_names, slot_families, slot_cardinalities))
            ],
            "parameters": [],
        }
    )
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status="approved",
        version=version,
        definition=definition,
        user_metadata=user_metadata if user_metadata is not None else {"name": "Not Null Check", "severity": "High"},
    )


class TestBuildRuleEmbedText:
    def test_includes_name_description_dimension(self):
        rule = _rule(
            user_metadata={
                "name": "Email is valid",
                "description": "Checks the email column matches a valid format",
                "dimension": "Validity",
                "severity": "High",
            }
        )

        text = build_rule_embed_text(rule)

        assert "Email is valid" in text
        assert "Checks the email column matches a valid format" in text
        assert "dimension: Validity" in text

    def test_omits_severity_even_when_rule_has_severity_tag(self):
        rule = _rule(
            user_metadata={
                "name": "Not Null",
                "severity": "Critical",
            }
        )

        text = build_rule_embed_text(rule)

        assert "severity:" not in text

    def test_includes_slot_family_and_cardinality_with_input_columns_label(self):
        rule = _rule(
            slot_names=["column"],
            slot_families=["text"],
            slot_cardinalities=["one"],
        )

        text = build_rule_embed_text(rule)

        assert "input columns:" in text
        assert "column (text, one)" in text

    def test_includes_multiple_slots_with_different_families(self):
        rule = _rule(
            slot_names=["start_date", "end_date"],
            slot_families=["temporal", "temporal"],
            slot_cardinalities=["one", "one"],
        )

        text = build_rule_embed_text(rule)

        assert "input columns:" in text
        assert "start_date (temporal, one)" in text
        assert "end_date (temporal, one)" in text

    def test_does_not_include_old_slots_format(self):
        rule = _rule(slot_names=["column_a", "column_b"])

        text = build_rule_embed_text(rule)

        assert "slots: column_a, column_b" not in text

    def test_includes_check_function_name(self):
        rule = _rule(body={"function": "is_not_null", "arguments": {}})

        text = build_rule_embed_text(rule)

        assert "check: is_not_null" in text

    def test_includes_check_function_for_regexp_match(self):
        rule = _rule(body={"function": "regexp_match", "arguments": {"column": "{{column}}", "regex": ".*@.*"}})

        text = build_rule_embed_text(rule)

        assert "check: regexp_match" in text

    def test_no_check_line_when_body_has_no_function(self):
        rule = _rule(body={"sql_query": "SELECT 1", "arguments": {}}, slot_names=[])

        text = build_rule_embed_text(rule)

        assert "check:" not in text

    def test_includes_free_text_tags_but_not_reserved_keys_twice(self):
        rule = _rule(user_metadata={"name": "Rule", "team": "data-platform"})

        text = build_rule_embed_text(rule)

        assert "team: data-platform" in text
        assert text.count("Rule") == 1

    def test_truncates_long_sql_predicate(self):
        long_predicate = "a" * 1000
        rule = _rule(body={"sql_query": long_predicate}, slot_names=[])

        text = build_rule_embed_text(rule)

        assert len(text) < 1000 + 50

    def test_extracts_arguments_when_no_predicate_field(self):
        rule = _rule(body={"function": "is_in_range", "arguments": {"min_limit": 0, "max_limit": 100}})

        text = build_rule_embed_text(rule)

        assert "predicate:" in text

    def test_empty_rule_returns_non_crashing_text(self):
        rule = _rule(user_metadata={}, slot_names=[], body={})

        text = build_rule_embed_text(rule)

        assert isinstance(text, str)


@pytest.fixture
def app_settings() -> create_autospec:
    return create_autospec(AppSettingsService, instance=True)


@pytest.fixture
def sp_ws() -> create_autospec:
    return create_autospec(WorkspaceClient, instance=True)


@pytest.fixture
def svc(sql_executor_mock, sp_ws, app_settings):
    sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    return RuleEmbeddingsService(sql=sql_executor_mock, sp_ws=sp_ws, app_settings=app_settings)


class TestIsConfigured:
    def test_false_when_endpoint_unset(self, svc, app_settings):
        app_settings.get_embedding_endpoint_name.return_value = ""
        assert svc.is_configured() is False

    def test_true_when_endpoint_set(self, svc, app_settings):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"
        assert svc.is_configured() is True


class TestEmbedText:
    def test_returns_none_when_unconfigured(self, svc, app_settings, sp_ws):
        app_settings.get_embedding_endpoint_name.return_value = ""

        assert svc.embed_text("hello") is None
        sp_ws.serving_endpoints.query.assert_not_called()

    def test_calls_serving_endpoint_and_extracts_embedding(self, svc, app_settings, sp_ws):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"
        sp_ws.serving_endpoints.query.return_value = SimpleNamespace(data=[SimpleNamespace(embedding=[0.1, 0.2, 0.3])])

        result = svc.embed_text("hello")

        assert result == [0.1, 0.2, 0.3]
        _, kwargs = sp_ws.serving_endpoints.query.call_args
        assert kwargs["name"] == "my-embedding-endpoint"
        assert kwargs["input"] == ["hello"]


class TestEmbedTexts:
    def test_batches_and_aligns_by_index(self, svc, app_settings, sp_ws):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"
        sp_ws.serving_endpoints.query.return_value = SimpleNamespace(
            data=[
                SimpleNamespace(index=1, embedding=[0.0, 1.0]),
                SimpleNamespace(index=0, embedding=[1.0, 0.0]),
            ]
        )

        result = svc.embed_texts(["a", "b"])

        assert result == [[1.0, 0.0], [0.0, 1.0]]
        _, kwargs = sp_ws.serving_endpoints.query.call_args
        assert kwargs["input"] == ["a", "b"]

    def test_chunks_wide_batches(self, svc, app_settings, sp_ws):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"

        def _respond(*, name, input):  # noqa: A002 — mirrors SDK kwarg
            return SimpleNamespace(data=[SimpleNamespace(index=i, embedding=[float(i)]) for i in range(len(input))])

        sp_ws.serving_endpoints.query.side_effect = _respond

        result = svc.embed_texts(["t0", "t1", "t2"], batch_size=2)

        assert result == [[0.0], [1.0], [0.0]]
        assert sp_ws.serving_endpoints.query.call_count == 2
        first_input = sp_ws.serving_endpoints.query.call_args_list[0].kwargs["input"]
        second_input = sp_ws.serving_endpoints.query.call_args_list[1].kwargs["input"]
        assert first_input == ["t0", "t1"]
        assert second_input == ["t2"]

    def test_query_path_prefers_obo_client(self, sql_executor_mock, app_settings, sp_ws):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"
        user_ws = create_autospec(WorkspaceClient, instance=True)
        user_ws.serving_endpoints.query.return_value = SimpleNamespace(data=[SimpleNamespace(embedding=[9.0])])
        sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
        svc = RuleEmbeddingsService(sql=sql_executor_mock, sp_ws=sp_ws, app_settings=app_settings, user_ws=user_ws)

        assert svc.embed_texts(["q"]) == [[9.0]]
        user_ws.serving_endpoints.query.assert_called_once()
        sp_ws.serving_endpoints.query.assert_not_called()

    def test_embed_and_store_always_uses_sp_even_when_obo_present(self, sql_executor_mock, app_settings, sp_ws):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"
        user_ws = create_autospec(WorkspaceClient, instance=True)
        sp_ws.serving_endpoints.query.return_value = SimpleNamespace(data=[SimpleNamespace(embedding=[1.0, 2.0])])
        sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
        svc = RuleEmbeddingsService(sql=sql_executor_mock, sp_ws=sp_ws, app_settings=app_settings, user_ws=user_ws)

        assert svc.embed_and_store(_rule(rule_id="r42")) is True
        sp_ws.serving_endpoints.query.assert_called_once()
        user_ws.serving_endpoints.query.assert_not_called()


class TestEmbedAndStore:
    def test_no_op_when_unconfigured(self, svc, app_settings, sql_executor_mock):
        app_settings.get_embedding_endpoint_name.return_value = ""

        stored = svc.embed_and_store(_rule())

        assert stored is False
        sql_executor_mock.upsert.assert_not_called()

    def test_stores_when_configured(self, svc, app_settings, sp_ws, sql_executor_mock):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"
        sp_ws.serving_endpoints.query.return_value = SimpleNamespace(data=[SimpleNamespace(embedding=[1.0, 2.0])])

        stored = svc.embed_and_store(_rule(rule_id="r42", version=3))

        assert stored is True
        sql_executor_mock.upsert.assert_called_once()
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"rule_id": "r42"}
        assert kwargs["value_cols"]["rule_version"] == 3
        assert "[1.0, 2.0]" in kwargs["value_cols"]["embedding"]

    def test_returns_false_and_swallows_endpoint_errors(self, svc, app_settings, sp_ws, sql_executor_mock):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"
        sp_ws.serving_endpoints.query.side_effect = RuntimeError("endpoint unreachable")

        stored = svc.embed_and_store(_rule())

        assert stored is False
        sql_executor_mock.upsert.assert_not_called()

    def test_returns_false_when_no_embedding_returned(self, svc, app_settings, sp_ws, sql_executor_mock):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"
        sp_ws.serving_endpoints.query.return_value = SimpleNamespace(data=[])

        stored = svc.embed_and_store(_rule())

        assert stored is False
        sql_executor_mock.upsert.assert_not_called()


class TestBackfill:
    def test_counts_only_successful_embeds(self, svc, app_settings, sp_ws):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"
        sp_ws.serving_endpoints.query.side_effect = [
            SimpleNamespace(data=[SimpleNamespace(embedding=[1.0])]),
            RuntimeError("boom"),
        ]

        count = svc.backfill([_rule(rule_id="r1"), _rule(rule_id="r2")])

        assert count == 1

    def test_returns_zero_when_unconfigured(self, svc, app_settings):
        app_settings.get_embedding_endpoint_name.return_value = ""

        count = svc.backfill([_rule(rule_id="r1"), _rule(rule_id="r2")])

        assert count == 0


class TestIterEmbeddings:
    def test_parses_rows_and_json_vectors(self, svc, sql_executor_mock):
        sql_executor_mock.query_dicts.return_value = [
            {"rule_id": "r1", "embedding": json.dumps([0.1, 0.2, 0.3])},
            {"rule_id": "r2", "embedding": json.dumps([1.0, 0.0])},
        ]

        rows = svc.iter_embeddings()

        assert rows == [("r1", [0.1, 0.2, 0.3]), ("r2", [1.0, 0.0])]

    def test_memoises_corpus_within_instance(self, svc, sql_executor_mock):
        sql_executor_mock.query_dicts.return_value = [
            {"rule_id": "r1", "embedding": json.dumps([0.1])},
        ]

        first = svc.iter_embeddings()
        second = svc.iter_embeddings()

        assert first is second
        assert sql_executor_mock.query_dicts.call_count == 1

    def test_upsert_clears_memoised_corpus(self, svc, app_settings, sp_ws, sql_executor_mock):
        app_settings.get_embedding_endpoint_name.return_value = "my-embedding-endpoint"
        sp_ws.serving_endpoints.query.return_value = SimpleNamespace(data=[SimpleNamespace(embedding=[1.0])])
        sql_executor_mock.query_dicts.return_value = [
            {"rule_id": "r1", "embedding": json.dumps([0.1])},
        ]

        _ = svc.iter_embeddings()
        assert svc.embed_and_store(_rule(rule_id="r2")) is True
        sql_executor_mock.query_dicts.return_value = [
            {"rule_id": "r1", "embedding": json.dumps([0.1])},
            {"rule_id": "r2", "embedding": json.dumps([1.0])},
        ]

        rows = svc.iter_embeddings()

        assert [rule_id for rule_id, _ in rows] == ["r1", "r2"]
        assert sql_executor_mock.query_dicts.call_count == 2

    def test_skips_malformed_and_empty_vectors(self, svc, sql_executor_mock):
        sql_executor_mock.query_dicts.return_value = [
            {"rule_id": "ok", "embedding": json.dumps([0.5])},
            {"rule_id": "bad_json", "embedding": "not json"},
            {"rule_id": "empty", "embedding": json.dumps([])},
            {"rule_id": "null", "embedding": None},
            {"rule_id": None, "embedding": json.dumps([0.9])},
        ]

        rows = svc.iter_embeddings()

        assert rows == [("ok", [0.5])]

    def test_read_failure_degrades_to_empty(self, svc, sql_executor_mock):
        sql_executor_mock.query_dicts.side_effect = RuntimeError("warehouse down")

        assert svc.iter_embeddings() == []

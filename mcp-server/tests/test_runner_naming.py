"""Unit tests for the runner's pure naming/validation helpers.

``dqx_mcp_runner.naming`` is dependency-free (stdlib only), so it imports and runs in the
mcp-server test environment without pyspark or the DQX library. The runner is a sibling
sub-project rather than an installed dependency of the server, so its ``src`` is put on the path by
``pythonpath`` in mcp-server/pyproject.toml.
"""

import pytest

from dqx_mcp_runner.naming import (
    IDENTIFIER_RE,
    output_schema_for_user,
    qualify_output,
    validate_identifier,
)


class TestOutputSchemaForUser:
    def test_is_deterministic(self):
        assert output_schema_for_user("alice@databricks.com") == output_schema_for_user("alice@databricks.com")

    def test_is_a_valid_identifier(self):
        for email in ("alice@databricks.com", "a.b+c@x.co", "UPPER@X.COM", "weird!!name@x.com"):
            schema = output_schema_for_user(email)
            assert IDENTIFIER_RE.match(schema), f"{schema!r} is not a bare identifier"
            assert schema.startswith("dqx_mcp_")

    def test_distinct_per_user(self):
        assert output_schema_for_user("alice@databricks.com") != output_schema_for_user("bob@databricks.com")

    def test_case_insensitive(self):
        # Emails are case-insensitive in practice; the schema must not differ by case.
        assert output_schema_for_user("Alice@Databricks.com") == output_schema_for_user("alice@databricks.com")

    def test_distinguishes_same_local_part_across_domains(self):
        # Same local part, different domain → the sha8 (over the full email) keeps them distinct.
        assert output_schema_for_user("me@a.com") != output_schema_for_user("me@b.com")

    # None is included deliberately: the caller identity comes from an HTTP header, so at runtime it
    # can be absent as well as blank. output_schema_for_user handles that — `(email or "")` — so the
    # None case must reach it unaltered rather than being coerced here, which would stop testing it.
    @pytest.mark.parametrize("empty", ["", "   ", None])
    def test_empty_email_rejected(self, empty: str | None):
        with pytest.raises(ValueError, match="caller identity"):
            output_schema_for_user(empty)


class TestQualifyOutput:
    def test_returns_unquoted_fqn(self):
        assert qualify_output("cat", "dqx_mcp_alice_abcd1234", "orders_out") == "cat.dqx_mcp_alice_abcd1234.orders_out"

    @pytest.mark.parametrize(
        "name", ["catalog.schema.table", "/Volumes/x", "has space", "drop`table", "", "2024_out", "9x"]
    )
    def test_rejects_non_identifier_name(self, name):
        # Leading-digit names are rejected too — the FQN is interpolated unquoted, where a
        # digit-leading identifier part is a SQL parse error.
        with pytest.raises(ValueError, match="Invalid output name"):
            qualify_output("cat", "sch", name)


class TestValidateIdentifier:
    def test_accepts_and_returns(self):
        assert validate_identifier("abc_123", "thing") == "abc_123"

    @pytest.mark.parametrize("bad", ["a.b", "1abc", "9", "has space"])
    def test_rejects(self, bad):
        with pytest.raises(ValueError, match="Invalid thing"):
            validate_identifier(bad, "thing")

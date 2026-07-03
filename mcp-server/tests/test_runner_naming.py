"""Unit tests for the runner's pure naming/validation helpers.

``dqx_mcp_runner.naming`` is dependency-free (stdlib only), so it imports and runs in the
mcp-server test environment without pyspark or the DQX library. We add the runner package's ``src``
to ``sys.path`` since it is a sibling sub-project, not an installed dependency of the server.
"""

import pathlib
import sys

import pytest

_RUNNER_SRC = pathlib.Path(__file__).resolve().parent.parent / "runner" / "src"
if str(_RUNNER_SRC) not in sys.path:
    sys.path.insert(0, str(_RUNNER_SRC))

from dqx_mcp_runner.naming import (  # noqa: E402  (path insert must precede import)
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

    @pytest.mark.parametrize("empty", ["", "   ", None])
    def test_empty_email_rejected(self, empty):
        with pytest.raises(ValueError, match="caller identity"):
            output_schema_for_user(empty)  # type: ignore[arg-type]


class TestQualifyOutput:
    def test_returns_unquoted_fqn(self):
        assert qualify_output("cat", "dqx_mcp_alice_abcd1234", "orders_out") == "cat.dqx_mcp_alice_abcd1234.orders_out"

    @pytest.mark.parametrize("name", ["catalog.schema.table", "/Volumes/x", "has space", "drop`table", ""])
    def test_rejects_non_identifier_name(self, name):
        with pytest.raises(ValueError, match="Invalid output name"):
            qualify_output("cat", "sch", name)


class TestValidateIdentifier:
    def test_accepts_and_returns(self):
        assert validate_identifier("abc_123", "thing") == "abc_123"

    def test_rejects(self):
        with pytest.raises(ValueError, match="Invalid thing"):
            validate_identifier("a.b", "thing")

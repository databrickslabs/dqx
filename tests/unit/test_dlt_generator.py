import json
import pytest
from unittest.mock import Mock, patch

from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.errors import MissingParameterError, InvalidParameterError
import databricks.labs.dqx.profiler.dlt_generator as dlt_generator_module


def test_dlt_generator_init_with_llm(mock_workspace_client, mock_spark):
    """Test DQDltGenerator initialization with LLM support."""
    generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
    assert generator.spark is not None
    # LLM engine may or may not be available depending on test environment
    # Just verify the generator was created successfully
    assert generator is not None


def test_generate_dlt_rules_ai_assisted_missing_llm(mock_workspace_client, mock_spark, monkeypatch):
    """Test error when LLM is not available."""
    monkeypatch.setattr(dlt_generator_module, "LLM_ENABLED", False)

    generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)

    with pytest.raises(MissingParameterError, match="LLM engine not available"):
        generator.generate_dlt_rules_ai_assisted(user_input="Some input")


def test_generate_dlt_rules_ai_assisted_missing_inputs(mock_workspace_client, mock_spark):
    """Test error when neither user_input nor summary_stats are provided."""
    # Mock LLM engine to be available
    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            mock_llm_instance = Mock()
            mock_llm_class.return_value = mock_llm_instance

            generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)

            with pytest.raises(MissingParameterError, match="Either summary statistics or user input must be provided"):
                generator.generate_dlt_rules_ai_assisted()


def test_generate_dlt_rules_ai_assisted_conversion_is_not_null(mock_workspace_client, mock_spark):
    """Test conversion of is_not_null check function."""
    dq_rules = [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "user_id"}},
            "criticality": "error",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input")

                assert isinstance(result, list)
                assert len(result) == 1
                assert "user_id_is_not_null" in result[0]
                assert "ON VIOLATION FAIL UPDATE" in result[0]  # error criticality


def test_generate_dlt_rules_ai_assisted_conversion_is_in_list(mock_workspace_client, mock_spark):
    """Test conversion of is_in_list check function."""
    dq_rules = [
        {
            "check": {
                "function": "is_in_list",
                "arguments": {"column": "status", "allowed": ["active", "inactive", "pending"]},
            },
            "criticality": "warn",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input")

                assert isinstance(result, list)
                assert len(result) == 1
                assert "status_is_in" in result[0]
                assert "ON VIOLATION" not in result[0]  # warn criticality has no ON VIOLATION clause


def test_generate_dlt_rules_ai_assisted_conversion_is_not_null_and_not_empty(mock_workspace_client, mock_spark):
    """Test conversion of is_not_null_and_not_empty check function."""
    dq_rules = [
        {
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": "email", "trim_strings": True},
            },
            "criticality": "error",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input")

                assert isinstance(result, list)
                assert len(result) == 1
                assert "email_is_not_null_or_empty" in result[0]
                assert "trim" in result[0]


def test_generate_dlt_rules_ai_assisted_conversion_is_in_range(mock_workspace_client, mock_spark):
    """Test conversion of is_in_range check function."""
    dq_rules = [
        {
            "check": {
                "function": "is_in_range",
                "arguments": {"column": "age", "min_limit": 18, "max_limit": 100},
            },
            "criticality": "error",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input")

                assert isinstance(result, list)
                assert len(result) == 1
                assert "age_min_max" in result[0]
                assert ">=" in result[0] and "<=" in result[0]


def test_generate_dlt_rules_ai_assisted_conversion_is_not_less_than(mock_workspace_client, mock_spark):
    """Test conversion of is_not_less_than check function."""
    dq_rules = [
        {
            "check": {
                "function": "is_not_less_than",
                "arguments": {"column": "price", "limit": 0},
            },
            "criticality": "error",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input")

                assert isinstance(result, list)
                assert len(result) == 1
                assert "price_min_max" in result[0]
                assert ">=" in result[0]


def test_generate_dlt_rules_ai_assisted_conversion_is_not_greater_than(mock_workspace_client, mock_spark):
    """Test conversion of is_not_greater_than check function."""
    dq_rules = [
        {
            "check": {
                "function": "is_not_greater_than",
                "arguments": {"column": "quantity", "limit": 1000},
            },
            "criticality": "warn",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input")

                assert isinstance(result, list)
                assert len(result) == 1
                assert "quantity_min_max" in result[0]
                assert "<=" in result[0]
                assert "ON VIOLATION" not in result[0]  # warn criticality


def test_generate_dlt_rules_ai_assisted_unsupported_check_function(mock_workspace_client, mock_spark):
    """Test that unsupported check functions are skipped with warning."""
    dq_rules = [
        {
            "check": {
                "function": "unsupported_function",
                "arguments": {"column": "test_col"},
            },
            "criticality": "error",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input")

            # Should return empty result since unsupported function was skipped
            assert result == []


def test_generate_dlt_rules_ai_assisted_filter_extraction(mock_workspace_client, mock_spark):
    """Test that filter expressions are extracted from DQ rules."""
    dq_rules = [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "email"}},
            "criticality": "error",
            "filter": "age >= 18",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                # Filter is stored in profile but not directly visible in SQL output
                # We just verify the method completes without error
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input")

                assert isinstance(result, list)


def test_generate_dlt_rules_ai_assisted_python_output(mock_workspace_client, mock_spark):
    """Test Python output with per-rule criticality grouping."""
    dq_rules = [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "user_id"}},
            "criticality": "error",
        },
        {
            "check": {"function": "is_not_null", "arguments": {"column": "username"}},
            "criticality": "warn",
        },
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input", language="Python")

                assert isinstance(result, str)
                assert "@dlt.expect_all_or_fail" in result  # error criticality
                assert "@dlt.expect_all" in result  # warn criticality
                assert "user_id_is_not_null" in result
                assert "username_is_not_null" in result


def test_generate_dlt_rules_ai_assisted_python_dict_output(mock_workspace_client, mock_spark):
    """Test Python_Dict output with criticality metadata."""
    dq_rules = [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "user_id"}},
            "criticality": "error",
        },
        {
            "check": {"function": "is_not_null", "arguments": {"column": "username"}},
            "criticality": "warn",
        },
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input", language="Python_Dict")

                assert isinstance(result, dict)
                assert "user_id_is_not_null" in result
                assert "username_is_not_null" in result
                # Check that criticality is stored in the dict
                assert isinstance(result["user_id_is_not_null"], dict)
                assert result["user_id_is_not_null"]["criticality"] == "error"
                assert result["user_id_is_not_null"]["expression"] is not None
                assert isinstance(result["username_is_not_null"], dict)
                assert result["username_is_not_null"]["criticality"] == "warn"
                assert result["username_is_not_null"]["expression"] is not None


def test_generate_dlt_rules_ai_assisted_unsupported_language(mock_workspace_client, mock_spark):
    """Test error for unsupported language."""
    dq_rules = [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "user_id"}},
            "criticality": "error",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)

                with pytest.raises(InvalidParameterError, match="Unsupported language"):
                    generator.generate_dlt_rules_ai_assisted(user_input="Test input", language="unsupported")


def test_generate_dlt_rules_ai_assisted_unsupported_language_with_empty_profiles(mock_workspace_client, mock_spark):
    """Test that unsupported language raises error even when profiles are empty (bug fix)."""
    # Use rules that will result in empty profiles (e.g., all unsupported functions)
    dq_rules = [
        {
            "check": {"function": "unsupported_function", "arguments": {"column": "user_id"}},
            "criticality": "error",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)

                # Should raise error for unsupported language even though profiles will be empty
                with pytest.raises(InvalidParameterError, match="Unsupported language"):
                    generator.generate_dlt_rules_ai_assisted(user_input="Test input", language="unsupported")


def test_generate_dlt_rules_ai_assisted_with_summary_stats(mock_workspace_client, mock_spark):
    """Test generation with summary statistics."""
    dq_rules = [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "temperature"}},
            "criticality": "error",
        }
    ]

    summary_stats = {
        "temperature": {"mean": "22.5", "min": "-10.0", "max": "50.0"},
    }

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(summary_stats=summary_stats)

                assert isinstance(result, list)
                assert len(result) > 0
                # Verify LLM was called with summary_stats
                mock_llm_instance.detect_business_rules_with_llm.assert_called_once()
                call_kwargs = mock_llm_instance.detect_business_rules_with_llm.call_args[1]
                assert "summary_stats" in call_kwargs


def test_generate_dlt_rules_ai_assisted_empty_profiles(mock_workspace_client, mock_spark):
    """Test handling when no valid profiles are generated."""
    dq_rules = []  # Empty rules

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input")

                # Should return empty result since unsupported function was skipped
                assert result == []  # SQL returns empty list


def test_generate_dlt_rules_ai_assisted_missing_column(mock_workspace_client, mock_spark):
    """Test handling when column is missing from check arguments."""
    dq_rules = [
        {
            "check": {"function": "is_not_null", "arguments": {}},  # Missing column
            "criticality": "error",
        }
    ]

    with patch.object(dlt_generator_module, "LLM_ENABLED", True):
        with patch("databricks.labs.dqx.profiler.dlt_generator.DQLLMEngine") as mock_llm_class:
            with patch("databricks.labs.dqx.profiler.dlt_generator.DQEngine") as mock_dq_engine:
                mock_llm_instance = Mock()
                mock_prediction = Mock()
                mock_prediction.quality_rules = json.dumps(dq_rules)
                mock_prediction.reasoning = "Test reasoning"
                mock_llm_instance.detect_business_rules_with_llm.return_value = mock_prediction
                mock_llm_class.return_value = mock_llm_instance

                # Mock DQEngine.validate_checks to return no errors
                mock_validation_status = Mock()
                mock_validation_status.has_errors = False
                mock_validation_status.errors = []
                mock_dq_engine.validate_checks.return_value = mock_validation_status

                generator = DQDltGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
                result = generator.generate_dlt_rules_ai_assisted(user_input="Test input")

                # Should skip the rule with missing column
                assert result == []

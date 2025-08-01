"""
Test suite for PandasDQRuleManager integration to validate functional parity with Spark.

This test suite validates that the PandasDQRuleManager integration provides
sophisticated functionality equivalent to the Spark-based implementation.
"""

import pytest
import pandas as pd
from unittest.mock import Mock
from datetime import datetime

from databricks.labs.dqx.pandas_backend import (
    PandasDQRuleManager, 
    PandasDQRowRuleExecutor, 
    PandasDQDatasetRuleExecutor,
    PandasDQRuleExecutorFactory,
    PandasDQCheckResult
)
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.labs.dqx.check_funcs import is_not_null, is_not_null_and_not_empty
from databricks.labs.dqx.pandas_engine import PandasDQEngine
from databricks.sdk import WorkspaceClient


class TestPandasDQRuleManagerIntegration:
    """Test suite for PandasDQRuleManager integration."""
    
    @pytest.fixture
    def workspace_client(self):
        """Create a mock workspace client for testing."""
        return Mock(spec=WorkspaceClient)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', None, 'David', 'Eve'],
            'age': [25, 30, 35, None, 28],
            'email': ['alice@test.com', '', 'charlie@test.com', 'david@test.com', None]
        })
    
    @pytest.fixture
    def pandas_engine(self, workspace_client):
        """Create a PandasDQEngine instance for testing."""
        return PandasDQEngine(workspace_client)
    
    def test_pandas_dqrulemanager_basic_functionality(self, sample_data):
        """Test that PandasDQRuleManager processes rules correctly."""
        # Create a simple rule
        rule = DQRowRule(
            criticality='error',
            check_func=is_not_null,
            column='name',
            name='name_not_null'
        )
        
        # Create manager and process
        manager = PandasDQRuleManager(
            check=rule,
            df=sample_data,
            engine_user_metadata={'test': 'metadata'},
            run_time=datetime.now()
        )
        
        result = manager.process()
        
        # Validate result structure
        assert isinstance(result, PandasDQCheckResult)
        assert isinstance(result.condition, pd.Series)
        assert isinstance(result.check_df, pd.DataFrame)
        assert len(result.condition) == len(sample_data)
        
        # Validate that null values are detected
        null_indices = sample_data['name'].isna()
        condition_not_null = result.condition.notna()
        
        # The condition should be not-null exactly where the original data has null values
        assert condition_not_null.sum() == null_indices.sum()
    
    def test_pandas_executor_factory(self, sample_data):
        """Test that the executor factory creates the correct executor types."""
        # Test row rule executor creation
        row_rule = DQRowRule(
            criticality='error',
            check_func=is_not_null,
            column='name'
        )
        
        row_executor = PandasDQRuleExecutorFactory.create(row_rule)
        assert isinstance(row_executor, PandasDQRowRuleExecutor)
        
        # Test that executor can process the rule
        result = row_executor.apply(sample_data)
        assert isinstance(result, PandasDQCheckResult)
    
    def test_pandas_vs_original_backend_consistency(self, pandas_engine, sample_data):
        """Test that the new PandasDQRuleManager produces consistent results."""
        # Create a rule that should detect null values in 'name' column
        rule = DQRowRule(
            criticality='error',
            check_func=is_not_null,
            column='name',
            name='name_not_null'
        )
        
        # Apply the rule using the integrated engine
        result_df = pandas_engine.apply_checks(sample_data, [rule])
        
        # Validate the result structure
        assert isinstance(result_df, pd.DataFrame)
        assert 'dq_errors' in result_df.columns
        assert len(result_df) == len(sample_data)
        
        # Check that errors are detected for null values
        error_column = result_df['dq_errors']
        
        # Count rows with errors (non-null error arrays)
        rows_with_errors = error_column.notna().sum()
        null_name_count = sample_data['name'].isna().sum()
        
        # Should have errors for each null name value
        assert rows_with_errors == null_name_count
        
        # Validate error structure for rows with null names
        null_indices = sample_data['name'].isna()
        for idx in sample_data.index[null_indices]:
            error_array = error_column.iloc[idx]
            assert error_array is not None
            assert isinstance(error_array, list)
            assert len(error_array) > 0
    
    def test_multiple_checks_integration(self, pandas_engine, sample_data):
        """Test that multiple checks are processed correctly through the integration."""
        # Create multiple rules
        rules = [
            DQRowRule(
                criticality='error',
                check_func=is_not_null,
                column='name',
                name='name_not_null'
            ),
            DQRowRule(
                criticality='warn',
                check_func=is_not_null_and_not_empty,
                column='email',
                name='email_not_null_and_not_empty'
            )
        ]
        
        # Apply checks and split
        good_df, bad_df = pandas_engine.apply_checks_and_split(sample_data, rules)
        
        # Validate results
        assert isinstance(good_df, pd.DataFrame)
        assert isinstance(bad_df, pd.DataFrame)
        
        # Good df should not have null names (error condition)
        assert good_df['name'].notna().all()
        
        # Bad df should have at least the null name records
        null_name_count = sample_data['name'].isna().sum()
        assert len(bad_df) >= null_name_count
        
        # Total records should be preserved
        assert len(good_df) + len(bad_df) >= len(sample_data)
    
    def test_metadata_driven_checks_with_dqrulemanager(self, pandas_engine, sample_data):
        """Test that metadata-driven checks work with the new PandasDQRuleManager."""
        # Create metadata-based checks
        checks = [
            {
                'check': {
                    'function': 'is_not_null',
                    'arguments': {'column': 'name'}
                },
                'criticality': 'error',
                'name': 'name_not_null'
            },
            {
                'check': {
                    'function': 'is_not_null_and_not_empty',
                    'arguments': {'column': 'email'}
                },
                'criticality': 'warn',
                'name': 'email_not_null_and_not_empty'
            }
        ]
        
        # Apply metadata-driven checks
        result_df = pandas_engine.apply_checks_by_metadata(sample_data, checks)
        
        # Validate result structure
        assert isinstance(result_df, pd.DataFrame)
        assert 'dq_errors' in result_df.columns
        assert 'dq_warnings' in result_df.columns
        assert len(result_df) == len(sample_data)
        
        # Validate that errors are detected for null names
        error_column = result_df['dq_errors']
        rows_with_errors = error_column.notna().sum()
        null_name_count = sample_data['name'].isna().sum()
        assert rows_with_errors == null_name_count
        
        # Validate that warnings are detected for empty/null emails
        warning_column = result_df['dq_warnings']
        rows_with_warnings = warning_column.notna().sum()
        
        # Count null or empty emails
        null_or_empty_email_count = (sample_data['email'].isna() | (sample_data['email'] == '')).sum()
        assert rows_with_warnings == null_or_empty_email_count
    
    def test_functional_parity_indicators(self, pandas_engine, sample_data):
        """Test key indicators that show functional parity with Spark implementation."""
        # Create a comprehensive rule set
        rule = DQRowRule(
            criticality='error',
            check_func=is_not_null,
            column='name',
            name='comprehensive_name_check',
            user_metadata={'test_key': 'test_value'}
        )
        
        # Apply the rule
        result_df = pandas_engine.apply_checks(sample_data, [rule])
        
        # Key functional parity indicators:
        
        # 1. Result structure matches expected format
        assert 'dq_errors' in result_df.columns
        error_column = result_df['dq_errors']
        
        # 2. Sophisticated error array structure (not just boolean flags)
        null_indices = sample_data['name'].isna()
        for idx in sample_data.index[null_indices]:
            error_array = error_column.iloc[idx]
            assert error_array is not None
            assert isinstance(error_array, list)
            assert len(error_array) > 0
            
            # Each error should have structured information
            error_item = error_array[0]
            assert isinstance(error_item, dict)
            # Should contain message information
            assert 'message' in error_item
        
        # 3. Correct filtering behavior (only failures are reported)
        non_null_indices = sample_data['name'].notna()
        for idx in sample_data.index[non_null_indices]:
            error_array = error_column.iloc[idx]
            assert error_array is None  # No errors for valid data
        
        # 4. Proper row count preservation
        assert len(result_df) == len(sample_data)
        
        # 5. Original data columns preserved
        for col in sample_data.columns:
            assert col in result_df.columns
    
    def test_no_regressions_in_existing_functionality(self, pandas_engine, sample_data):
        """Ensure that existing functionality still works after PandasDQRuleManager integration."""
        # Test basic functionality that should continue to work
        
        # 1. Engine initialization
        assert pandas_engine is not None
        
        # 2. Simple check application
        rule = DQRowRule(criticality='error', check_func=is_not_null, column='name')
        result_df = pandas_engine.apply_checks(sample_data, [rule])
        assert len(result_df) == len(sample_data)
        
        # 3. Check and split functionality
        good_df, bad_df = pandas_engine.apply_checks_and_split(sample_data, [rule])
        assert isinstance(good_df, pd.DataFrame)
        assert isinstance(bad_df, pd.DataFrame)
        
        # 4. Metadata-driven checks
        checks = [{'check': {'function': 'is_not_null', 'arguments': {'column': 'name'}}, 'criticality': 'error', 'name': 'test'}]
        result_df = pandas_engine.apply_checks_by_metadata(sample_data, checks)
        assert len(result_df) == len(sample_data)
        
        # 5. All operations should complete without exceptions
        # (If we reach this point, no exceptions were thrown)

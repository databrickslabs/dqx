"""
Integration tests for the backend-agnostic check functions with Pandas engine.
"""

import pandas as pd
import pytest
from unittest.mock import Mock

from databricks.labs.dqx.check_funcs_backend import (
    is_not_null, is_not_empty, is_not_null_and_not_empty, is_in_list, is_not_null_and_is_in_list,
    sql_expression, is_older_than_col2_for_n_days, is_older_than_n_days, is_not_in_future,
    is_not_in_near_future, is_not_less_than, is_not_greater_than, is_in_range, is_not_in_range,
    regex_match, is_not_null_and_not_empty_array, is_valid_date, is_valid_timestamp,
    is_valid_ipv4_address, is_ipv4_address_in_cidr
)
from databricks.labs.dqx.pandas_engine import PandasDQEngine
from databricks.labs.dqx.rule import DQRule


class TestCheckFuncsBackendIntegration:
    """Integration test suite for backend-agnostic check functions with Pandas engine."""
    
    def test_is_not_null_integration(self):
        """Test is_not_null function with Pandas engine."""
        # Create sample data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', None, 'Charlie', '', 'Eve']
        })
        
        # Create engine with mock workspace client and mock Spark session
        mock_workspace_client = Mock()
        mock_spark_session = Mock()
        engine = PandasDQEngine(workspace_client=mock_workspace_client, spark_session=mock_spark_session)
        
        # Create check using backend-agnostic function
        check = DQRule(
            name="name_not_null",
            condition=is_not_null('name'),
            message="Name should not be null"
        )
        
        # Apply check
        result_df = engine.apply_checks(data, [check])
        
        # Verify result structure
        assert 'name_not_null_error' in result_df.columns
        assert 'name_not_null_warn' in result_df.columns
        
        # Verify that the check was applied (placeholder implementation returns string)
        # In a full implementation, we would verify actual error detection
        assert len(result_df) == len(data)
    
    def test_is_not_empty_integration(self):
        """Test is_not_empty function with Pandas engine."""
        # Create sample data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', None, 'Charlie', '', 'Eve']
        })
        
        # Create engine with mock workspace client and mock Spark session
        mock_workspace_client = Mock()
        mock_spark_session = Mock()
        engine = PandasDQEngine(workspace_client=mock_workspace_client, spark_session=mock_spark_session)
        
        # Create check using backend-agnostic function
        check = DQRule(
            name="name_not_empty",
            condition=is_not_empty('name'),
            message="Name should not be empty"
        )
        
        # Apply check
        result_df = engine.apply_checks(data, [check])
        
        # Verify result structure
        assert 'name_not_empty_error' in result_df.columns
        assert 'name_not_empty_warn' in result_df.columns
        
        # Verify that the check was applied
        assert len(result_df) == len(data)
    
    def test_is_not_null_and_not_empty_integration(self):
        """Test is_not_null_and_not_empty function with Pandas engine."""
        # Create sample data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', None, 'Charlie', '', 'Eve']
        })
        
        # Create engine with mock workspace client and mock Spark session
        mock_workspace_client = Mock()
        mock_spark_session = Mock()
        engine = PandasDQEngine(workspace_client=mock_workspace_client, spark_session=mock_spark_session)
        
        # Create check using backend-agnostic function
        check = DQRule(
            name="name_not_null_and_not_empty",
            condition=is_not_null_and_not_empty('name'),
            message="Name should not be null or empty"
        )
        
        # Apply check
        result_df = engine.apply_checks(data, [check])
        
        # Verify result structure
        assert 'name_not_null_and_not_empty_error' in result_df.columns
        assert 'name_not_null_and_not_empty_warn' in result_df.columns
        
        # Verify that the check was applied
        assert len(result_df) == len(data)
    
    def test_is_in_list_integration(self):
        """Test is_in_list function with Pandas engine."""
        # Create sample data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'status': ['active', 'inactive', 'pending', 'active', 'unknown']
        })
        
        # Create engine with mock workspace client and mock Spark session
        mock_workspace_client = Mock()
        mock_spark_session = Mock()
        engine = PandasDQEngine(workspace_client=mock_workspace_client, spark_session=mock_spark_session)
        
        # Create check using backend-agnostic function
        allowed_values = ['active', 'inactive', 'pending']
        check = DQRule(
            name="status_in_list",
            condition=is_in_list('status', allowed_values),
            message="Status should be in allowed list"
        )
        
        # Apply check
        result_df = engine.apply_checks(data, [check])
        
        # Verify result structure
        assert 'status_in_list_error' in result_df.columns
        assert 'status_in_list_warn' in result_df.columns
        
        # Verify that the check was applied
        assert len(result_df) == len(data)
    
    def test_is_not_null_and_is_in_list_integration(self):
        """Test is_not_null_and_is_in_list function with Pandas engine."""
        # Create sample data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'status': ['active', 'inactive', None, 'active', 'unknown']
        })
        
        # Create engine with mock workspace client and mock Spark session
        mock_workspace_client = Mock()
        mock_spark_session = Mock()
        engine = PandasDQEngine(workspace_client=mock_workspace_client, spark_session=mock_spark_session)
        
        # Create check using backend-agnostic function
        allowed_values = ['active', 'inactive', 'pending']
        check = DQRule(
            name="status_not_null_and_in_list",
            condition=is_not_null_and_is_in_list('status', allowed_values),
            message="Status should not be null and should be in allowed list"
        )
        
        # Apply check
        result_df = engine.apply_checks(data, [check])
        
        # Verify result structure
        assert 'status_not_null_and_in_list_error' in result_df.columns
        assert 'status_not_null_and_in_list_warn' in result_df.columns
        
        # Verify that the check was applied
        assert len(result_df) == len(data)
    
    def test_multiple_checks_integration(self):
        """Test multiple backend-agnostic checks with Pandas engine."""
        # Create sample data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', None, 'Charlie', '', 'Eve'],
            'age': [25, 30, None, 40, 35],
            'status': ['active', 'inactive', 'pending', 'active', 'unknown']
        })
        
        # Create engine with mock workspace client and mock Spark session
        mock_workspace_client = Mock()
        mock_spark_session = Mock()
        engine = PandasDQEngine(workspace_client=mock_workspace_client, spark_session=mock_spark_session)
        
        # Create multiple checks using backend-agnostic functions
        checks = [
            DQRule(
                name="name_not_null",
                condition=is_not_null('name'),
                message="Name should not be null"
            ),
            DQRule(
                name="age_not_null",
                condition=is_not_null('age'),
                message="Age should not be null"
            ),
            DQRule(
                name="status_in_list",
                condition=is_in_list('status', ['active', 'inactive', 'pending']),
                message="Status should be in allowed list"
            )
        ]
        
        # Apply checks
        result_df = engine.apply_checks(data, checks)
        
        # Verify result structure
        assert 'name_not_null_error' in result_df.columns
        assert 'name_not_null_warn' in result_df.columns
        assert 'age_not_null_error' in result_df.columns
        assert 'age_not_null_warn' in result_df.columns
        assert 'status_in_list_error' in result_df.columns
        assert 'status_in_list_warn' in result_df.columns
        
        # Verify that all checks were applied
        assert len(result_df) == len(data)

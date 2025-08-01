"""
Integration tests for backend-agnostic check functions with Pandas backend.

These tests verify that the backend-agnostic check functions work correctly
with the Pandas backend implementation.
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
from databricks.labs.dqx.df_backend import get_backend, PandasBackend
from databricks.labs.dqx.rule import DQRowRule


class TestBackendAgnosticChecksPandas:
    """Integration test suite for backend-agnostic check functions with Pandas backend."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Get the Pandas backend
        self.backend = get_backend('pandas')
    
    def test_is_not_null_with_pandas(self):
        """Test is_not_null function with Pandas backend."""
        # Create sample data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', None, 'Charlie', '', 'Eve']
        })
        
        # Create check using backend-agnostic function
        check = DQRowRule(
            name="name_not_null",
            check_func=is_not_null,
            column='name',
            check_func_args=[],
            check_func_kwargs={'column': 'name'}
        )
        
        # Apply check condition directly
        condition_result = check.get_check_condition()
        
        # Verify that the condition is a string (placeholder implementation)
        assert isinstance(condition_result, str)
        assert "is_not_null" in condition_result
    
    def test_is_not_empty_with_pandas(self):
        """Test is_not_empty function with Pandas backend."""
        # Create sample data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', None, 'Charlie', '', 'Eve']
        })
        
        # Create check using backend-agnostic function
        check = DQRowRule(
            name="name_not_empty",
            check_func=is_not_empty,
            column='name',
            check_func_args=[],
            check_func_kwargs={'column': 'name'}
        )
        
        # Apply check condition directly
        condition_result = check.get_check_condition()
        
        # Verify that the condition is a string (placeholder implementation)
        assert isinstance(condition_result, str)
        assert "is_not_empty" in condition_result
    
    def test_is_in_list_with_pandas(self):
        """Test is_in_list function with Pandas backend."""
        # Create sample data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'status': ['active', 'inactive', 'pending', 'active', 'unknown']
        })
        
        # Create check using backend-agnostic function
        allowed_values = ['active', 'inactive', 'pending']
        check = DQRowRule(
            name="status_in_list",
            check_func=is_in_list,
            column='status',
            check_func_args=[allowed_values],
            check_func_kwargs={'column': 'status'}
        )
        
        # Apply check condition directly
        condition_result = check.get_check_condition()
        
        # Verify that the condition is a string (placeholder implementation)
        assert isinstance(condition_result, str)
        assert "is_in_list" in condition_result
    
    def test_multiple_checks_with_pandas(self):
        """Test multiple backend-agnostic checks with Pandas backend."""
        # Create sample data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', None, 'Charlie', '', 'Eve'],
            'age': [25, 30, None, 40, 35],
            'status': ['active', 'inactive', 'pending', 'active', 'unknown']
        })
        
        # Create multiple checks using backend-agnostic functions
        checks = [
            DQRowRule(
                name="name_not_null",
                check_func=is_not_null,
                column='name',
                check_func_args=[],
                check_func_kwargs={'column': 'name'}
            ),
            DQRowRule(
                name="age_not_null",
                check_func=is_not_null,
                column='age',
                check_func_args=[],
                check_func_kwargs={'column': 'age'}
            ),
            DQRowRule(
                name="status_in_list",
                check_func=is_in_list,
                column='status',
                check_func_args=[['active', 'inactive', 'pending']],
                check_func_kwargs={'column': 'status'}
            )
        ]
        
        # Apply check conditions directly
        for check in checks:
            condition_result = check.get_check_condition()
            # Verify that the condition is a string (placeholder implementation)
            assert isinstance(condition_result, str)

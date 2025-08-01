"""
Unit tests for the backend-agnostic check functions.
"""

import pytest
from unittest.mock import Mock, patch

from databricks.labs.dqx.check_funcs_backend import (
    is_not_null, is_not_empty, is_not_null_and_not_empty, is_in_list, is_not_null_and_is_in_list,
    sql_expression, is_older_than_col2_for_n_days, is_older_than_n_days, is_not_in_future,
    is_not_in_near_future, is_not_less_than, is_not_greater_than, is_in_range, is_not_in_range,
    regex_match, is_not_null_and_not_empty_array, is_valid_date, is_valid_timestamp,
    is_valid_ipv4_address, is_ipv4_address_in_cidr
)
from databricks.labs.dqx.df_backend import DataFrameBackend


class TestCheckFuncsBackend:
    """Test suite for backend-agnostic check functions."""
    
    def test_is_not_null(self):
        """Test is_not_null function."""
        # Test with default backend
        result = is_not_null('column1')
        assert isinstance(result, str)
        assert 'is_not_null(column1)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_not_null('column1', backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_not_null(column1)' in result
    
    def test_is_not_empty(self):
        """Test is_not_empty function."""
        # Test with default backend
        result = is_not_empty('column1')
        assert isinstance(result, str)
        assert 'is_not_empty(column1)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_not_empty('column1', backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_not_empty(column1)' in result
    
    def test_is_not_null_and_not_empty(self):
        """Test is_not_null_and_not_empty function."""
        # Test with default backend
        result = is_not_null_and_not_empty('column1')
        assert isinstance(result, str)
        assert 'is_not_null_and_not_empty(column1, False)' in result
        
        # Test with explicit backend and trim_strings
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_not_null_and_not_empty('column1', trim_strings=True, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_not_null_and_not_empty(column1, True)' in result
    
    def test_is_in_list(self):
        """Test is_in_list function."""
        allowed_values = ['value1', 'value2', 'value3']
        
        # Test with default backend
        result = is_in_list('column1', allowed_values)
        assert isinstance(result, str)
        assert 'is_in_list(column1, ' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_in_list('column1', allowed_values, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_in_list(column1, ' in result
    
    def test_is_not_null_and_is_in_list(self):
        """Test is_not_null_and_is_in_list function."""
        allowed_values = ['value1', 'value2', 'value3']
        
        # Test with default backend
        result = is_not_null_and_is_in_list('column1', allowed_values)
        assert isinstance(result, str)
        assert 'is_not_null_and_is_in_list(column1, ' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_not_null_and_is_in_list('column1', allowed_values, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_not_null_and_is_in_list(column1, ' in result
    
    def test_sql_expression(self):
        """Test sql_expression function."""
        expression = "column1 > 0"
        
        # Test with default backend
        result = sql_expression(expression)
        assert isinstance(result, str)
        assert 'sql_expression(' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = sql_expression(expression, backend=mock_backend)
        assert isinstance(result, str)
        assert 'sql_expression(' in result
    
    def test_is_older_than_col2_for_n_days(self):
        """Test is_older_than_col2_for_n_days function."""
        # Test with default backend
        result = is_older_than_col2_for_n_days('column1', 'column2', days=5)
        assert isinstance(result, str)
        assert 'is_older_than_col2_for_n_days(column1, column2, 5, False)' in result
        
        # Test with explicit backend and negate
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_older_than_col2_for_n_days('column1', 'column2', days=5, negate=True, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_older_than_col2_for_n_days(column1, column2, 5, True)' in result
    
    def test_is_older_than_n_days(self):
        """Test is_older_than_n_days function."""
        # Test with default backend
        result = is_older_than_n_days('column1', days=5)
        assert isinstance(result, str)
        assert 'is_older_than_n_days(column1, 5, None, False)' in result
        
        # Test with explicit backend and negate
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_older_than_n_days('column1', days=5, negate=True, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_older_than_n_days(column1, 5, None, True)' in result
    
    def test_is_not_in_future(self):
        """Test is_not_in_future function."""
        # Test with default backend
        result = is_not_in_future('column1')
        assert isinstance(result, str)
        assert 'is_not_in_future(column1, 0, None)' in result
        
        # Test with explicit backend and offset
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_not_in_future('column1', offset=3600, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_not_in_future(column1, 3600, None)' in result
    
    def test_is_not_in_near_future(self):
        """Test is_not_in_near_future function."""
        # Test with default backend
        result = is_not_in_near_future('column1')
        assert isinstance(result, str)
        assert 'is_not_in_near_future(column1, 0, None)' in result
        
        # Test with explicit backend and offset
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_not_in_near_future('column1', offset=3600, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_not_in_near_future(column1, 3600, None)' in result
    
    def test_is_not_less_than(self):
        """Test is_not_less_than function."""
        # Test with default backend
        result = is_not_less_than('column1', limit=5)
        assert isinstance(result, str)
        assert 'is_not_less_than(column1, 5)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_not_less_than('column1', limit=5, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_not_less_than(column1, 5)' in result
    
    def test_is_not_greater_than(self):
        """Test is_not_greater_than function."""
        # Test with default backend
        result = is_not_greater_than('column1', limit=5)
        assert isinstance(result, str)
        assert 'is_not_greater_than(column1, 5)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_not_greater_than('column1', limit=5, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_not_greater_than(column1, 5)' in result
    
    def test_is_in_range(self):
        """Test is_in_range function."""
        # Test with default backend
        result = is_in_range('column1', min_limit=1, max_limit=10)
        assert isinstance(result, str)
        assert 'is_in_range(column1, 1, 10)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_in_range('column1', min_limit=1, max_limit=10, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_in_range(column1, 1, 10)' in result
    
    def test_is_not_in_range(self):
        """Test is_not_in_range function."""
        # Test with default backend
        result = is_not_in_range('column1', min_limit=1, max_limit=10)
        assert isinstance(result, str)
        assert 'is_not_in_range(column1, 1, 10)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_not_in_range('column1', min_limit=1, max_limit=10, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_not_in_range(column1, 1, 10)' in result
    
    def test_regex_match(self):
        """Test regex_match function."""
        regex = r'^[A-Za-z]+$'
        
        # Test with default backend
        result = regex_match('column1', regex)
        assert isinstance(result, str)
        assert 'regex_match(column1, ' in result
        
        # Test with explicit backend and negate
        mock_backend = Mock(spec=DataFrameBackend)
        result = regex_match('column1', regex, negate=True, backend=mock_backend)
        assert isinstance(result, str)
        assert 'regex_match(column1, ' in result
        assert ', True)' in result
    
    def test_is_not_null_and_not_empty_array(self):
        """Test is_not_null_and_not_empty_array function."""
        # Test with default backend
        result = is_not_null_and_not_empty_array('column1')
        assert isinstance(result, str)
        assert 'is_not_null_and_not_empty_array(column1)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_not_null_and_not_empty_array('column1', backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_not_null_and_not_empty_array(column1)' in result
    
    def test_is_valid_date(self):
        """Test is_valid_date function."""
        date_format = 'yyyy-mm-dd'
        
        # Test with default backend
        result = is_valid_date('column1', date_format=date_format)
        assert isinstance(result, str)
        assert 'is_valid_date(column1, yyyy-mm-dd)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_valid_date('column1', date_format=date_format, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_valid_date(column1, yyyy-mm-dd)' in result
    
    def test_is_valid_timestamp(self):
        """Test is_valid_timestamp function."""
        timestamp_format = 'yyyy-mm-dd HH:mm:ss'
        
        # Test with default backend
        result = is_valid_timestamp('column1', timestamp_format=timestamp_format)
        assert isinstance(result, str)
        assert 'is_valid_timestamp(column1, yyyy-mm-dd HH:mm:ss)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_valid_timestamp('column1', timestamp_format=timestamp_format, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_valid_timestamp(column1, yyyy-mm-dd HH:mm:ss)' in result
    
    def test_is_valid_ipv4_address(self):
        """Test is_valid_ipv4_address function."""
        # Test with default backend
        result = is_valid_ipv4_address('column1')
        assert isinstance(result, str)
        assert 'is_valid_ipv4_address(column1)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_valid_ipv4_address('column1', backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_valid_ipv4_address(column1)' in result
    
    def test_is_ipv4_address_in_cidr(self):
        """Test is_ipv4_address_in_cidr function."""
        cidr_block = '192.168.1.0/24'
        
        # Test with default backend
        result = is_ipv4_address_in_cidr('column1', cidr_block)
        assert isinstance(result, str)
        assert 'is_ipv4_address_in_cidr(column1, 192.168.1.0/24)' in result
        
        # Test with explicit backend
        mock_backend = Mock(spec=DataFrameBackend)
        result = is_ipv4_address_in_cidr('column1', cidr_block, backend=mock_backend)
        assert isinstance(result, str)
        assert 'is_ipv4_address_in_cidr(column1, 192.168.1.0/24)' in result

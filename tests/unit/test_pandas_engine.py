"""
Unit tests for the Pandas-compatible DQX engine implementation.
"""

import pytest
import pandas as pd
from unittest.mock import Mock

from databricks.labs.dqx.pandas_engine import PandasDQEngine, PandasDQEngineCore
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.check_funcs import is_not_null
from databricks.sdk import WorkspaceClient


class TestPandasDQEngine:
    """Test suite for the Pandas-compatible DQX engine."""
    
    @pytest.fixture
    def workspace_client(self):
        """Create a mock workspace client for testing."""
        return Mock(spec=WorkspaceClient)
    
    @pytest.fixture
    def engine(self, workspace_client):
        """Create a PandasDQEngine instance for testing."""
        return PandasDQEngine(workspace_client)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', None, 'David', 'Eve'],
            'age': [25, 30, 35, None, 28]
        })
    
    def test_init(self, workspace_client):
        """Test that the engine initializes correctly."""
        engine = PandasDQEngine(workspace_client)
        assert isinstance(engine, PandasDQEngine)
        assert engine._workspace_client == workspace_client
    
    def test_apply_checks(self, engine, sample_data):
        """Test applying checks to a DataFrame."""
        # Create a simple check
        check = DQRowRule(
            criticality='error',
            check_func=is_not_null,
            column='name'
        )
        
        # Apply checks
        result_df = engine.apply_checks(sample_data, [check])
        
        # Verify result
        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == len(sample_data)
        # Note: This is a placeholder test, actual implementation would need more detailed verification
    
    def test_apply_checks_and_split(self, engine, sample_data):
        """Test applying checks and splitting results."""
        # Create a simple check
        check = DQRowRule(
            criticality='error',
            check_func=is_not_null,
            column='name'
        )
        
        # Apply checks and split
        good_df, bad_df = engine.apply_checks_and_split(sample_data, [check])
        
        # Verify results
        assert isinstance(good_df, pd.DataFrame)
        assert isinstance(bad_df, pd.DataFrame)
        assert len(good_df) == len(sample_data)
        assert len(bad_df) <= len(sample_data)
        # Note: This is a placeholder test, actual implementation would need more detailed verification
    
    def test_apply_checks_by_metadata(self, engine, sample_data):
        """Test applying checks by metadata."""
        # Create metadata-based checks in the format expected by Spark engine
        checks = [
            {
                'check': {
                    'function': 'is_not_null',
                    'arguments': {'column': 'name'}
                },
                'criticality': 'error',
                'name': 'name_not_null'
            }
        ]
        
        # Apply checks
        result_df = engine.apply_checks_by_metadata(sample_data, checks)
        
        # Verify result
        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == len(sample_data)
        # Note: This is a placeholder test, actual implementation would need more detailed verification
    
    def test_apply_checks_by_metadata_and_split(self, engine, sample_data):
        """Test applying checks by metadata and splitting results."""
        # Create metadata-based checks in the format expected by Spark engine
        checks = [
            {
                'check': {
                    'function': 'is_not_null',
                    'arguments': {'column': 'name'}
                },
                'criticality': 'error',
                'name': 'name_not_null'
            }
        ]
        
        # Apply checks and split
        good_df, bad_df = engine.apply_checks_by_metadata_and_split(sample_data, checks)
        
        # Verify results
        assert isinstance(good_df, pd.DataFrame)
        assert isinstance(bad_df, pd.DataFrame)
        assert len(good_df) == len(sample_data)
        assert len(bad_df) <= len(sample_data)
        # Note: This is a placeholder test, actual implementation would need more detailed verification

"""
Integration tests for the Pandas-compatible DQX engine implementation.
"""

import pytest
import pandas as pd
from unittest.mock import Mock

from databricks.labs.dqx.pandas_engine import PandasDQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.check_funcs import is_not_null, is_not_empty
from databricks.sdk import WorkspaceClient


class TestPandasDQEngineIntegration:
    """Integration test suite for the Pandas-compatible DQX engine."""
    
    @pytest.fixture
    def workspace_client(self):
        """Create a mock workspace client for testing."""
        # Mock the workspace client methods that are called during initialization
        ws = Mock(spec=WorkspaceClient)
        user = Mock()
        user.user_name = "test_user"
        ws.current_user.me.return_value = user
        return ws
    
    @pytest.fixture
    def engine(self, workspace_client):
        """Create a PandasDQEngine instance for testing."""
        return PandasDQEngine(workspace_client)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', '', 'David', None],
            'age': [25, 30, 35, None, 28]
        })
    
    def test_engine_initialization(self, workspace_client):
        """Test that the engine initializes correctly with a real workspace client mock."""
        engine = PandasDQEngine(workspace_client)
        assert isinstance(engine, PandasDQEngine)
        assert engine._workspace_client == workspace_client
    
    def test_apply_simple_checks(self, engine, sample_data):
        """Test applying simple null and empty checks to a DataFrame."""
        # Create checks
        checks = [
            DQRowRule(
                criticality='error',
                check_func=is_not_null,
                column='name'
            ),
            DQRowRule(
                criticality='warn',
                check_func=is_not_empty,
                column='name'
            )
        ]
        
        # Apply checks
        result_df = engine.apply_checks(sample_data, checks)
        
        # Verify result structure
        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == len(sample_data)
        # Note: This is a placeholder test, actual implementation would need more detailed verification
    
    def test_apply_checks_and_split(self, engine, sample_data):
        """Test applying checks and splitting into good and bad DataFrames."""
        # Create checks
        checks = [
            DQRowRule(
                criticality='error',
                check_func=is_not_null,
                column='name'
            )
        ]
        
        # Apply checks and split
        good_df, bad_df = engine.apply_checks_and_split(sample_data, checks)
        
        # Verify results
        assert isinstance(good_df, pd.DataFrame)
        assert isinstance(bad_df, pd.DataFrame)
        assert len(good_df) + len(bad_df) >= len(sample_data)  # Some rows might be in both
        # Note: This is a placeholder test, actual implementation would need more detailed verification
    
    def test_metadata_driven_checks(self, engine, sample_data):
        """Test applying metadata-driven checks."""
        # Create metadata-based checks in the format expected by Spark engine
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
                    'arguments': {'column': 'name'}
                },
                'criticality': 'warn',
                'name': 'name_not_empty'
            }
        ]
        
        # Apply checks
        result_df = engine.apply_checks_by_metadata(sample_data, checks)
        
        # Verify result
        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == len(sample_data)
        # Note: This is a placeholder test, actual implementation would need more detailed verification

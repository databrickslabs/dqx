import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from databricks.labs.dqx.engine import DQEngine

@pytest.fixture
def mock_engine():
  engine = DQEngine(MagicMock())
  engine._get_installation = MagicMock()
  engine._load_run_config = MagicMock()
  return engine

@pytest.fixture
def mock_dataframes():
  quarantine_df = MagicMock(spec=DataFrame)
  output_df = MagicMock(spec=DataFrame)
  return quarantine_df, output_df

def test_save_results_in_table_quarantine_only(mock_engine, mock_dataframes):
  quarantine_df, _ = mock_dataframes
  mock_run_config = MagicMock()
  mock_run_config.quarantine_table = "quarantine_table"
  mock_run_config.output_table = None
  mock_engine._load_run_config.return_value = mock_run_config

  mock_engine.save_results_in_table(quarantine_df=quarantine_df, output_df=None)

  mock_engine._get_installation.assert_called_once_with(True, "dqx")
  mock_engine._load_run_config.assert_called_once()
  quarantine_df.write.format.assert_called_once_with("delta")
  quarantine_df.write.format().mode.assert_called_once_with("overwrite")
  quarantine_df.write.format().mode().saveAsTable.assert_called_once_with("quarantine_table")

def test_save_results_in_table_output_only(mock_engine, mock_dataframes):
  _, output_df = mock_dataframes
  mock_run_config = MagicMock()
  mock_run_config.quarantine_table = None
  mock_run_config.output_table = "output_table"
  mock_engine._load_run_config.return_value = mock_run_config

  mock_engine.save_results_in_table(quarantine_df=None, output_df=output_df)

  mock_engine._get_installation.assert_called_once_with(True, "dqx")
  mock_engine._load_run_config.assert_called_once()
  output_df.write.format.assert_called_once_with("delta")
  output_df.write.format().mode.assert_called_once_with("append")
  output_df.write.format().mode().saveAsTable.assert_called_once_with("output_table")

def test_save_results_in_table_both(mock_engine, mock_dataframes):
  quarantine_df, output_df = mock_dataframes
  mock_run_config = MagicMock()
  mock_run_config.quarantine_table = "quarantine_table"
  mock_run_config.output_table = "output_table"
  mock_engine._load_run_config.return_value = mock_run_config

  mock_engine.save_results_in_table(quarantine_df=quarantine_df, output_df=output_df)

  mock_engine._get_installation.assert_called_once_with(True, "dqx")
  mock_engine._load_run_config.assert_called_once()
  quarantine_df.write.format.assert_called_once_with("delta")
  quarantine_df.write.format().mode.assert_called_once_with("overwrite")
  quarantine_df.write.format().mode().saveAsTable.assert_called_once_with("quarantine_table")
  output_df.write.format.assert_called_once_with("delta")
  output_df.write.format().mode.assert_called_once_with("append")
  output_df.write.format().mode().saveAsTable.assert_called_once_with("output_table")

def test_save_results_in_table_no_data(mock_engine):
  mock_run_config = MagicMock()
  mock_run_config.quarantine_table = None
  mock_run_config.output_table = None
  mock_engine._load_run_config.return_value = mock_run_config

  mock_engine.save_results_in_table(quarantine_df=None, output_df=None)

  mock_engine._get_installation.assert_called_once_with(True, "dqx")
  mock_engine._load_run_config.assert_called_once()

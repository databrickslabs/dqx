"""Unit tests for anomaly detection configuration classes."""

from databricks.labs.dqx.config import (
    AnomalyConfig,
    AnomalyParams,
    IsolationForestConfig,
    FeatureEngineeringConfig,
)


# ============================================================================
# IsolationForestConfig Tests
# ============================================================================


def test_isolation_forest_config_defaults():
    """Test IsolationForestConfig with default values."""
    cfg = IsolationForestConfig()
    assert cfg.contamination == 0.1
    assert cfg.num_trees == 200
    assert cfg.random_seed == 42


def test_isolation_forest_config_custom_contamination():
    """Test IsolationForestConfig with custom contamination values."""
    # Valid contamination values
    cfg1 = IsolationForestConfig(contamination=0.05)
    assert cfg1.contamination == 0.05

    cfg2 = IsolationForestConfig(contamination=0.5)
    assert cfg2.contamination == 0.5

    # Edge cases
    cfg3 = IsolationForestConfig(contamination=0.01)
    assert cfg3.contamination == 0.01

    cfg4 = IsolationForestConfig(contamination=0.99)
    assert cfg4.contamination == 0.99


def test_isolation_forest_config_custom_num_trees():
    """Test IsolationForestConfig with custom number of trees."""
    cfg1 = IsolationForestConfig(num_trees=100)
    assert cfg1.num_trees == 100

    cfg2 = IsolationForestConfig(num_trees=500)
    assert cfg2.num_trees == 500

    # Minimum valid value
    cfg3 = IsolationForestConfig(num_trees=1)
    assert cfg3.num_trees == 1


def test_isolation_forest_config_custom_random_seed():
    """Test IsolationForestConfig with custom random seed."""
    cfg1 = IsolationForestConfig(random_seed=123)
    assert cfg1.random_seed == 123

    cfg2 = IsolationForestConfig(random_seed=0)
    assert cfg2.random_seed == 0

    cfg3 = IsolationForestConfig(random_seed=999999)
    assert cfg3.random_seed == 999999


def test_isolation_forest_config_all_custom():
    """Test IsolationForestConfig with all custom values."""
    cfg = IsolationForestConfig(
        contamination=0.15,
        num_trees=300,
        random_seed=999,
    )
    assert cfg.contamination == 0.15
    assert cfg.num_trees == 300
    assert cfg.random_seed == 999


# ============================================================================
# AnomalyParams Tests
# ============================================================================


def test_anomaly_params_defaults():
    """Test AnomalyParams with default values."""
    params = AnomalyParams()
    assert params.sample_fraction == 0.3
    assert params.max_rows == 1_000_000
    assert params.train_ratio == 0.8
    assert isinstance(params.algorithm_config, IsolationForestConfig)


def test_anomaly_params_custom_sample_fraction():
    """Test AnomalyParams with custom sample fraction values."""
    # Valid sample fractions
    params1 = AnomalyParams(sample_fraction=0.1)
    assert params1.sample_fraction == 0.1

    params2 = AnomalyParams(sample_fraction=0.5)
    assert params2.sample_fraction == 0.5

    params3 = AnomalyParams(sample_fraction=1.0)
    assert params3.sample_fraction == 1.0

    # Edge case: very small fraction
    params4 = AnomalyParams(sample_fraction=0.01)
    assert params4.sample_fraction == 0.01


def test_anomaly_params_custom_max_rows():
    """Test AnomalyParams with custom max_rows values."""
    params1 = AnomalyParams(max_rows=500_000)
    assert params1.max_rows == 500_000

    params2 = AnomalyParams(max_rows=2_000_000)
    assert params2.max_rows == 2_000_000

    # Small value
    params3 = AnomalyParams(max_rows=1000)
    assert params3.max_rows == 1000


def test_anomaly_params_custom_train_ratio():
    """Test AnomalyParams with custom train_ratio values."""
    params1 = AnomalyParams(train_ratio=0.7)
    assert params1.train_ratio == 0.7

    params2 = AnomalyParams(train_ratio=0.9)
    assert params2.train_ratio == 0.9

    # Edge cases
    params3 = AnomalyParams(train_ratio=0.5)
    assert params3.train_ratio == 0.5

    params4 = AnomalyParams(train_ratio=0.99)
    assert params4.train_ratio == 0.99


def test_anomaly_params_custom_ensemble_size():
    """Test AnomalyParams with custom ensemble_size values."""
    params1 = AnomalyParams(ensemble_size=1)
    assert params1.ensemble_size == 1

    params2 = AnomalyParams(ensemble_size=5)
    assert params2.ensemble_size == 5

    params3 = AnomalyParams(ensemble_size=10)
    assert params3.ensemble_size == 10


def test_anomaly_params_with_custom_algorithm_config():
    """Test AnomalyParams with custom IsolationForestConfig."""
    custom_algo_config = IsolationForestConfig(
        contamination=0.15,
        num_trees=300,
        random_seed=123,
    )

    params = AnomalyParams(
        sample_fraction=0.5,
        algorithm_config=custom_algo_config,
    )

    assert params.sample_fraction == 0.5
    assert params.algorithm_config.contamination == 0.15
    assert params.algorithm_config.num_trees == 300
    assert params.algorithm_config.random_seed == 123


def test_anomaly_params_all_custom():
    """Test AnomalyParams with all custom values."""
    params = AnomalyParams(
        sample_fraction=0.4,
        max_rows=500_000,
        train_ratio=0.75,
        ensemble_size=7,
        algorithm_config=IsolationForestConfig(contamination=0.08, num_trees=250),
    )

    assert params.sample_fraction == 0.4
    assert params.max_rows == 500_000
    assert params.train_ratio == 0.75
    assert params.ensemble_size == 7
    assert params.algorithm_config.contamination == 0.08
    assert params.algorithm_config.num_trees == 250


# ============================================================================
# AnomalyConfig Tests
# ============================================================================


def test_anomaly_config_defaults():
    """Test AnomalyConfig defaults."""
    cfg = AnomalyConfig()
    assert cfg.columns is None
    assert cfg.segment_by is None
    assert cfg.model_name is None
    assert cfg.registry_table is None


def test_anomaly_config_with_columns_and_segments():
    """Test AnomalyConfig with custom columns and segmentation."""
    cfg = AnomalyConfig(
        columns=["a", "b"],
        segment_by=["region"],
        model_name="demo_model",
        registry_table="main.default.dqx_anomaly_models",
    )
    assert cfg.columns == ["a", "b"]
    assert cfg.segment_by == ["region"]
    assert cfg.model_name == "demo_model"
    assert cfg.registry_table == "main.default.dqx_anomaly_models"


def test_anomaly_config_with_single_column():
    """Test AnomalyConfig with a single column."""
    cfg = AnomalyConfig(columns=["single_col"])
    assert cfg.columns == ["single_col"]
    assert len(cfg.columns) == 1


def test_anomaly_config_with_many_columns():
    """Test AnomalyConfig with many columns."""
    columns = [f"col_{i}" for i in range(10)]
    cfg = AnomalyConfig(columns=columns)
    assert cfg.columns == columns
    assert len(cfg.columns) == 10


def test_anomaly_config_empty_columns():
    """Test AnomalyConfig with empty column list."""
    cfg = AnomalyConfig(columns=[])
    assert cfg.columns == []
    assert len(cfg.columns) == 0


# ============================================================================
# FeatureEngineeringConfig Tests
# ============================================================================


def test_feature_engineering_config_defaults():
    """Test FeatureEngineeringConfig with default values."""
    cfg = FeatureEngineeringConfig()

    # Check defaults exist
    assert hasattr(cfg, "categorical_cardinality_threshold")
    assert hasattr(cfg, "max_input_columns")
    assert hasattr(cfg, "max_engineered_features")


def test_feature_engineering_config_custom_thresholds():
    """Test FeatureEngineeringConfig with custom threshold values."""
    cfg = FeatureEngineeringConfig(
        categorical_cardinality_threshold=10,
        max_input_columns=15,
        max_engineered_features=100,
    )

    assert cfg.categorical_cardinality_threshold == 10
    assert cfg.max_input_columns == 15
    assert cfg.max_engineered_features == 100


def test_feature_engineering_config_edge_case_thresholds():
    """Test FeatureEngineeringConfig with edge case values."""
    # Very low thresholds
    cfg1 = FeatureEngineeringConfig(
        categorical_cardinality_threshold=2,
        max_input_columns=1,
        max_engineered_features=5,
    )
    assert cfg1.categorical_cardinality_threshold == 2
    assert cfg1.max_input_columns == 1
    assert cfg1.max_engineered_features == 5

    # Very high thresholds
    cfg2 = FeatureEngineeringConfig(
        categorical_cardinality_threshold=1000,
        max_input_columns=100,
        max_engineered_features=500,
    )
    assert cfg2.categorical_cardinality_threshold == 1000
    assert cfg2.max_input_columns == 100
    assert cfg2.max_engineered_features == 500


# ============================================================================
# Config Combination Tests
# ============================================================================


def test_feature_engineering_config_with_algo_config():
    """Test feature engineering config remains independent of algorithm config."""
    algo_config = IsolationForestConfig(
        contamination=0.12,
        num_trees=250,
        random_seed=777,
    )
    params = AnomalyParams(
        sample_fraction=0.4,
        max_rows=750_000,
        train_ratio=0.85,
        ensemble_size=5,
        algorithm_config=algo_config,
    )
    feature_config = FeatureEngineeringConfig(
        categorical_cardinality_threshold=15,
        max_input_columns=12,
        max_engineered_features=80,
    )

    assert params.algorithm_config.contamination == 0.12
    assert params.algorithm_config.num_trees == 250
    assert feature_config.categorical_cardinality_threshold == 15
    assert feature_config.max_input_columns == 12

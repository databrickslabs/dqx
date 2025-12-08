from databricks.labs.dqx.config import AnomalyConfig, AnomalyParams, IsolationForestConfig


def test_isolation_forest_config_defaults():
    cfg = IsolationForestConfig()
    assert cfg.contamination == 0.1
    assert cfg.num_trees == 200
    assert cfg.random_seed == 42


def test_anomaly_params_defaults():
    params = AnomalyParams()
    assert params.sample_fraction == 0.3
    assert params.max_rows == 1_000_000
    assert params.train_ratio == 0.8
    assert isinstance(params.algorithm_config, IsolationForestConfig)


def test_anomaly_config_accepts_params():
    params = AnomalyParams(sample_fraction=0.5)
    cfg = AnomalyConfig(columns=["a", "b"], params=params)
    assert cfg.columns == ["a", "b"]
    assert cfg.params.sample_fraction == 0.5


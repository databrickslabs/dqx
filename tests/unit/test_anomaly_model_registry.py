from databricks.labs.dqx.anomaly.model_registry import compute_config_hash


def test_compute_config_hash_order_independent() -> None:
    hash_a = compute_config_hash(["b", "a"], ["seg2", "seg1"])
    hash_b = compute_config_hash(["a", "b"], ["seg1", "seg2"])
    assert hash_a == hash_b


def test_compute_config_hash_handles_none_segment_by() -> None:
    hash_a = compute_config_hash(["a", "b"], None)
    hash_b = compute_config_hash(["b", "a"], None)
    assert hash_a == hash_b

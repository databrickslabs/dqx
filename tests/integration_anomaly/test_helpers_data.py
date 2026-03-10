"""Lightweight data-only helpers for anomaly tests (no PySpark/dqx). Use for unit tests and fast imports."""

# Match tests.integration_anomaly.constants for test point "clear_anomaly"
_OUTLIER_AMOUNT = 9999.0
_OUTLIER_QUANTITY = 1.0


def qualify_model_name(model_name: str, registry_table: str) -> str:
    """Return a fully qualified model name using the registry table prefix."""
    if model_name.count(".") >= 2:
        return model_name
    registry_prefix = registry_table.rsplit(".", 1)[0]
    return f"{registry_prefix}.{model_name}"


def get_standard_2d_training_data() -> list[tuple[float, float]]:
    """Standard 2D training data for amount/quantity anomaly detection (400 points)."""
    return [(100.0 + i * 0.5, 10.0 + i * 0.1) for i in range(400)]


def get_standard_3d_training_data() -> list[tuple[float, float, float]]:
    """Standard 3D training data for anomaly detection with contributions (400 points)."""
    return [(100.0 + i * 0.5, 10.0 + i * 0.1, 0.1 + i * 0.001) for i in range(400)]


def get_standard_4d_training_data() -> list[tuple[float, float, float, float]]:
    """Standard 4D training data for multi-column anomaly detection (400 points)."""
    return [
        (100.0 + i * 0.5, 10.0 + i * 0.1, 0.1 + i * 0.001, 50.0 + i * 0.2)
        for i in range(400)
    ]


def get_standard_test_points_2d() -> dict[str, tuple[float, float]]:
    """Pre-validated test points for 2D anomaly detection tests."""
    return {
        "normal_in_center": (200.0, 30.0),
        "normal_near_center": (210.0, 32.0),
        "clear_anomaly": (_OUTLIER_AMOUNT, _OUTLIER_QUANTITY),
    }


def get_standard_test_points_4d() -> dict[str, tuple[float, float, float, float]]:
    """Pre-validated test points for 4D anomaly detection tests."""
    return {
        "normal_in_center": (200.0, 30.0, 0.25, 90.0),
        "clear_anomaly": (_OUTLIER_AMOUNT, _OUTLIER_QUANTITY, 0.95, 1.0),
    }


def get_standard_training_ranges() -> dict[str, dict[str, tuple[float, float]]]:
    """Expected ranges for standard training data (2d and 4d)."""
    return {
        "2d": {"amount": (100.0, 300.0), "quantity": (10.0, 50.0)},
        "4d": {
            "amount": (100.0, 300.0),
            "quantity": (10.0, 50.0),
            "discount": (0.1, 0.5),
            "weight": (50.0, 130.0),
        },
    }

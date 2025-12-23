"""Unit tests for anomaly detection test helper functions."""

import pytest

from tests.integration.test_anomaly_utils import (
    get_standard_2d_training_data,
    get_standard_3d_training_data,
    get_standard_4d_training_data,
    get_standard_test_points_2d,
    get_standard_test_points_4d,
    get_standard_training_ranges,
)


# ============================================================================
# Shared Test Data Constants
# ============================================================================

# Standard feature names for one-hot encoded categorical columns (region + product)
# Used across multiple test files to test feature engineering transformations
STANDARD_REGION_PRODUCT_FEATURES = [
    "region_US",
    "region_EU",
    "region_APAC",
    "product_A",
    "product_B",
    "product_C",
    "product_D",
    "product_E",
]


# ============================================================================
# Training Data Generator Tests
# ============================================================================


def test_get_standard_2d_training_data_shape():
    """Test that 2D training data has correct shape."""
    data = get_standard_2d_training_data()

    assert isinstance(data, list)
    assert len(data) == 400
    assert all(isinstance(point, tuple) for point in data)
    assert all(len(point) == 2 for point in data)


def test_get_standard_2d_training_data_ranges():
    """Test that 2D training data has expected value ranges."""
    data = get_standard_2d_training_data()

    amounts = [point[0] for point in data]
    quantities = [point[1] for point in data]

    # Check amount range: 100.0 to 300.0 (step 0.5)
    assert min(amounts) == 100.0
    assert max(amounts) == pytest.approx(100.0 + 399 * 0.5)

    # Check quantity range: 10.0 to 50.0 (step 0.1)
    assert min(quantities) == 10.0
    assert max(quantities) == pytest.approx(10.0 + 399 * 0.1)


def test_get_standard_2d_training_data_types():
    """Test that 2D training data contains float values."""
    data = get_standard_2d_training_data()

    for amount, quantity in data:
        assert isinstance(amount, float)
        assert isinstance(quantity, float)


def test_get_standard_3d_training_data_shape():
    """Test that 3D training data has correct shape."""
    data = get_standard_3d_training_data()

    assert isinstance(data, list)
    assert len(data) == 400
    assert all(isinstance(point, tuple) for point in data)
    assert all(len(point) == 3 for point in data)


def test_get_standard_3d_training_data_ranges():
    """Test that 3D training data has expected value ranges."""
    data = get_standard_3d_training_data()

    amounts = [point[0] for point in data]
    quantities = [point[1] for point in data]
    discounts = [point[2] for point in data]

    # Check amount range
    assert min(amounts) == 100.0
    assert max(amounts) == pytest.approx(100.0 + 399 * 0.5)

    # Check quantity range
    assert min(quantities) == 10.0
    assert max(quantities) == pytest.approx(10.0 + 399 * 0.1)

    # Check discount range: 0.1 to 0.5 (step 0.001)
    assert min(discounts) == pytest.approx(0.1)
    assert max(discounts) == pytest.approx(0.1 + 399 * 0.001)


def test_get_standard_4d_training_data_shape():
    """Test that 4D training data has correct shape."""
    data = get_standard_4d_training_data()

    assert isinstance(data, list)
    assert len(data) == 400
    assert all(isinstance(point, tuple) for point in data)
    assert all(len(point) == 4 for point in data)


def test_get_standard_4d_training_data_ranges():
    """Test that 4D training data has expected value ranges."""
    data = get_standard_4d_training_data()

    amounts = [point[0] for point in data]
    quantities = [point[1] for point in data]
    discounts = [point[2] for point in data]
    weights = [point[3] for point in data]

    # Check all ranges
    assert min(amounts) == 100.0
    assert min(quantities) == 10.0
    assert min(discounts) == pytest.approx(0.1)
    assert min(weights) == 50.0

    # Weight range: 50.0 to 130.0 (step 0.2)
    assert max(weights) == pytest.approx(50.0 + 399 * 0.2)


def test_get_standard_4d_training_data_types():
    """Test that 4D training data contains float values."""
    data = get_standard_4d_training_data()

    for amount, quantity, discount, weight in data:
        assert isinstance(amount, float)
        assert isinstance(quantity, float)
        assert isinstance(discount, float)
        assert isinstance(weight, float)


# ============================================================================
# Test Points Generator Tests
# ============================================================================


def test_get_standard_test_points_2d_structure():
    """Test that 2D test points dict has expected structure."""
    test_points = get_standard_test_points_2d()

    assert isinstance(test_points, dict)
    assert "normal_in_center" in test_points
    assert "normal_near_center" in test_points
    assert "clear_anomaly" in test_points


def test_get_standard_test_points_2d_types():
    """Test that 2D test points contain tuples of floats."""
    test_points = get_standard_test_points_2d()

    for _key, value in test_points.items():
        assert isinstance(value, tuple)
        assert len(value) == 2
        assert isinstance(value[0], float)
        assert isinstance(value[1], float)


def test_get_standard_test_points_2d_values():
    """Test that 2D test points have expected values."""
    test_points = get_standard_test_points_2d()

    # Normal points should be in training range (100-300, 10-50)
    normal_in_center = test_points["normal_in_center"]
    assert 100.0 <= normal_in_center[0] <= 300.0
    assert 10.0 <= normal_in_center[1] <= 50.0

    # Anomaly point should be far outside
    anomaly = test_points["clear_anomaly"]
    assert anomaly[0] > 300.0 or anomaly[0] < 100.0 or anomaly[1] > 50.0 or anomaly[1] < 10.0


def test_get_standard_test_points_4d_structure():
    """Test that 4D test points dict has expected structure."""
    test_points = get_standard_test_points_4d()

    assert isinstance(test_points, dict)
    assert "normal_in_center" in test_points
    assert "clear_anomaly" in test_points


def test_get_standard_test_points_4d_types():
    """Test that 4D test points contain tuples of floats."""
    test_points = get_standard_test_points_4d()

    for _key, value in test_points.items():
        assert isinstance(value, tuple)
        assert len(value) == 4
        assert all(isinstance(v, float) for v in value)


def test_get_standard_test_points_4d_values():
    """Test that 4D test points have expected values."""
    test_points = get_standard_test_points_4d()

    # Normal point should be in reasonable ranges
    normal = test_points["normal_in_center"]
    assert 100.0 <= normal[0] <= 300.0  # amount
    assert 10.0 <= normal[1] <= 50.0  # quantity
    assert 0.1 <= normal[2] <= 0.5  # discount
    assert 50.0 <= normal[3] <= 130.0  # weight

    # Anomaly point should have at least one dimension far outside
    anomaly = test_points["clear_anomaly"]
    # At least one value should be anomalous
    assert (
        anomaly[0] > 300.0
        or anomaly[0] < 100.0
        or anomaly[1] > 50.0
        or anomaly[1] < 10.0
        or anomaly[2] > 0.5
        or anomaly[2] < 0.1
        or anomaly[3] > 130.0
        or anomaly[3] < 50.0
    )


# ============================================================================
# Training Ranges Tests
# ============================================================================


def test_get_standard_training_ranges_structure():
    """Test that training ranges dict has expected structure."""
    ranges = get_standard_training_ranges()

    assert isinstance(ranges, dict)
    assert "2d" in ranges
    assert "4d" in ranges


def test_get_standard_training_ranges_2d():
    """Test 2D training ranges have correct column names and values."""
    ranges = get_standard_training_ranges()
    ranges_2d = ranges["2d"]

    assert "amount" in ranges_2d
    assert "quantity" in ranges_2d

    # Each column should have (min, max) tuple
    assert isinstance(ranges_2d["amount"], tuple)
    assert len(ranges_2d["amount"]) == 2
    assert isinstance(ranges_2d["quantity"], tuple)
    assert len(ranges_2d["quantity"]) == 2

    # Verify ranges match training data
    amount_min, amount_max = ranges_2d["amount"]
    quantity_min, quantity_max = ranges_2d["quantity"]

    assert amount_min == 100.0
    assert amount_max == pytest.approx(299.5, rel=0.01)
    assert quantity_min == 10.0
    assert quantity_max == pytest.approx(49.9, rel=0.01)


def test_get_standard_training_ranges_4d():
    """Test 4D training ranges have correct column names and values."""
    ranges = get_standard_training_ranges()
    ranges_4d = ranges["4d"]

    assert "amount" in ranges_4d
    assert "quantity" in ranges_4d
    assert "discount" in ranges_4d
    assert "weight" in ranges_4d

    # Each column should have (min, max) tuple
    for _col_name, (min_val, max_val) in ranges_4d.items():
        assert isinstance(min_val, float)
        assert isinstance(max_val, float)
        assert min_val < max_val


# ============================================================================
# Data Consistency Tests
# ============================================================================


def test_training_data_consistency():
    """Test that all training data functions return consistent data."""
    # 2D data
    data_2d = get_standard_2d_training_data()
    assert len(data_2d) == 400

    # 3D data
    data_3d = get_standard_3d_training_data()
    assert len(data_3d) == 400

    # 4D data
    data_4d = get_standard_4d_training_data()
    assert len(data_4d) == 400

    # All should have same number of samples
    assert len(data_2d) == len(data_3d) == len(data_4d)


def test_test_points_consistency():
    """Test that test points align with training data ranges."""
    ranges = get_standard_training_ranges()
    test_points_2d = get_standard_test_points_2d()

    # Normal points should be within training ranges
    normal_point = test_points_2d["normal_in_center"]
    amount_min, amount_max = ranges["2d"]["amount"]
    quantity_min, quantity_max = ranges["2d"]["quantity"]

    assert amount_min <= normal_point[0] <= amount_max
    assert quantity_min <= normal_point[1] <= quantity_max


def test_data_generators_are_deterministic():
    """Test that data generators produce consistent results."""
    # Generate data twice
    data1 = get_standard_2d_training_data()
    data2 = get_standard_2d_training_data()

    # Should be identical
    assert len(data1) == len(data2)
    assert data1 == data2


def test_test_points_are_deterministic():
    """Test that test points are deterministic."""
    points1 = get_standard_test_points_2d()
    points2 = get_standard_test_points_2d()

    assert points1 == points2
    assert points1["normal_in_center"] == points2["normal_in_center"]
    assert points1["clear_anomaly"] == points2["clear_anomaly"]

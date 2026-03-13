"""Shared constants for unit tests that need anomaly-related test data (no PySpark/dqx imports)."""

# Standard feature names for one-hot encoded categorical columns (region + product).
# Used by unit tests for feature engineering transformations.
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

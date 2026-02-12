"""Validation utilities for anomaly detection."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import sklearn

if TYPE_CHECKING:
    from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord


def _parse_version_tuple(version_str: str) -> tuple[int, int]:
    """Parse version string into (major, minor) tuple."""
    parts = version_str.split(".")
    major = int(parts[0])
    minor = int(parts[1]) if len(parts) > 1 else 0
    return major, minor


def validate_sklearn_compatibility(model_record: AnomalyModelRecord) -> None:
    """Validate sklearn version compatibility between training and inference.

    Args:
        model_record: Model record containing sklearn_version from training

    Raises:
        Warning if minor version mismatch detected (e.g., 1.2.x vs 1.3.x)

    Example:
        >>> record = AnomalyModelRecord(...)
        >>> validate_sklearn_compatibility(record)
        # Warns if sklearn versions don't match
    """
    if not model_record.segmentation.sklearn_version:
        # Old models without version tracking - can't validate
        return

    trained_version = model_record.segmentation.sklearn_version
    current_version = sklearn.__version__

    if trained_version == current_version:
        return  # Perfect match

    # Parse versions
    try:
        trained_major, trained_minor = _parse_version_tuple(trained_version)
        current_major, current_minor = _parse_version_tuple(current_version)

        # Check for version mismatches
        if trained_major != current_major or trained_minor != current_minor:
            warnings.warn(
                f"\nSKLEARN VERSION MISMATCH DETECTED\n"
                f"Model Information:\n"
                f"  - Model: {model_record.identity.model_name}\n"
                f"  - Trained with: sklearn {trained_version}\n"
                f"  - Current environment: sklearn {current_version}\n"
                f"\n"
                f"Minor version changes (e.g., 1.2 -> 1.3) can cause pickle incompatibility errors.\n"
                f"\n"
                f"RECOMMENDED ACTION:\n"
                f"Retrain the model with your current sklearn version:\n"
                f"  anomaly_engine.train(\n"
                f"      df=df_train,\n"
                f"      model_name=\"{model_record.identity.model_name}\",\n"
                f"      columns={model_record.training.columns}\n"
                f"  )\n",
                UserWarning,
                stacklevel=3,
            )
    except (ValueError, IndexError):
        # Couldn't parse version - skip validation
        pass

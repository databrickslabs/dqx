"""Load and validate sklearn anomaly models from MLflow."""

import sys
from datetime import datetime
from typing import Any
import warnings

import sklearn
import mlflow
import mlflow.sklearn

from databricks.labs.dqx.anomaly.mlflow_registry import get_default_registry
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord
from databricks.labs.dqx.anomaly.validation import validate_sklearn_compatibility


def load_sklearn_model_with_error_handling(model_uri: str, model_record: AnomalyModelRecord) -> Any:
    """Load sklearn model from MLflow with graceful error handling.

    Args:
        model_uri: MLflow model URI
        model_record: Model record with metadata for error messages

    Returns:
        Loaded sklearn model

    Raises:
        RuntimeError with actionable error message if loading fails
    """
    get_default_registry().ensure_registry_configured()

    try:
        return mlflow.sklearn.load_model(model_uri)
    except (ValueError, AttributeError, TypeError) as e:
        error_msg = str(e)
        trained_version = model_record.segmentation.sklearn_version or "unknown"
        current_version = sklearn.__version__
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"

        raise RuntimeError(
            f"\nFAILED TO LOAD ANOMALY DETECTION MODEL\n"
            f"\n"
            f"Model Information:\n"
            f"  - Model: {model_record.identity.model_name}\n"
            f"  - Trained with: sklearn {trained_version}\n"
            f"  - Current environment: sklearn {current_version}, Python {python_version}\n"
            f"  - Model URI: {model_uri}\n"
            f"\n"
            f"Error Details:\n"
            f"  {type(e).__name__}: {error_msg}\n"
            f"\n"
            f"This typically happens when scikit-learn's internal pickle format changes between versions.\n"
            f"Common causes:\n"
            f"  - Major/minor sklearn version mismatch (e.g., 1.2.x vs 1.3.x)\n"
            f"  - Python version changes (e.g., 3.11 vs 3.12)\n"
            f"  - Databricks runtime upgrades\n"
            f"\n"
            f"SOLUTIONS:\n"
            f"\n"
            f"1. Retrain the model (RECOMMENDED):\n"
            f"   anomaly_engine.train(\n"
            f"       df=df_train,\n"
            f"       model_name=\"{model_record.identity.model_name}\",\n"
            f"       columns={model_record.training.columns},\n"
            f"       registry_table=\"<your_registry_table>\"\n"
            f"   )\n"
            f"\n"
            f"2. Downgrade sklearn (temporary workaround):\n"
            f"   %pip install scikit-learn=={trained_version}\n"
            f"   # Then restart Python kernel\n"
            f"\n"
            f"3. Clear registry and retrain all models:\n"
            f"   spark.sql(\"DROP TABLE IF EXISTS <your_registry_table>\")\n"
            f"   # Then retrain all models\n"
        ) from e


def load_and_validate_model(model_uri: str, model_record: AnomalyModelRecord) -> Any:
    """Load model with validation and error handling.

    Args:
        model_uri: MLflow model URI
        model_record: Model record for version validation

    Returns:
        Loaded sklearn model
    """
    validate_sklearn_compatibility(model_record)
    return load_sklearn_model_with_error_handling(model_uri, model_record)


def check_model_staleness(record: AnomalyModelRecord, model_name: str) -> None:
    """Check model training age and issue warning if stale (>30 days)."""
    if record.training.training_time:
        age_days = (datetime.utcnow() - record.training.training_time).days
        if age_days > 30:
            warnings.warn(
                f"Model '{model_name}' is {age_days} days old. Consider retraining.",
                UserWarning,
                stacklevel=3,
            )

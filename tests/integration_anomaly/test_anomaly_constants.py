from datetime import datetime, timezone


from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.schema import dq_result_schema

# Shared constants for anomaly integration tests to keep expectations deterministic.
REPORTING_COLUMNS = f", _errors: {dq_result_schema.simpleString()}, _warnings: {dq_result_schema.simpleString()}"
RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
RUN_ID = "2f9120cf-e9f2-446a-8278-12d508b00639"
EXTRA_PARAMS = ExtraParams(run_time_overwrite=RUN_TIME.isoformat(), run_id_overwrite=RUN_ID)

# Default thresholds used across anomaly integration tests.
DEFAULT_SCORE_THRESHOLD = 0.5
DQENGINE_SCORE_THRESHOLD = 0.6
DRIFT_THRESHOLD = 3.0
DRIFT_TRAIN_SIZE = 1500

# Commonly used “obvious anomaly” values for 2D features.
OUTLIER_AMOUNT = 9999.0
OUTLIER_QUANTITY = 1.0

# Common segment values used for region-based segmentation tests.
SEGMENT_REGIONS = ("US", "EU", "APAC")

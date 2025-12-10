# 🚨 Critical Blockers for Anomaly Tests

## Blocker 1: Decimal Conversion in Model Registry

## Issue
PyArrow cannot convert `Decimal` values to `double` in Spark Connect mode when saving model records to the registry.

## Error
```
pyarrow.lib.ArrowInvalid: Could not convert Decimal('2.00000') with type decimal.Decimal: tried to convert to double
```

## Location
- **File**: `src/databricks/labs/dqx/anomaly/model_registry.py`
- **Line**: 62
- **Method**: `build_model_df()`

## Root Cause
When creating DataFrame from `record.__dict__`, the `baseline_stats` dictionary contains `Decimal` values from Spark aggregations (mean, stddev, etc.) that PyArrow cannot automatically convert to doubles in Spark Connect mode.

## Impact
Affects tests that use `spark.range()` to generate training data:
- ❌ `test_anomaly_sampling.py` - All 8 tests fail
- Potentially other files using similar data generation

## Tests that Work (Not Affected)
- ✅ `test_anomaly_registry.py` - 8/8 PASS (uses explicit `spark.createDataFrame()`)
- ✅ `test_anomaly_threshold.py` - 7/7 PASS (uses explicit `spark.createDataFrame()`)

## Fix Required
Convert Decimal values to float in `build_model_df()` before creating DataFrame:

```python
def build_model_df(self, spark: SparkSession, record: AnomalyModelRecord) -> DataFrame:
    # Convert Decimal values in baseline_stats to float for PyArrow compatibility
    record_dict = record.__dict__.copy()
    if record_dict.get("baseline_stats"):
        record_dict["baseline_stats"] = {
            k: float(v) if isinstance(v, Decimal) else v
            for k, v in record_dict["baseline_stats"].items()
        }
    
    return spark.createDataFrame([record_dict], schema=ANOMALY_MODEL_TABLE_SCHEMA)
```

## Priority
**HIGH** - Blocks 8+ integration tests from passing.

## Workaround for Testing
Use explicit `spark.createDataFrame()` with typed schemas instead of `spark.range()`.

---

## Blocker 2: YAML Check Resolution Fails for Anomaly Functions

### Issue
Anomaly check functions (`has_no_anomalies`) are not properly registered with the checks_resolver for YAML-based metadata resolution.

### Error
```
databricks.labs.dqx.errors.InvalidCheckError: function 'has_no_anomalies' is not defined
```

### Debug Log
```
DEBUG [d.l.dqx.checks_resolver] Function has_no_anomalies resolved successfully: None
```

### Location
- **File**: `src/databricks/labs/dqx/checks_resolver.py`
- **Issue**: The function resolves to `None` instead of the actual function object

### Root Cause
When YAML checks are parsed, `apply_checks_by_metadata()` calls `checks_resolver` to look up functions by name. The anomaly module is conditionally imported (wrapped in try/except), but the functions aren't being properly exported or registered in the resolver's lookup mechanism.

### Impact
Affects YAML-based tests:
- ❌ `test_anomaly_yaml.py` - 6/7 tests fail (only the validation test passes since it doesn't actually run checks)

### Fix Required
Ensure anomaly check functions are properly registered in the checks_resolver module map. The conditional import in `checks_resolver.py` needs to properly expose the functions, not just import the module.

Current code (line 9-10):
```python
try:
    from databricks.labs.dqx.anomaly import check_funcs as anomaly_check_funcs
    ANOMALY_ENABLED = True
except ImportError:
    ANOMALY_ENABLED = False
```

The functions need to be added to a module-level dictionary or the resolver's lookup mechanism.

### Priority
**MEDIUM** - Blocks 6 integration tests, but workaround exists (use direct function calls instead of YAML).


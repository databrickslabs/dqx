from databricks.labs.dqx.telemetry import is_dlt_pipeline, count_tables_in_spark_plan


class DummySparkConf:
    def __init__(self, value=None, raise_exc=False):
        self.value = value
        self.raise_exc = raise_exc

    def get(self, key, default=None):
        if self.raise_exc:
            raise RuntimeError("Config error")
        if key == "pipelines.id":
            return self.value
        return default


class DummySparkSession:
    def __init__(self, conf):
        self.conf = conf


class MockDataFrameWithPlan:
    """Mock DataFrame that prints a specific plan when explain() is called."""

    def __init__(self, plan_output):
        self.plan_output = plan_output

    def explain(self, extended=False):
        print(self.plan_output, end='')


class MockDataFrameWithException:
    """Mock DataFrame that raises an exception when explain() is called."""

    def __init__(self, exception):
        self.exception = exception

    def explain(self, extended=False):
        raise self.exception


def test_is_dlt_pipeline_true():
    dummy_spark = DummySparkSession(DummySparkConf(value="dlt-123"))
    assert is_dlt_pipeline(dummy_spark) is True


def test_is_dlt_pipeline_false_missing_id():
    dummy_spark = DummySparkSession(DummySparkConf(value=None))
    assert is_dlt_pipeline(dummy_spark) is False


def test_is_dlt_pipeline_false_exception():
    dummy_spark = DummySparkSession(DummySparkConf(raise_exc=True))
    assert is_dlt_pipeline(dummy_spark) is False


def test_count_tables_with_missing_analyzed_plan_section():
    """Test that count_tables_in_spark_plan returns 0 when plan has no Analyzed Logical Plan section."""
    # Create a plan string without the "== Analyzed Logical Plan ==" section
    plan_without_analyzed_section = """== Physical Plan ==
Some physical plan details here
== Optimized Logical Plan ==
Some optimized plan details here
"""

    mock_df = MockDataFrameWithPlan(plan_without_analyzed_section)
    count = count_tables_in_spark_plan(mock_df)

    assert count == 0, f"Expected 0 tables when Analyzed Logical Plan section is missing, but found {count}"


def test_count_tables_with_explain_exception():
    """Test that count_tables_in_spark_plan returns 0 when df.explain() raises an exception."""
    mock_df = MockDataFrameWithException(RuntimeError("Failed to explain plan"))

    count = count_tables_in_spark_plan(mock_df)

    assert count == 0, f"Expected 0 tables when explain() raises exception, but found {count}"


def test_count_tables_with_general_exception():
    """Test that count_tables_in_spark_plan returns 0 when an unexpected exception occurs."""
    mock_df = MockDataFrameWithException(ValueError("Unexpected error"))

    count = count_tables_in_spark_plan(mock_df)

    assert count == 0, f"Expected 0 tables when exception occurs, but found {count}"

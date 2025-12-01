from databricks.labs.dqx.telemetry import is_dlt_pipeline


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


def test_is_dlt_pipeline_true():
    dummy_spark = DummySparkSession(DummySparkConf(value="dlt-123"))
    assert is_dlt_pipeline(dummy_spark) is True


def test_is_dlt_pipeline_false_missing_id():
    dummy_spark = DummySparkSession(DummySparkConf(value=None))
    assert is_dlt_pipeline(dummy_spark) is False


def test_is_dlt_pipeline_false_exception():
    dummy_spark = DummySparkSession(DummySparkConf(raise_exc=True))
    assert is_dlt_pipeline(dummy_spark) is False

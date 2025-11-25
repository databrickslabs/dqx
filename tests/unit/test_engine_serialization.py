import pickle
from unittest.mock import MagicMock
from tests.conftest import TestClass


def test_picklable_mixin_basic():
    obj = TestClass()

    assert obj.ws is not None
    assert obj.data == "test_data"

    serialized = pickle.dumps(obj)
    restored = pickle.loads(serialized)

    assert isinstance(restored.ws, MagicMock)
    assert restored.data == "test_data"


def test_picklable_mixin_with_restoration():
    obj = TestClass()
    serialized = pickle.dumps(obj)
    restored = pickle.loads(serialized)

    assert restored.spark == "restored_spark"
    assert restored.restored is True

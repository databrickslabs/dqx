import sys
import importlib
from databricks.labs import dqx


def test_init_module_imports_without_mlflow():
    """Test that dqx module can be imported when mlflow is not available."""

    # Simulate environment without mlflow
    original_mlflow = sys.modules.get('mlflow')

    try:
        # Remove mlflow
        if 'mlflow' in sys.modules:
            del sys.modules['mlflow']

        # Block mlflow import
        sys.modules['mlflow'] = None

        # This should work even without mlflow
        importlib.reload(dqx)

        # Verify basic functionality still works
        assert hasattr(dqx, '__version__')

    finally:
        # Restore original state
        if original_mlflow is not None:
            sys.modules['mlflow'] = original_mlflow
        elif 'mlflow' in sys.modules:
            del sys.modules['mlflow']

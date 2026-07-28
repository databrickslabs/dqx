import importlib
import logging

import databricks.labs.dqx


def test_importing_dqx_does_not_reconfigure_the_root_logger():
    # Regression test for issue #1136: re-running the import must leave the root logger untouched.
    root = logging.getLogger()
    app_handler = logging.StreamHandler()
    original_level = root.level
    root.addHandler(app_handler)
    root.setLevel(logging.WARNING)
    try:
        importlib.reload(databricks.labs.dqx)
        assert app_handler in root.handlers
        assert root.level == logging.WARNING
    finally:
        root.removeHandler(app_handler)
        root.setLevel(original_level)


def test_importing_dqx_attaches_a_null_handler_to_its_own_logger():
    importlib.reload(databricks.labs.dqx)
    dqx_logger = logging.getLogger("databricks.labs.dqx")
    assert any(isinstance(handler, logging.NullHandler) for handler in dqx_logger.handlers)

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


def test_importing_dqx_attaches_a_baseline_stream_handler_to_its_own_logger():
    # DQX must provide a real sink on its own logger so its WARNING/INFO records (e.g. the
    # plaintext-secret warning and the log alert destination) are visible on runtimes where NO
    # handler exists from our logger up to root (e.g. Databricks serverless). Scoped to the DQX
    # logger; root is never touched (covered by the #1136 test above).
    # Simulate a bare environment: clear both the DQX logger's handlers and root's handlers (pytest
    # itself installs a root handler, which would otherwise correctly suppress the baseline sink).
    dqx_logger = logging.getLogger("databricks.labs.dqx")
    dqx_logger.handlers = []
    root = logging.getLogger()
    saved_root_handlers = root.handlers[:]
    root.handlers = []
    try:
        importlib.reload(databricks.labs.dqx)
        real_handlers = [h for h in dqx_logger.handlers if not isinstance(h, logging.NullHandler)]
        assert any(isinstance(h, logging.StreamHandler) for h in real_handlers)
    finally:
        root.handlers = saved_root_handlers


def test_dqx_baseline_handler_not_added_when_app_configured_its_logger():
    # If the application already configured a real handler on the DQX logger, importing DQX must not
    # add a duplicate baseline handler.
    dqx_logger = logging.getLogger("databricks.labs.dqx")
    dqx_logger.handlers = []
    app_handler = logging.StreamHandler()
    dqx_logger.addHandler(app_handler)
    try:
        importlib.reload(databricks.labs.dqx)
        stream_handlers = [h for h in dqx_logger.handlers if isinstance(h, logging.StreamHandler)]
        assert stream_handlers == [app_handler]
    finally:
        dqx_logger.removeHandler(app_handler)


def test_dqx_baseline_handler_not_added_when_root_has_handler():
    # Regression: on runtimes where the ROOT logger already has a handler (e.g. Databricks
    # serverless), DQX must NOT add its own baseline handler, otherwise every DQX record is emitted
    # twice (once by DQX's handler, once by the root handler via propagation).
    dqx_logger = logging.getLogger("databricks.labs.dqx")
    dqx_logger.handlers = []
    root = logging.getLogger()
    root_handler = logging.StreamHandler()
    root.addHandler(root_handler)
    try:
        importlib.reload(databricks.labs.dqx)
        real_handlers = [h for h in dqx_logger.handlers if not isinstance(h, logging.NullHandler)]
        assert real_handlers == []
    finally:
        root.removeHandler(root_handler)

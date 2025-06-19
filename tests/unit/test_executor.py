import pytest
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.executor import (
    DQRowRuleExecutor,
    DQDatasetRuleExecutor,
    DQRuleExecutorFactory,
    DQRowRule,
    DQDatasetRule,
)
from databricks.labs.dqx.rule import DQRule


def test_factory_creates_row_executor():
    rule = DQRowRule(check_func=check_funcs.is_not_null, column="a")
    executor = DQRuleExecutorFactory.create(rule)
    assert isinstance(executor, DQRowRuleExecutor)


def test_factory_creates_dataset_executor():
    rule = DQDatasetRule(check_func=check_funcs.is_unique, columns=["a"])
    executor = DQRuleExecutorFactory.create(rule)
    assert isinstance(executor, DQDatasetRuleExecutor)


def test_factory_raises_for_unknown_rule():
    class UnknownRule(DQRule):
        pass

    with pytest.raises(ValueError):
        DQRuleExecutorFactory.create(
            UnknownRule(
                check_func=check_funcs.is_unique,
            )
        )

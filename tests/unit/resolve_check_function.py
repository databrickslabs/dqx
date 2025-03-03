import pytest
from databricks.labs.dqx.engine import DQEngineCore


def test_resolve_predefined_function():
    result = DQEngineCore.resolve_check_function('is_not_null')
    assert result


def custom_global_func():
    pass


def test_resolve_function_with_globals():
    result = DQEngineCore.resolve_check_function('custom_global_func', {"custom_global_func": custom_global_func})
    assert result

    result = DQEngineCore.resolve_check_function('custom_global_func', globals())
    assert result


def test_resolve_function_with_locals():
    def custom_local_func():
        pass

    result = DQEngineCore.resolve_check_function('custom_local_func', {"custom_local_func": custom_local_func})
    assert result

    result = DQEngineCore.resolve_check_function('custom_local_func', locals())
    assert result


def test_resolve_function_fail_on_missing():
    with pytest.raises(AttributeError):
        DQEngineCore.resolve_check_function('missing_func', fail_on_missing=True)


def test_resolve_function_not_fail_on_missing():
    result = DQEngineCore.resolve_check_function('missing_func', fail_on_missing=False)
    assert not result

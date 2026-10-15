"""Fast contract tests for the pure-Python generator embedded in the demo notebook."""

import ast
import operator
import random
from datetime import datetime
from pathlib import Path
from types import CodeType, FunctionType

import pytest


def _load_generator(rng: random.Random) -> FunctionType:
    """Load the notebook function without executing Databricks setup or Spark cells."""
    source_path = Path(__file__).parents[2] / "demos" / "dqx_row_anomaly_detection_demo.py"
    source_lines = []
    for line in source_path.read_text().splitlines():
        if line.lstrip().startswith("%"):
            line = line[: len(line) - len(line.lstrip())] + "pass"
        source_lines.append(line)

    tree = ast.parse("\n".join(source_lines), filename=str(source_path))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "inject_anomalies_and_dq_issues"
    )
    namespace = {"random": rng}
    module_code = compile(ast.Module(body=[function], type_ignores=[]), str(source_path), "exec")
    function_code = next(
        constant
        for constant in module_code.co_consts
        if isinstance(constant, CodeType) and constant.co_name == function.name
    )
    return FunctionType(function_code, namespace)


@pytest.fixture(name="inject")
def fixture_inject() -> FunctionType:
    return _load_generator(random.Random(42))


@pytest.fixture(name="base_rows")
def fixture_base_rows() -> list[tuple]:
    return [("TXN000001", datetime(2024, 1, 1, 9), 200.0, 5, "Food", "North")]


@pytest.fixture(name="many_base_rows")
def fixture_many_base_rows() -> list[tuple]:
    return [(f"TXN{i:06d}", datetime(2024, 1, 1, 9), 200.0 + i, 5 + i % 3, "Food", "North") for i in range(32)]


def test_negative_amount_fault_survives_the_generator_clamp(inject, base_rows):
    """The forced DQ fault must remain invalid so the walkthrough can show it being caught."""
    rows = inject(
        base_rows,
        anomaly_rate=0.0,
        dq_null_amount_rate=0.0,
        dq_null_quantity_rate=0.0,
        dq_negative_amount_rate=1.0,
    )

    assert rows == [("NEW000000", datetime(2024, 1, 1, 9), -200.0, 5, "Food", "North", False)]


def test_clean_rows_keep_their_values_when_no_fault_is_injected(inject, base_rows):
    """The fix must not alter ordinary generated data or the deterministic ID/label contract."""
    rows = inject(
        base_rows,
        anomaly_rate=0.0,
        dq_null_amount_rate=0.0,
        dq_null_quantity_rate=0.0,
        dq_negative_amount_rate=0.0,
    )

    assert rows == [("NEW000000", datetime(2024, 1, 1, 9), 200.0, 5, "Food", "North", False)]


def test_negative_fault_changes_only_amount_across_a_fixed_seed_batch(inject, many_base_rows):
    """The DQ fault preserves IDs, timestamps, dimensions, quantities, and labels for every row."""
    clean_rows = inject(
        many_base_rows,
        anomaly_rate=0.0,
        dq_null_amount_rate=0.0,
        dq_null_quantity_rate=0.0,
        dq_negative_amount_rate=0.0,
    )
    injected_generator = _load_generator(random.Random(42))
    injected_rows = operator.call(
        injected_generator,
        many_base_rows,
        anomaly_rate=0.0,
        dq_null_amount_rate=0.0,
        dq_null_quantity_rate=0.0,
        dq_negative_amount_rate=1.0,
    )

    assert len(clean_rows) == len(injected_rows) == len(many_base_rows)
    for clean, injected in zip(clean_rows, injected_rows, strict=True):
        assert injected[0] == clean[0]
        assert injected[1] == clean[1]
        assert injected[2] == -clean[2]
        assert injected[3:] == clean[3:]


def test_other_synthetic_anomaly_path_keeps_its_exact_seeded_transformation(inject, base_rows):
    """The shared clamp still applies to ordinary synthetic anomalies."""
    rows = inject(
        base_rows,
        anomaly_rate=1.0,
        dq_null_amount_rate=0.0,
        dq_null_quantity_rate=0.0,
        dq_negative_amount_rate=0.0,
    )

    assert rows == [("NEW000000", datetime(2024, 1, 1, 9), 3446.42, 111, "Food", "North", True)]


@pytest.mark.parametrize(
    ("dq_null_amount_rate", "dq_null_quantity_rate", "expected"),
    [
        pytest.param(1.0, 0.0, (None, 5), id="null_amount"),
        pytest.param(0.0, 1.0, (200.0, None), id="null_quantity"),
    ],
)
def test_existing_null_faults_keep_their_seeded_values(
    inject, base_rows, dq_null_amount_rate, dq_null_quantity_rate, expected
):
    rows = inject(
        base_rows,
        anomaly_rate=0.0,
        dq_null_amount_rate=dq_null_amount_rate,
        dq_null_quantity_rate=dq_null_quantity_rate,
        dq_negative_amount_rate=0.0,
    )

    assert (rows[0][2], rows[0][3]) == expected

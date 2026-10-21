"""Pure contracts for the integration anomaly quality-metric helpers."""

import numpy as np
import pandas as pd
import pytest

from tests.integration_anomaly.quality_metrics import per_group_metrics, trivial_baselines


def test_per_group_metrics_accounts_for_a_null_group_key() -> None:
    frame = pd.DataFrame(
        {
            "group": ["known", None],
            "label": [0, 1],
            "score": [0.1, 0.9],
            "flagged": [0, 1],
        }
    )

    result = per_group_metrics(frame, group_col="group")

    assert len(result) == 2
    assert result["n"].sum() == len(frame)
    null_group = result[result["group"].isna()]
    assert len(null_group) == 1
    assert null_group.iloc[0]["n"] == 1


@pytest.mark.parametrize("bad_value", [np.nan, np.inf, -np.inf])
def test_trivial_baselines_reject_nonfinite_metric_values(bad_value: float) -> None:
    frame = pd.DataFrame({"label": [0, 0], "metric": [1.0, bad_value]})

    with pytest.raises(ValueError, match="metric_cols must contain only finite values"):
        trivial_baselines(frame, ["metric"])

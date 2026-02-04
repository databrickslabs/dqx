import types

import pytest

from databricks.labs.dqx.anomaly.explainer import add_top_contributors_to_message
from databricks.labs.dqx.errors import InvalidParameterError


def test_add_top_contributors_requires_severity_column():
    """Missing severity_percentile/_dq_info should raise InvalidParameterError."""

    with pytest.raises(InvalidParameterError, match="severity_percentile is required"):
        add_top_contributors_to_message(types.SimpleNamespace(columns=[]), threshold=60.0)

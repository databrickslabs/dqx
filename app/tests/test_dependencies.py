"""Tests for application dependency boundaries."""

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.dependencies import get_sp_oltp_executor, set_oltp_executor


@pytest.mark.asyncio
async def test_oltp_dependency_fails_closed_until_lakebase_is_registered() -> None:
    set_oltp_executor(None)

    with pytest.raises(HTTPException) as raised:
        await get_sp_oltp_executor()

    assert raised.value.status_code == 503
    assert raised.value.detail == "DQX Studio setup is not ready."

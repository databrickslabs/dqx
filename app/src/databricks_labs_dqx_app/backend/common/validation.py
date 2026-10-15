"""Shared request-validation helpers for route handlers."""

from collections.abc import Callable
from typing import TypeVar

from fastapi import HTTPException

T = TypeVar("T")


def require_valid(validator: Callable[[T], object], value: T) -> None:
    """Run *validator(value)* and translate a *ValueError* into HTTP 400.

    Centralizes the validate-then-400 pattern shared by the status and results
    endpoints so the 400 contract (status code, detail shape) lives in one place.
    """
    try:
        validator(value)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

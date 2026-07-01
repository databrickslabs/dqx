"""Pydantic field support for *str | DQSecret* values.

Destination credentials (webhook URLs, basic-auth username/password) may be a
plain string (local dev) or a *DQSecret* scope/key reference. This module
provides the *SecretOrStr* annotated type so Pydantic models can validate and
round-trip those values while preserving the tagged wire format
(a dict mapping the key *secret* to a *scope/key* reference) used in storage.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BeforeValidator, PlainSerializer

from databricks.labs.dqx.config import DQSecret


def _parse_secret_or_str(value: object) -> object:
    """Convert a tagged dict (whose only key is *secret*, mapping to a *scope/key* reference) into a *DQSecret*.

    Plain strings and existing *DQSecret* instances pass through unchanged.

    Args:
        value: The raw value from a serialized payload or direct construction.

    Returns:
        A *DQSecret* when *value* is the tagged dict form, otherwise *value* unchanged.
    """
    if isinstance(value, dict) and set(value.keys()) == {"secret"}:
        return DQSecret.from_reference(str(value["secret"]))
    return value


def _dump_secret_or_str(value: object) -> object:
    """Serialize a *DQSecret* to its tagged dict form (the key *secret* mapping to a *scope/key* reference).

    Plain strings (and *None*) are returned unchanged so the tagged form is only
    used for *DQSecret* values.

    Args:
        value: A plain string, a *DQSecret*, or *None*.

    Returns:
        A tagged dict for *DQSecret* values, otherwise *value* unchanged.
    """
    if isinstance(value, DQSecret):
        return {"secret": value.as_reference()}
    return value


# A credential value that is either a plain string or a DQSecret reference.
# On load, a tagged dict is parsed back into a DQSecret; on dump, a DQSecret is
# re-encoded as that tagged dict — so the value round-trips losslessly.
SecretOrStr = Annotated[
    str | DQSecret,
    BeforeValidator(_parse_secret_or_str),
    PlainSerializer(_dump_secret_or_str, return_type=object),
]

__all__ = ["SecretOrStr"]

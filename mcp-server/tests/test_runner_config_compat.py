"""Unit tests for the runner's version-agnostic storage-config helpers.

``dqx_mcp_runner.config_compat`` is stdlib-only (no pyspark / DQX import), so it runs in the
mcp-server test environment. It exists because DQX's storage-config classes are pydantic models in
newer releases and plain dataclasses in older ones; the runner pins a published DQX release that
could be either shape. These tests exercise both shapes so a regression (e.g. assuming
``model_fields`` exists) is caught here rather than only in the live integration test — which is the
failure mode that motivated this module.
"""

import dataclasses
import pathlib
import sys

_RUNNER_SRC = pathlib.Path(__file__).resolve().parent.parent / "runner" / "src"
if str(_RUNNER_SRC) not in sys.path:
    sys.path.insert(0, str(_RUNNER_SRC))

from dqx_mcp_runner.config_compat import config_has_field, config_replace  # noqa: E402


# --- Dataclass-shaped config (older DQX releases, e.g. 0.15.0) ---------------------------------


@dataclasses.dataclass
class _DataclassConfig:
    location: str
    mode: str = "append"


# --- Pydantic-shaped config (newer DQX releases) ------------------------------------------------
# A minimal stand-in that mimics the two attributes the helpers rely on — a ``model_fields`` class
# attribute and a ``replace()`` method — without depending on pydantic being installed.


class _PydanticLikeConfig:
    model_fields = {"location": None, "mode": None}

    def __init__(self, location: str, mode: str = "append"):
        self.location = location
        self.mode = mode

    def replace(self, **changes):
        fields = {"location": self.location, "mode": self.mode}
        fields.update(changes)
        return type(self)(**fields)


class TestConfigHasField:
    def test_dataclass_declared_field(self):
        assert config_has_field(_DataclassConfig(location="c.s.t"), "mode") is True

    def test_dataclass_missing_field(self):
        assert config_has_field(_DataclassConfig(location="c.s.t"), "instance_name") is False

    def test_pydantic_declared_field(self):
        assert config_has_field(_PydanticLikeConfig(location="c.s.t"), "mode") is True

    def test_pydantic_missing_field(self):
        assert config_has_field(_PydanticLikeConfig(location="c.s.t"), "instance_name") is False

    def test_non_config_object(self):
        assert config_has_field(object(), "mode") is False


class TestConfigReplace:
    def test_dataclass_replace_returns_new_instance_with_override(self):
        original = _DataclassConfig(location="c.s.t", mode="append")
        updated = config_replace(original, mode="overwrite")
        assert updated.mode == "overwrite"
        assert updated.location == "c.s.t"
        assert original.mode == "append"  # original untouched

    def test_pydantic_replace_prefers_configs_own_method(self):
        original = _PydanticLikeConfig(location="c.s.t", mode="append")
        updated = config_replace(original, mode="overwrite")
        assert updated.mode == "overwrite"
        assert updated.location == "c.s.t"
        assert original.mode == "append"
        assert isinstance(updated, _PydanticLikeConfig)

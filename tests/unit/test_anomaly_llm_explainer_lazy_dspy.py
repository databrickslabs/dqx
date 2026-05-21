"""Verify P0-1: the explainer must not pull DSPy at module-load time.

The original bug: ``anomaly_llm_explainer`` did
``from databricks.labs.dqx.llm.llm_core import LLMModelConfigurator`` at module top, and
``llm_core`` does ``import dspy`` unconditionally. So users on ``executor='ai_query'`` who
installed ``[anomaly]`` but not ``[llm]`` hit ImportError on import, contradicting the
documented install matrix.

A full runtime simulation (subprocess + meta_path block on *dspy*) was tried and rejected:
the explainer's transitive imports include heavy ML deps (numba via shap) that frequently
break for reasons unrelated to *dspy* — making the test a brittle proxy for the property we
actually want to lock. AST-level structural assertions are the chosen tradeoff: they are
narrow but precise, run in milliseconds, and fail loudly on the exact regression shape that
caused P0-1 (eager top-level import of *LLMModelConfigurator*, or removal of the lazy
import inside *build_language_model*).
"""

from __future__ import annotations

import ast
import importlib.util
import pathlib


def _explainer_path() -> pathlib.Path:
    """Resolve the explainer module's source file via importlib.

    Avoids hardcoding the *tests/unit* → *src/...* path layout — survives any future
    test-tree reorganisation.
    """
    spec = importlib.util.find_spec("databricks.labs.dqx.anomaly.anomaly_llm_explainer")
    assert spec is not None and spec.origin is not None, "explainer module not importable"
    return pathlib.Path(spec.origin)


def _module_top_imports(path: pathlib.Path) -> set[str]:
    """Return the set of fully-qualified names imported at module level (not inside functions)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                names.add(f"{module}.{alias.name}")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
    return names


def _function_local_imports(path: pathlib.Path, func_name: str) -> set[str]:
    """Return the set of fully-qualified names imported inside *func_name*'s body.

    Looks only at top-level functions (``tree.body``) — nested functions with the same
    name in unrelated scopes won't shadow the target. Handles both *ImportFrom* and
    *Import* so refactors between the two styles don't silently break the test.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    target = next(
        (n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == func_name),
        None,
    )
    assert target is not None, f"top-level function {func_name!r} not found in {path}"
    names: set[str] = set()
    for node in ast.walk(target):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                names.add(f"{module}.{alias.name}")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
    return names


def test_llm_model_configurator_is_not_imported_at_module_top():
    """Module-top imports must not include LLMModelConfigurator (it would drag in DSPy)."""
    top = _module_top_imports(_explainer_path())
    bad = {n for n in top if "LLMModelConfigurator" in n}
    assert not bad, (
        f"LLMModelConfigurator imported at module top: {bad}. "
        "It must be lazy-imported inside *build_language_model* — see P0-1."
    )


def test_llm_model_configurator_is_lazy_imported_inside_build_language_model():
    """The lazy import must actually exist where we expect — protects against a regression
    that re-introduces the eager import without a compensating in-function import."""
    inner = _function_local_imports(_explainer_path(), "build_language_model")
    assert any("LLMModelConfigurator" in n for n in inner), (
        f"build_language_model is missing the lazy import of LLMModelConfigurator. " f"Local imports were: {inner}"
    )

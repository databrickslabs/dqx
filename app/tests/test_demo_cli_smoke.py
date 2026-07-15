"""Smoke test: verify seed_demo.py is importable and exposes a callable main()."""


def test_cli_module_imports_and_has_main() -> None:
    import importlib.util
    import pathlib

    p = pathlib.Path(__file__).parents[1] / "scripts" / "seed_demo.py"
    spec = importlib.util.spec_from_file_location("demo_cli", p)
    assert spec is not None and spec.loader is not None, f"Could not build spec for {p}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    assert callable(mod.main), "seed_demo.py must expose a callable main()"

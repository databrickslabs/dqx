from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
from typing import Any

# Mock hatchling before importing the hook
with patch.dict(
    'sys.modules',
    {
        'hatchling': MagicMock(),
        'hatchling.plugin': MagicMock(),
        'hatchling.plugin.interface': MagicMock(),
        'hatchling.builders': MagicMock(),
        'hatchling.builders.hooks': MagicMock(),
        'hatchling.builders.hooks.plugin': MagicMock(),
        'hatchling.builders.hooks.plugin.interface': MagicMock(),
    },
):
    # Mock BuildHookInterface
    class MockBuildHookInterface:
        PLUGIN_NAME = "mock-plugin"

        def __init__(self, root: Any, config: Any) -> None:
            self.root = root
            self.config = config

        def initialize(self, version: str, build_data: dict[str, Any]) -> None:
            pass

        # Set up the mock interfaces properly

    mock_plugin_interface = sys.modules['hatchling.plugin.interface']
    mock_plugin_interface.BuildHookInterface = MockBuildHookInterface  # type: ignore[attr-defined]

    mock_builders_interface = sys.modules['hatchling.builders.hooks.plugin.interface']
    mock_builders_interface.BuildHookInterface = MockBuildHookInterface  # type: ignore[attr-defined]

    # Add the script directory to Python path to import the hook
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / ".github" / "script"))

    # Import with type ignore for mypy since this is a dynamic import
    from hatch_build_hook import ExtractDocResourcesHook  # type: ignore[import-not-found]


def run_hatch_build_hook_simple() -> bool:
    """Simple standalone function to run the hatch_build_hook in the actual repo"""
    try:
        # Get the actual repo root
        repo_root = Path(__file__).parent.parent.parent

        # Create the hook instance
        hook = ExtractDocResourcesHook(None, None)
        hook.root = str(repo_root)

        # Run the hook
        print("🚀 Running hatch build hook...")
        hook.initialize(version="1.0.0", build_data={})

        # Check results
        resources_dir = repo_root / "src" / "databricks" / "labs" / "dqx" / "llm" / "resources"
        if resources_dir.exists():
            print(f"✅ Resources directory created: {resources_dir}")

            combined_file = resources_dir / "quality_checks_all_examples.yml"
            if combined_file.exists():
                print(f"✅ Combined file created: {combined_file}")
                print(f"📊 File size: {combined_file.stat().st_size} bytes")
                return True
            else:
                print("❌ Combined file not found")
                return False
        else:
            print("❌ Resources directory not found")
            return False

    except Exception as e:
        print(f"❌ Error running hatch build hook: {e}")
        return False


if __name__ == "__main__":
    # Run the simple function when script is executed directly
    success = run_hatch_build_hook_simple()
    if success:
        print("🎉 Hatch build hook completed successfully!")
    else:
        print("💥 Hatch build hook failed!")

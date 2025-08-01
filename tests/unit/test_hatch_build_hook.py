import tempfile
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


def test_run_hatch_build_hook() -> bool:
    """Test function to run the hatch_build_hook with mocked environment"""
    # Create a temporary directory to simulate the repo root
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create mock MDX files with YAML content
        docs_dir = temp_path / "docs" / "dqx" / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)

        # Create reference directory and quality_rules.mdx
        reference_dir = docs_dir / "reference"
        reference_dir.mkdir(parents=True, exist_ok=True)

        quality_rules_content = """
# Quality Rules

Here's an example YAML configuration:

```yaml
- check_name: "null_check"
  table_name: "my_table"
  column_name: "id"
  rule_type: "not_null"
```

Another example:

```yml
- check_name: "range_check"
  table_name: "sales"
  column_name: "amount"
  rule_type: "range"
  min_value: 0
```
"""
        (reference_dir / "quality_rules.mdx").write_text(quality_rules_content)

        # Create guide directory and quality_checks.mdx
        guide_dir = docs_dir / "guide"
        guide_dir.mkdir(parents=True, exist_ok=True)

        quality_checks_content = """
# Quality Checks

Example configuration:

```yaml
- name: "data_quality_check"
  type: "completeness"
  threshold: 0.95
```
"""
        (guide_dir / "quality_checks.mdx").write_text(quality_checks_content)

        # Create the hook instance
        hook = ExtractDocResourcesHook(None, None)
        hook.root = str(temp_path)

        # Run the hook
        try:
            hook.initialize(version="1.0.0", build_data={})

            # Check if resources were created
            resources_dir = temp_path / "src" / "databricks" / "labs" / "dqx" / "llm" / "resources"
            assert resources_dir.exists(), "Resources directory should be created"

            init_file = resources_dir / "__init__.py"
            assert init_file.exists(), "__init__.py should be created"

            combined_file = resources_dir / "quality_checks_all_examples.yml"
            assert combined_file.exists(), "Combined YAML file should be created"

            # Check content of combined file
            content = combined_file.read_text()
            assert "null_check" in content, "Should contain content from quality_rules.mdx"
            assert "data_quality_check" in content, "Should contain content from quality_checks.mdx"

            print("✅ Hatch build hook ran successfully!")
            print(f"📁 Resources created at: {resources_dir}")
            print(f"📄 Combined file: {combined_file}")
            print(f"📊 File size: {combined_file.stat().st_size} bytes")

            assert True, "Hatch build hook completed successfully"

        except Exception as e:
            print(f"❌ Error running hatch build hook: {e}")
            return False

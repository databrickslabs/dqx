import os
import subprocess
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface  # type: ignore


class CustomExtractYamlChecksExamplesHook(BuildHookInterface):
    """
    Custom build hook to extract YAML checks examples from MDX files for LLM tasks.

    The hook is executed when running: `hatch build` command.
    """

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        print("🔧 [hatch] Extracting yaml checks examples ...")

        script_path = os.path.join(os.path.dirname(__file__), ".github", "script", "extract_yaml_checks_examples.py")

        try:
            subprocess.run(["python", script_path])

            print("✅ [hatch] YAML Checks Examples extraction completed successfully")
        except subprocess.CalledProcessError as e:
            print(f"❌ [hatch] YAML Checks Examples extraction failed (exit code {e.returncode})")
            raise

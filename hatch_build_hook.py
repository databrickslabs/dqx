import re
import yaml  # type: ignore[import-untyped]
from pathlib import Path
from hatchling.plugin.interface import BuildHookInterface  # type: ignore[import-not-found]


class ExtractResourcesHook(BuildHookInterface):
    PLUGIN_NAME = "extract-resources"

    def initialize(self, version, build_data):
        """Extract resources before build"""
        self.extract_checks_yml()

    def extract_yaml_from_mdx(self, mdx_file_path):
        """Extract all YAML examples from a given MDX file"""

        mdx_file = Path(mdx_file_path)

        if not mdx_file.exists():
            print(f"MDX file not found: {mdx_file}")
            return False, []

        print(f"Reading MDX file: {mdx_file}")
        content = mdx_file.read_text(encoding='utf-8')

        # Extract YAML from code blocks
        yaml_pattern = r'```(?:yaml|yml)\n(.*?)\n```'
        yaml_matches = re.findall(yaml_pattern, content, re.DOTALL)

        print(f"Found {len(yaml_matches)} YAML code blocks in {mdx_file.name}")

        if not yaml_matches:
            print(f"No YAML code blocks found in {mdx_file.name}")
            return False, []

        # Combine all YAML blocks
        all_yaml_content = []

        for i, yaml_content in enumerate(yaml_matches):
            print(
                f"Processing YAML block {i+1}/{len(yaml_matches)} from {mdx_file.name} (length: {len(yaml_content)} characters)"
            )

            # Validate each YAML block
            try:
                parsed_yaml = yaml.safe_load(yaml_content)
                if not parsed_yaml:  # Skip empty YAML blocks
                    print(f"  - Skipped empty YAML block {i+1}")
                    continue

                if isinstance(parsed_yaml, list):
                    all_yaml_content.extend(parsed_yaml)
                    print(f"  - Added {len(parsed_yaml)} items from YAML block {i+1}")
                else:
                    all_yaml_content.append(parsed_yaml)
                    print(f"  - Added 1 item from YAML block {i+1}")
            except yaml.YAMLError as e:
                print(f"  - Invalid YAML in block {i+1}: {e}")
                continue

        return len(all_yaml_content) > 0, all_yaml_content

    def extract_checks_yml(self):
        """Extract all YAML examples from both quality_rules.mdx and quality_checks.mdx"""

        # Setup paths
        repo_root = Path(self.root)
        resources_dir = repo_root / "src" / "databricks" / "labs" / "dqx" / "resources"

        # Create resources directory
        resources_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created resources directory: {resources_dir}")

        # Create __init__.py
        init_file = resources_dir / "__init__.py"
        init_file.write_text("# Resources package\n")
        print(f"Created __init__.py: {init_file}")

        # Define MDX files to extract from
        mdx_files = [
            {
                "path": repo_root / "docs" / "dqx" / "docs" / "reference" / "quality_rules.mdx",
                "output": "quality_rules_examples.yml",
                "description": "quality rules reference examples",
            },
            {
                "path": repo_root / "docs" / "dqx" / "docs" / "guide" / "quality_checks.mdx",
                "output": "quality_checks_examples.yml",
                "description": "quality checks guide examples",
            },
        ]

        all_combined_content = []
        success_count = 0

        for mdx_info in mdx_files:
            print(f"\n--- Processing {mdx_info['description']} ---")
            success, yaml_content = self.extract_yaml_from_mdx(mdx_info["path"])

            if success and yaml_content:
                # Save individual file
                # output_file = resources_dir / mdx_info["output"]
                # individual_yaml = yaml.dump(yaml_content, default_flow_style=False, sort_keys=False)
                # output_file.write_text(individual_yaml)
                # print(f"Extracted {len(yaml_content)} YAML items to: {output_file}")

                # Add to combined content
                all_combined_content.extend(yaml_content)
                success_count += 1

            else:
                print(f"No YAML content extracted from {mdx_info['path']}")

        # Create combined file
        if all_combined_content:
            print("\n--- Creating combined file ---")
            combined_output = resources_dir / "quality_checks_all_examples.yml"
            combined_yaml = yaml.dump(all_combined_content, default_flow_style=False, sort_keys=False)
            combined_output.write_text(combined_yaml)
            print(f"Created combined file with {len(all_combined_content)} total YAML items: {combined_output}")
            print(f"Combined file size: {combined_output.stat().st_size} bytes")

        if success_count > 0:
            print("\n YAML extraction completed successfully!")
        else:
            print("\n YAML extraction failed!")

---
sidebar_label: extract_yaml_checks_examples
title: databricks.labs.dqx.llm.extract_yaml_checks_examples
---

#### extract\_yaml\_checks\_from\_content

```python
def extract_yaml_checks_from_content(content: str,
                                     source_name: str = "content"
                                     ) -> list[dict[str, Any]]
```

Extract all YAML examples from MDX content string.

**Arguments**:

- `content`: The MDX content string to extract YAML from
- `source_name`: Name of the source for logging purposes (default: &quot;content&quot;)

**Returns**:

List of parsed YAML objects from all valid blocks

#### extract\_yaml\_checks\_from\_mdx

```python
def extract_yaml_checks_from_mdx(mdx_file_path: str) -> list[dict[str, Any]]
```

Extract all YAML examples from a given MDX file.

**Arguments**:

- `mdx_file_path`: Path to the MDX file to extract YAML from

**Raises**:

- `FileNotFoundError`: If the MDX file does not exist

**Returns**:

List of parsed YAML objects from all valid blocks

#### extract\_yaml\_checks\_examples

```python
def extract_yaml_checks_examples(output_file_path: Path | None = None) -> bool
```

Extract all YAML examples from both quality_rules.mdx and quality_checks.mdx.

Creates a combined YAML file with all examples from the documentation files
in the LLM resources directory for use in language model processing.

**Returns**:

True if extraction was successful, False otherwise


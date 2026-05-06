# databricks.labs.dqx.installer.version\_checker

## VersionChecker Objects[​](#versionchecker-objects "Direct link to VersionChecker Objects")

```python
class VersionChecker()

```

Encapsulates version detection and comparison logic for DQX installations.

### extract\_major\_minor[​](#extract_major_minor "Direct link to extract_major_minor")

```python
@staticmethod
def extract_major_minor(version_string: str)

```

Extracts the major and minor version from a version string.

**Arguments**:

* `version_string` - The version string to extract from.

**Returns**:

The major.minor version as a string, or None if not found.

### compare\_and\_prompt\_upgrade[​](#compare_and_prompt_upgrade "Direct link to compare_and_prompt_upgrade")

```python
def compare_and_prompt_upgrade() -> None

```

Compares released and installed versions and optionally asks user to update.

Behavior matches previous inline implementation in `WorkspaceInstaller`.

import os
import pytest

from databricks.labs.pytester.fixtures.baseline import factory
from databricks.sdk.service.workspace import ImportFormat


@pytest.fixture
def checks_yaml_content():
    return """- criticality: error
  check:
    function: is_not_null
    for_each_column:
      - col1
      - col2
    arguments: {}
- name: col_col3_is_null_or_empty
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col3
      trim_strings: true
- criticality: warn
  check:
    function: is_in_list
    arguments:
      column: col4
      allowed:
      - 1
      - 2
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: col1 not like "Team %"
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: col2 not like 'Team %'
- name: check_with_user_metadata
  criticality: error
  check:
    function: is_not_null
    arguments:
      column: col1
  user_metadata:
    check_type: completeness
    check_owner: "someone@email.com"
    """


@pytest.fixture
def checks_json_content():
    return """[
    {
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "for_each_column": ["col1", "col2"],
            "arguments": {}
        }
    },
    {
        "name": "col_col3_is_null_or_empty",
        "criticality": "error",
        "check": {
            "function": "is_not_null_and_not_empty",
            "arguments": {
                "column": "col3",
                "trim_strings": true
            }
        }
    },
    {
        "criticality": "warn",
        "check": {
            "function": "is_in_list",
            "arguments": {
                "column": "col4",
                "allowed": [1, 2]
            }
        }
    },
    {
        "criticality": "error",
        "check": {"function": "sql_expression", "arguments": {"expression": "col1 not like \\"Team %\\""}}
    },
    {
        "criticality": "error",
        "check": {"function": "sql_expression", "arguments": {"expression": "col2 not like 'Team %'"}}
    },
    {
        "name": "check_with_user_metadata", "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"}
    }
]
    """


@pytest.fixture
def checks_yaml_invalid_content():
    """This YAML has wrong indentation for the function field."""
    return """- criticality: error
  check:
function: is_not_null_and_not_empty
    for_each_column:
    col1
    - col2
    arguments: {}
    """


@pytest.fixture
def checks_json_invalid_content():
    """This JSON is missing a comma after criticality field."""
    return """[
    {
        "criticality": "error"
        "function": "is_not_null_and_not_empty",
        "for_each_column": ["col1", "col2"],
        "check": {
            "arguments": {}
        }
    }
]
    """


@pytest.fixture
def expected_checks():
    return [
        {
            "criticality": "error",
            "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
        },
        {
            "name": "col_col3_is_null_or_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "col3", "trim_strings": True}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "col4", "allowed": [1, 2]}},
        },
        {
            "criticality": "error",
            "check": {"function": "sql_expression", "arguments": {"expression": 'col1 not like "Team %"'}},
        },
        {
            "criticality": "error",
            "check": {"function": "sql_expression", "arguments": {"expression": "col2 not like 'Team %'"}},
        },
        {
            "name": "check_with_user_metadata", "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
            "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"}
        }
    ]


@pytest.fixture
def make_local_check_file_as_yaml(checks_yaml_content):
    file_path = "checks.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_yaml_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_local_check_file_as_json(checks_json_content):
    file_path = "checks.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_json_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_invalid_local_check_file_as_yaml(checks_yaml_invalid_content):
    file_path = "invalid_checks.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_yaml_invalid_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_invalid_local_check_file_as_json(checks_json_invalid_content):
    file_path = "invalid_checks.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_json_invalid_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_check_file_as_yaml(ws, make_directory, checks_yaml_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            workspace_file_path = kwargs["install_dir"] + "/checks.yml"
        else:
            folder = make_directory()
            workspace_file_path = str(folder.absolute()) + "/checks.yml"

        ws.workspace.upload(
            path=workspace_file_path, format=ImportFormat.AUTO, content=checks_yaml_content.encode(), overwrite=True
        )

        return workspace_file_path

    def delete(workspace_file_path: str) -> None:
        ws.workspace.delete(workspace_file_path)

    yield from factory("file", create, delete)


@pytest.fixture
def make_check_file_as_json(ws, make_directory, checks_json_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            workspace_file_path = kwargs["install_dir"] + "/checks.json"
        else:
            folder = make_directory()
            workspace_file_path = str(folder.absolute()) + "/checks.json"

        ws.workspace.upload(
            path=workspace_file_path, format=ImportFormat.AUTO, content=checks_json_content.encode(), overwrite=True
        )

        return workspace_file_path

    def delete(workspace_file_path: str) -> None:
        ws.workspace.delete(workspace_file_path)

    yield from factory("file", create, delete)


@pytest.fixture
def make_invalid_check_file_as_yaml(ws, make_directory, checks_yaml_invalid_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            workspace_file_path = kwargs["install_dir"] + "/checks.yml"
        else:
            folder = make_directory()
            workspace_file_path = str(folder.absolute()) + "/checks.yml"

        ws.workspace.upload(
            path=workspace_file_path,
            format=ImportFormat.AUTO,
            content=checks_yaml_invalid_content.encode(),
            overwrite=True,
        )

        return workspace_file_path

    def delete(workspace_file_path: str) -> None:
        ws.workspace.delete(workspace_file_path)

    yield from factory("file", create, delete)


@pytest.fixture
def make_invalid_check_file_as_json(ws, make_directory, checks_json_invalid_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            workspace_file_path = kwargs["install_dir"] + "/checks.json"
        else:
            folder = make_directory()
            workspace_file_path = str(folder.absolute()) + "/checks.json"

        ws.workspace.upload(
            path=workspace_file_path,
            format=ImportFormat.AUTO,
            content=checks_json_invalid_content.encode(),
            overwrite=True,
        )

        return workspace_file_path

    def delete(workspace_file_path: str) -> None:
        ws.workspace.delete(workspace_file_path)

    yield from factory("file", create, delete)

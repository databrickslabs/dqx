---
sidebar_label: dashboard_installer
title: databricks.labs.dqx.installer.dashboard_installer
---

## DashboardMetadata Objects

```python
class DashboardMetadata()
```

Creates Dashboard Metadata from exported Lakeview dashboard (.lvdash.json)

#### from\_path

```python
@classmethod
def from_path(cls, file: Path) -> "DashboardMetadata"
```

Load dashboard metadata from exported Lakeview dashboard file.

Expected structure:
dashboard_folder/
└── dashboard_name.lvdash.json

**Arguments**:

- `file` - The path to the .lvdash.json file.
  

**Returns**:

  A DashboardMetadata instance populated with the display name and
  dashboard definition from the exported Lakeview dashboard.

## DashboardInstaller Objects

```python
class DashboardInstaller()
```

Creates or updates Lakeview dashboards from exported .lvdash.json files.

#### get\_create\_dashboard\_tasks

```python
def get_create_dashboard_tasks() -> Iterable[Callable[[], None]]
```

Returns a generator of tasks to create dashboards from exported Lakeview dashboards.

Each task is a callable that, when executed, will create a dashboard in the workspace.
The tasks are created based on the .lvdash.json files found in the dashboard directory.


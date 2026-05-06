# databricks.labs.dqx.installer.dashboard\_installer

## DashboardMetadata Objects[​](#dashboardmetadata-objects "Direct link to DashboardMetadata Objects")

```python
class DashboardMetadata()

```

Creates Dashboard Metadata from exported Lakeview dashboard (.lvdash.json)

### from\_path[​](#from_path "Direct link to from_path")

```python
@classmethod
def from_path(cls, file: Path) -> "DashboardMetadata"

```

Load dashboard metadata from exported Lakeview dashboard file.

Expected structure: dashboard\_folder/ └── dashboard\_name.lvdash.json

**Arguments**:

* `file` - The path to the .lvdash.json file.

**Returns**:

A DashboardMetadata instance populated with the display name and dashboard definition from the exported Lakeview dashboard.

## DashboardInstaller Objects[​](#dashboardinstaller-objects "Direct link to DashboardInstaller Objects")

```python
class DashboardInstaller()

```

Creates or updates Lakeview dashboards from exported .lvdash.json files.

### get\_create\_dashboard\_tasks[​](#get_create_dashboard_tasks "Direct link to get_create_dashboard_tasks")

```python
def get_create_dashboard_tasks() -> Iterable[Callable[[], None]]

```

Returns a generator of tasks to create dashboards from exported Lakeview dashboards.

Each task is a callable that, when executed, will create a dashboard in the workspace. The tasks are created based on the .lvdash.json files found in the dashboard directory.

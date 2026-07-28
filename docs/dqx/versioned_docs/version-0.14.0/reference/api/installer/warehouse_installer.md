---
sidebar_label: warehouse_installer
title: databricks.labs.dqx.installer.warehouse_installer
---

## WarehouseInstaller Objects

```python
class WarehouseInstaller()
```

Configures or selects a SQL warehouse used by the dashboards.
Encapsulates all interactions with the Databricks SQL Warehouses API.

#### create

```python
def create() -> str
```

Select an existing PRO or SERVERLESS warehouse or create a new PRO warehouse.


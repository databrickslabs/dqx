---
sidebar_label: config_loader
title: databricks.labs.dqx.config_loader
---

## RunConfigLoader Objects

```python
class RunConfigLoader()
```

Class to handle loading of run configurations from the installation.

#### load\_run\_config

```python
def load_run_config(run_config_name: str | None,
                    assume_user: bool = True,
                    product_name: str = "dqx") -> RunConfig
```

Load run configuration from the installation.

**Arguments**:

- `run_config_name`: name of the run configuration to use
- `assume_user`: if True, assume user installation
- `product_name`: name of the product

#### get\_installation

```python
def get_installation(assume_user: bool, product_name: str) -> Installation
```

Get the installation for the given product name.

**Arguments**:

- `assume_user`: if True, assume user installation
- `product_name`: name of the product


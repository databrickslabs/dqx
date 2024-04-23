# Contributing

## First Principles

Favoring standard libraries over external dependencies, especially in specific contexts like Databricks, is a best practice in software 
development. 

There are several reasons why this approach is encouraged:
- Standard libraries are typically well-vetted, thoroughly tested, and maintained by the official maintainers of the programming language or platform. This ensures a higher level of stability and reliability. 
- External dependencies, especially lesser-known or unmaintained ones, can introduce bugs, security vulnerabilities, or compatibility issues  that can be challenging to resolve. Adding external dependencies increases the complexity of your codebase. 
- Each dependency may have its own set of dependencies, potentially leading to a complex web of dependencies that can be difficult to manage. This complexity can lead to maintenance challenges, increased risk, and longer build times. 
- External dependencies can pose security risks. If a library or package has known security vulnerabilities and is widely used, it becomes an attractive target for attackers. Minimizing external dependencies reduces the potential attack surface and makes it easier to keep your code secure. 
- Relying on standard libraries enhances code portability. It ensures your code can run on different platforms and environments without being tightly coupled to specific external dependencies. This is particularly important in settings like Databricks, where you may need to run your code on different clusters or setups. 
- External dependencies may have their versioning schemes and compatibility issues. When using standard libraries, you have more control over versioning and can avoid conflicts between different dependencies in your project. 
- Fewer external dependencies mean faster build and deployment times. Downloading, installing, and managing external packages can slow down these processes, especially in large-scale projects or distributed computing environments like Databricks. 
- External dependencies can be abandoned or go unmaintained over time. This can lead to situations where your project relies on outdated or unsupported code. When you depend on standard libraries, you have confidence that the core functionality you rely on will continue to be maintained and improved. 

While minimizing external dependencies is essential, exceptions can be made case-by-case. There are situations where external dependencies are 
justified, such as when a well-established and actively maintained library provides significant benefits, like time savings, performance improvements, 
or specialized functionality unavailable in standard libraries.

## Common fixes for `mypy` errors

See https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html for more details

### ..., expression has type "None", variable has type "str"

* Add `assert ... is not None` if it's a body of a method. Example:

```
# error: Argument 1 to "delete" of "DashboardWidgetsAPI" has incompatible type "str | None"; expected "str"
self._ws.dashboard_widgets.delete(widget.id)
```

after

```
assert widget.id is not None
self._ws.dashboard_widgets.delete(widget.id)
```

* Add `... | None` if it's in the dataclass. Example: `cloud: str = None` -> `cloud: str | None = None`

### ..., has incompatible type "Path"; expected "str"

Add `.as_posix()` to convert Path to str

###  Argument 2 to "get" of "dict" has incompatible type "None"; expected ...

Add a valid default value for the dictionary return. 

Example: 
```python
def viz_type(self) -> str:
    return self.viz.get("type", None)
```

after:

Example: 
```python
def viz_type(self) -> str:
    return self.viz.get("type", "UNKNOWN")
```

## Local Setup

This section provides a step-by-step guide to set up and start working on the project. These steps will help you set up your project environment and dependencies for efficient development.

To begin, run `make dev` create the default environment and install development dependencies, assuming you've already cloned the github repo.

```shell
make dev
```

Verify installation with 
```shell
make test
```

Before every commit, apply the consistent formatting of the code, as we want our codebase look consistent:
```shell
make fmt
```

Before every commit, run automated bug detector (`make lint`) and unit tests (`make test`) to ensure that automated
pull request checks do pass, before your code is reviewed by others: 
```shell
make test
```

## First contribution

Here are the example steps to submit your first contribution:

1. Make a Fork from ucx repo (if you really want to contribute)
2. `git clone`
3. `git checkout main` (or `gcm` if you're using [ohmyzsh](https://ohmyz.sh/)).
4. `git pull` (or `gl` if you're using [ohmyzsh](https://ohmyz.sh/)).
5. `git checkout -b FEATURENAME` (or `gcb FEATURENAME` if you're using [ohmyzsh](https://ohmyz.sh/)).
6. .. do the work
7. `make fmt`
8. `make lint`
9. .. fix if any
10. `make test`
11. .. fix if any
12. `git commit -a`. Make sure to enter meaningful commit message title.
13. `git push origin FEATURENAME`
14. Go to GitHub UI and create PR. Alternatively, `gh pr create` (if you have [GitHub CLI](https://cli.github.com/) installed). 
    Use a meaningful pull request title because it'll appear in the release notes. Use `Resolves #NUMBER` in pull
    request description to [automatically link it](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/using-keywords-in-issues-and-pull-requests#linking-a-pull-request-to-an-issue)
    to an existing issue. 
15. announce PR for the review

## Troubleshooting

If you encounter any package dependency errors after `git pull`, run `make clean`

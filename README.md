DQX by Databricks Labs
===

<p align="center">
    <a href="https://github.com/databrickslabs/dqx">
        <img src="docs/dqx/static/img/logo.svg" class="align-center" width="200" height="200" alt="logo" />
    </a>
</p>

Simplified Data Quality checking at Scale for PySpark Workloads on streaming and standard DataFrames.

Key capabilities:

- **Rule-based quality checks** — 80+ built-in checks (null, range, regex, referential, aggregate, geo, PII, and more) for row-level and column/dataset-level validation, with support for custom check functions.
- **Code or config checks** — define checks programmatically as code or declaratively as YAML/JSON configuration.
- **Check levels** — mark failed checks as warning or error.
- **Custom reactions to failed checks** — drop, mark, or quarantine invalid data flexibly.
- **Detailed failure info** — get detailed insights into why each check failed.
- **AI-assisted rule generation** — LLM-driven rule suggestions from business descriptions, powered by DSPy and Databricks Model Serving.
- **Data profiling & rule generation** — automatic statistics collection and quality rule candidate generation from existing data.
- **ML row anomaly detection** — Isolation Forest–based row anomaly detection with SHAP explanations and AI-generated narratives.
- **Data contracts** — generate quality rules from ODCS data contracts, including schema validation.
- **Summary metrics & quality dashboard** — built-in and custom aggregate metrics (input/error/warning/valid row counts, per-check breakdowns) persisted to Delta tables, with a Lakeview quality dashboard for tracking and identifying data quality issues.
- **Actions and alerting** — automatically send Slack, Microsoft Teams, or generic webhook alerts and/or fail the pipeline when summary metrics cross a threshold.
- **Flexible checks storage** — save and load quality rules from YAML/JSON files, Unity Catalog tables, Volumes, or Lakebase (PostgreSQL).
- **DQX Studio** — browser-based no-code UI for authoring, reviewing, running, and monitoring quality rules, deployed as a Databricks App.
- **Data format agnostic & streaming support** — works with PySpark DataFrames and applies checks to both batch and Spark Structured Streaming (including Lakeflow Pipelines / DLT) using the same API.

[![build](https://github.com/databrickslabs/dqx/actions/workflows/push.yml/badge.svg)](https://github.com/databrickslabs/dqx/actions/workflows/push.yml) 
[![codecov](https://codecov.io/github/databrickslabs/dqx/graph/badge.svg)](https://codecov.io/github/databrickslabs/dqx) 
![linesofcode](https://aschey.tech/tokei/github/databrickslabs/dqx?category=code)
[![PyPI](https://img.shields.io/pypi/v/databricks-labs-dqx?label=pypi%20package&cacheSeconds=3600)](https://pypi.org/project/databricks-labs-dqx/) 
![PyPI Downloads](https://static.pepy.tech/personalized-badge/databricks-labs-dqx?period=month&units=international_system&left_color=grey&right_color=orange&left_text=PyPI%20downloads&cacheSeconds=3600)

# 📖 Documentation

The complete documentation is available at: [https://databrickslabs.github.io/dqx/](https://databrickslabs.github.io/dqx/)

# 🛠️ Contribution

Please see the contribution guidance [here](https://databrickslabs.github.io/dqx/docs/dev/contributing/) on how to contribute to the project (build, test, and submit a PR).

# 💬 Project Support

Please note that this project is provided for your exploration only and is not 
formally supported by Databricks with Service Level Agreements (SLAs). They are 
provided AS-IS, and we do not make any guarantees. Please do not 
submit a support ticket relating to any issues arising from the use of this project.

Any issues discovered through the use of this project should be filed as GitHub 
[Issues on this repository](https://github.com/databrickslabs/dqx/issues). 
They will be reviewed as time permits, but no formal SLAs for support exist.

# Security Policy

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues, discussions, or pull requests.**

If you believe you've found a security vulnerability in DQX (the library, the CLI, or the DQX Studio web application), report it privately through one of the following channels:

1. **GitHub Security Advisories (preferred)** — open a private advisory on the [DQX repository](https://github.com/databrickslabs/dqx/security/advisories/new). This keeps the report confidential until a fix is released.
2. **Email** — if you cannot use GitHub Security Advisories, contact the Databricks Labs maintainers at [labs-oss@databricks.com](mailto:labs-oss@databricks.com) with `DQX SECURITY` in the subject line.

### What to include

To help us triage quickly, please include as much of the following as you can:

- A description of the issue and its potential impact.
- Steps to reproduce (proof-of-concept code, sample input, or a minimal repro repo).
- The affected version(s) of DQX and, if relevant, the Databricks Runtime version.
- Any mitigations or workarounds you have identified.

We will acknowledge receipt within **5 business days** and provide a more detailed response within **10 business days** indicating our next steps. We aim to patch confirmed vulnerabilities in a timely manner and will coordinate disclosure with the reporter.

## Supported Versions

Security fixes are only guaranteed for the **latest minor release** on the [`main` branch](https://github.com/databrickslabs/dqx) and published on [PyPI](https://pypi.org/project/databricks-labs-dqx/). Please upgrade to the latest release before reporting an issue when possible.

## Scope

The following are in scope:

- The `databricks-labs-dqx` Python library and CLI.
- The DQX Studio web application under `app/` (FastAPI backend + React frontend).
- The task-runner Databricks Job under `app/tasks/`.
- Example code in `demos/` (informational only — reports welcome but low priority).

Out of scope:

- Vulnerabilities in third-party dependencies that are better reported upstream.
- Issues requiring a privileged user (e.g., workspace admin) to already be compromised.
- Denial-of-service resulting from reasonable use of Databricks platform limits.

## Disclosure Policy

- We follow a **coordinated disclosure** process and will credit reporters in release notes unless they prefer anonymity.
- We request that reporters give us a reasonable window (typically 90 days) to issue a fix before any public disclosure.

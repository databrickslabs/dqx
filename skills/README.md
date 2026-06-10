# DQX AI Assistant Skills

DQX provides agent skills that can be used with [Databricks Genie Code](https://docs.databricks.com/aws/en/genie-code/skills) or any tool that follows the [Agent Skills](https://agentskills.io/) open standard. Skills teach AI assistants how to use DQX.

## Skills

Each skill is a folder with a `SKILL.md` file that documents usage patterns. The following skills are available:

| Skill                                                                 | What it covers                                                                                                          |
|-----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| [`dqx-define-checks`](./dqx-define-checks/SKILL.md)                   | Creating quality rules (`DQRowRule`, `DQDatasetRule`, `DQForEachColRule`) and the equivalent YAML / dict metadata form. |
| [`dqx-apply-checks`](./dqx-apply-checks/SKILL.md)                     | Validating data with a set of rules (checks loadable from YAML / JSON / Delta).                                         |
| [`dqx-end-to-end`](./dqx-end-to-end/SKILL.md)                         | Running check → save cycles in one call with `apply_checks_and_save_in_table`.                                          |
| [`dqx-profile-and-generate`](./dqx-profile-and-generate/SKILL.md)     | Profiling a dataset and generating rule candidates with `DQProfiler` / `DQGenerator`.                                   |
| [`dqx-storage`](./dqx-storage/SKILL.md)                               | Loading and saving checks across file / workspace / volume / table / installation / Lakebase backends.                  |

## Install

See the [installation and usage guide](https://databrickslabs.github.io/dqx/docs/guide/ai_tools_skills) for Databricks Genie Code and other AI tools.

## Scope and guardrails

DQX's agent skills are scoped to DQX's public APIs. See [Extending DQX skills](https://databrickslabs.github.io/dqx/docs/guide/ai_tools_skills#extending-dqx-skills) on the docs site for the full list of design guidelines.

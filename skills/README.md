# DQX AI Assistant Skills

Agent Skills — compatible with [Databricks Genie Code](https://docs.databricks.com/aws/en/genie-code/skills), [Claude Code](https://docs.claude.com/en/docs/claude-code/plugins), and any tool that follows the [Agent Skills](https://agentskills.io/) open standard — that teach an AI assistant how to use DQX correctly.

## Skills

| Skill | What it covers |
|---|---|
| [`dqx-define-checks`](./dqx-define-checks/SKILL.md) | Creating quality rules (`DQRowRule`, `DQDatasetRule`, `DQForEachColRule`) and the equivalent YAML / dict metadata form. |
| [`dqx-apply-checks`](./dqx-apply-checks/SKILL.md) | Validating data with a set of rules via `DQEngine.apply_checks*` methods. |
| [`dqx-end-to-end`](./dqx-end-to-end/SKILL.md) | Running check → save cycles in one call with `apply_checks_and_save_in_table`. |
| [`dqx-profile-and-generate`](./dqx-profile-and-generate/SKILL.md) | Profiling a dataset and generating rule candidates with `DQProfiler` / `DQGenerator`. |
| [`dqx-storage`](./dqx-storage/SKILL.md) | Loading and saving checks across file / workspace / volume / table / installation / Lakebase backends. |

Each skill is a folder with a `SKILL.md`. That's the standard layout that Databricks Genie Code and Claude Code both accept.

## Install

See the full installation and usage guide for **Databricks Genie Code**, **Claude Code**, and other AI dev tools:

**[AI Tools & Skills](https://databrickslabs.github.io/dqx/docs/dev/ai_tools_skills)** (or `docs/dqx/docs/dev/ai_tools_skills.mdx` in this repo)

## Scope and guardrails

These skills are scoped to **the public DQX API** as documented at [databrickslabs.github.io/dqx](https://databrickslabs.github.io/dqx/docs/guide/). They intentionally:

- Prefer the declarative YAML / dict form for portability when the user is defining many checks, and the DQX class form when the user is editing one or two rules interactively.
- Always import from `databricks.labs.dqx.*` (never guess module paths).
- Point at the canonical doc section for any topic outside the skill's core responsibility rather than duplicating content.

If you extend these skills, keep them short — AI tools load the full `SKILL.md` into context every time, so every line costs tokens on every invocation.

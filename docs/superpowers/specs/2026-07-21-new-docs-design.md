# New DQX Docs — Landing Zone + Core & Studio Sections

**Branch:** `dqx/new-docs` (local only — no merge, no deploy, no push)
**Date:** 2026-07-21
**Docs stack:** Docusaurus under `docs/dqx/`

## Goal

Split the documentation into two clear product tracks — **DQX Core** (the Python
package) and **DQX Studio** (the no-code web app) — behind a rebuilt landing
zone. Replace the single, outdated `guide/dqx_studio.mdx` page with a multi-page,
task-oriented Studio guide aimed at **non-technical Data Stewards and Data
Governance Managers**, illustrated with real screenshots from the live app.

This is **user-facing** documentation. It must not expose anything internal to
the app's implementation (no service principals, backend routes, synthetic FQNs,
"bindings", or other engineering vocabulary). Everything is described in the
language a steward or governance manager uses.

## Approved decisions

- **Nav:** Studio-first landing zone routing into two sections — **DQX Studio**
  and **DQX Core** — with shared Getting Started / Reference / Development /
  Demos retained.
- **Granularity:** ~18 Studio pages (middle ground) — split the high-value
  differentiators and security, merge the smaller governance topics.
- **Landing zone:** full rebuild of `src/pages/index.tsx` with a Marketplace
  "coming soon" teaser.
- **Core:** re-home + rebrand only (content unchanged).
- **Screenshots:** captured from the **live fe-sandbox app** (user authenticates
  the browser; agent drives via Chrome DevTools/Playwright MCP), **both light +
  dark** via the existing `ThemedImage` pattern.

## Navigation structure (`docusaurus.config.ts` sections)

```
Home (rebuilt landing zone)
├─ DQX Studio        NEW multi-page, no-code (primary focus)
├─ DQX Core          existing guide pages, re-homed + rebranded
├─ Getting Started   installation + motivation (shared, unchanged)
├─ Reference         API/CLI/schemas (shared, unchanged)
├─ Development        contributing (shared, unchanged)
└─ Demos             unchanged
```

- **DQX Core** = today's `docs/guide/**` code-level pages, moved under a `core/`
  route and relabelled away from "User Guide". Content unchanged.
- **DQX Studio** = new page set under `docs/studio/**`.
- Old `guide/dqx_studio.mdx` is **deleted**.

## Landing zone (`src/pages/index.tsx`, full rebuild)

- **Hero** — DQX one-liner + two primary CTAs: **DQX Studio (No-Code)** and
  **DQX Core (Python)** so the two audiences self-select.
- **"What do you want to do today?"** launcher — task tiles (Check my data's
  health · Add a rule · Automate quality checks · Govern a rollout) linking into
  Studio how-tos.
- **Key features grid** — brief explainers *with screenshots*: AI-assisted
  generation, profiling, monitored tables + thresholds, approvals/four-eyes,
  results dashboard, data contracts.
- **Marketplace — Coming soon** — a distinct teaser band, clearly upcoming.
- Retain existing capability content lower down for the Core story.

## DQX Studio pages (~18)

### A. Start here
1. **What is DQX Studio?** — plain-language value, who it's for, the two
   doorways ("check my data's health" / "add a rule").
2. **Quickstart: your first monitored table in 10 minutes** — one linear happy
   path that *threads the differentiators*: generate a rule (AI or profiling) →
   register a table → set a pass threshold → run → view results.
3. **Set up DQX Studio for your organization** *(admin / governance on-ramp)* —
   roles→groups, four-eyes on/off, labels, governed tags, thresholds, retention,
   AI endpoint.
4. **Key concepts** — **rules vs checks vs tests**; error vs warning; the
   **hierarchy** (see below); roles & the orthogonal Runner. Plain-language
   definitions inline; diagram of the hierarchy.

### B. Authoring rules
5. **Create a rule (no-code)** — *the most important authoring page; extra care.*
   The form builder happy path, the three authoring surfaces explained in user
   terms (form vs SQL, and which is "for me"), arguments, severity, labels, and
   **dry-run before saving**. Steward-first: lead with the simplest real task
   ("this column must not be blank").
6. **Generate rules with AI** — natural-language description workflow.
7. **Generate rules by profiling** — scan-a-table → candidate rules workflow.
8. **Import rules (YAML & data contracts)** — YAML import + a dedicated ODCS
   data-contract section + bulk import.
9. **Checks catalog** *(reference)* — grouped by intent ("check for blanks",
   "value in an allowed list", "format like email/phone"), plain description
   first, function name second; covers schema validation & foreign-key checks.

### C. Monitor your tables
10. **Monitor a table** — register a table + apply rules + severity overrides
    (+ bulk register). A monitored table is the **single source of truth** for
    that table's quality rules.
11. **Set quality thresholds** — the "95% must pass / gradual improvement"
    story, per-rule/per-column overrides, results indicators.
12. **Governed tags & auto-mapping** — tag a rule (e.g. `email`) and the app
    suggests which columns it applies to; matched-tag chips.
13. **Collections (data products)** — bundle multiple **related** tables into a
    data product you can view, run, and schedule **together** — e.g. the set of
    tables feeding a Genie space or a published data product. Emphasize the
    cross-table "look at related tables at once" value, *not just* scheduling.
    Note it's optional until you have related tables to group.

### D. Run & review
14. **Run checks** — manual + scheduled (cadence, pause/resume/delete).
15. **A rule failed — what now?** — the steward triage page: read the ratio
    bars, open the sample failing rows, and *your options* (fix the data, adjust
    the threshold, mute, or escalate to the data owner).
16. **Measure & report quality health** — the Results dashboard, org-wide score
    & trends, **Ask-Genie**, framed for leadership reporting.

### E. Governance & security
17. **Roles & permissions** — "find your role" first; who-can-do-what matrix;
    orthogonal Runner called out.
18. **Approval workflow (four-eyes)** — draft → submit → review → approve, diff
    & comments, a lifecycle diagram.
19. **Data access & security (on-behalf-of)** — what you can see, what identity
    runs checks, how it maps to Unity Catalog permissions.
20. **Governance controls (audit trail & retention)** — what's logged and for
    how long, how to export for an auditor; runs vs PII quarantine retention and
    who can view sampled rows. *(Merged per "middle ground".)*

### F. Help
21. **Troubleshooting & FAQ** — seeded with real steward/governance
    panic-questions ("Why isn't my table listed?", "Why can't I approve my own
    rule?", "Does failing data get deleted?" — no).
22. **Glossary** — one-line definitions of every term.

> ~22 files listed; the "middle ground" merges (governance pages, some Start-here
> overlap) land the *distinct authored pages* at ~18. Final count may shift ±1 as
> pages are drafted; each stays short, single-job, and cross-linked.

## The hierarchy explainer (must appear in Key concepts + diagram)

1. **Rule Registry** — the library of reusable rule definitions (author once).
2. **Monitored table** — a Unity Catalog table with rules applied to it; the
   **single source of truth** for that table's quality.
3. **Collection (data product)** — a group of related monitored tables looked
   at, run, and scheduled **together** (e.g. tables feeding one Genie space or
   data product). Enables cross-table quality views, not merely scheduling.

## Screenshots

- Source: **live fe-sandbox deployed app**. User authenticates the browser;
  agent drives via Chrome DevTools/Playwright MCP.
- **Both light + dark**, wired through `ThemedImage` + `useBaseUrl`.
- Stored under `static/img/studio/` as `<name>_light.png` / `<name>_dark.png`.
- Tight crops per step; larger hero shots for the landing zone.

## Writing principles (Diátaxis + persona feedback)

- One page = one job; task-titled, not feature-titled.
- Tutorials (Quickstart) are linear and guaranteed; how-tos may branch; concepts
  live in their own pages and are linked, never inlined.
- Define jargon in plain language the first time it appears; glossary as backup.
- **No internal/developer vocabulary** — user-facing only.
- Steward-first ordering: "what did I come to do?" before architecture; two
  doorways (health / add a rule) surfaced early.
- Reassure: Collections are optional until you have related tables to group.

## Out of scope / constraints

- No merge, no deploy, no push. Preview on **localhost** only.
- DQX Core content is not rewritten — only re-homed and rebranded.

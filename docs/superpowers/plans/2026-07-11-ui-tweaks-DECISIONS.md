# UI Tweaks — Resolved decisions (authoritative input to the plan)

Answers from the user, 2026-07-11. These override anything ambiguous in RAW/QUESTIONS.

## Gating decisions
1. **Backend scope**: Backend changes ALLOWED, including schema migrations + `make app-regen-api`. Flag each migration in the plan.
2. **AI suggester (#89)**: Scope to the "Build with AI" GENERATOR (not the mapping suggester). Add **low-code output** — study dqlake's low-code generator (`databricks-dqwatch/src/dqlake/...`) for inspiration on output shape. **PRESERVE the existing field-fill instructions** (name, description, dqx-native ability, dimension, severity, polarity) — consolidate them into ONE master prompt. Preference order low-code → dqx-native → raw sql. Plus UI polish #45/#46/#47/#48.
3. **Toasts (#20)**: Move to **bottom-right** (Sonner stacks automatically). Add a **bottom offset** so a toast never overlaps the sticky Save/Submit footer (the real prior bug was footer overlap + hover-pause blocking the Save click, not stacking).
4. **SQL↔low-code (#5)**: DO NOT reconstruct. **Cache the low-code AST in the form** and restore on switch-back to low-code (covers flit-back-and-forth). Only if the user hand-edited the raw SQL while in SQL mode → fall back to clear + improved warning. Coordinate the SQL_TO_LOWCODE modeSwitch copy (#4) with this.
5. **Draft % sampling (#86)**: SKIP ENTIRELY — implemented on another branch.
6. **Drafts & Review popout (#13)**: FULL PARITY. Full visual redesign of all 3 (RR-style header + FadeIn, remove "Create rules" button, consistent cards, order RR → MT → TS). Real change-diff popout for RR now, AND add the backend endpoints so MT (checks_json per version), TS (version snapshots), and per-table rule-history diffs work too. BUILD a new "Monitored Tables needing approval" card (none exists today). Reuse RulesReview/RulesTable/RegistryRuleBadges for styled diffs.
7. **Verification**: Deploy per theme to `fe-sandbox-dq-demo`/`dev` and live-verify. If a deploy is problematic (e.g. permission-related), just carry on as normal (don't block). Keep local suites green (app-check/app-test/tsc/bun) throughout regardless.
8. **Long tail (~80 items)**: Proceed on my judgment using the recommended defaults recorded in QUESTIONS.md. Record each non-obvious decision in the plan/commits.

## Standing defaults (my judgment, from QUESTIONS.md — apply unless contradicted)
- **Model**: fable for implementer subagents if no spend-limit; fall back to opus 4.8 on spend-limit error (per user's standing instruction — no need to re-ask). Probe fable first.
- **Stale files (#10)**: delete only truly-dead files (SidebarUserFooter.tsx, Navbar.tsx). KEEP redirect-stub routes (bookmark preservation). Leave orphaned /profiler unless clearly superseded (verify).
- **Loading screen (#26)**: app default = **dark** (reconcile __root `dark` vs provider `system` → dark); honor stored/system preference via an inline pre-mount script.
- **Results icon (#54)**: `LineChart` (lucide), not colliding with Profile's BarChart3.
- **Animated sidebar icons (#55)**: DROP (lucide has no animated variants; low value / high effort) unless trivial.
- **#18 review-status alignment**: target = the per-run review status in the RESULTS UI (ties to #15), not runs-history (already a column).
- **#58 "already exists"** = run in-progress (not "any run ever").
- **#42 versions**: make RR match MT/TS filled secondary Badge + em-dash at v0.
- **#93**: swap Approved/Rejected in RR + MT status filters.
- **#91 DQ-quality buckets**: match dqlake's boundaries (0-25/25-50/50-75/75-100 + no-score); client-side filter.
- **#6 canonical buttons**: "Save as draft" / "Submit for review"; top-right position; single Submit icon; RR gets a top-right header action row (footer removed).
- **#89 model default (#23)**: research exact endpoint; note current gateway default `databricks-gpt-5-5`. Change only the gateway default (not DQX_LLM_ENDPOINT) unless verified. Confirm exact "5.4 nano/mini" endpoint name exists in workspace before hardcoding; else pick the closest available small Databricks-hosted model and note it.
- **AI default ON (#27)**: per explicit user request, default AI settings to ON (note OBO cost implication in the plan).
- **Comments (#19)**: add monitored_table + data_product entity types; mount via 3-dot menu.
- **Entitlements (#43)**: roles already act as the hard outer gate; verify + document + add tests that object grants can't exceed role; rename "Role Management" → "Entitlements". Do NOT remove the admin/approver bypass (that IS roles-as-hard-boundary, consistent with intent) unless later told.
- **Approvals mode (#94)**: implement the 3-state admin setting exactly as specified, default ENABLED. auto-bypass predicate = user is admin OR (can edit AND can approve). In auto-approve/disabled mode, record the acting user as approver with an "(auto)" marker.
- **PR/branch**: implement on `ui-tweaks`; commit per item/theme; do NOT open a PR — leave for the user to review in the morning.

## Cross-cutting engineering constraints (from exploration)
- i18n: every string change in all 4 locales (en/es/it/pt-BR); en is source of truth.
- Backend model changes → `make app-regen-api` (never hand-edit ui/lib/api.ts).
- SINGLE-OWNER shared files (no parallel edits): `RegistryRuleFormDialog.tsx` (items 1,2,4,8,25,32,56 + RR header 6), `ScoreTrendChart.tsx` (60,65,83), `MultiTableResults.tsx` (60,67,84), `Labels.tsx`/LabelFilter (30,16), `dropdown-menu.tsx` (44), `route.tsx`/SidebarLayout/HeaderUserMenu (11,12,28,54,55,72), locale JSON files (many — serialize edits).
- Build ONE shared per-binding run-activity hook (mirror useProductRunSets) for #58/#69/#76.
- Preserve `getWorkspaceHost`/`_workspace_host` (needed by #15) when removing Insights (#62).
- dqlake reference source: `/Users/oliver.gordon/Documents/Code/Other/databricks-dqwatch/src/dqlake/ui`.

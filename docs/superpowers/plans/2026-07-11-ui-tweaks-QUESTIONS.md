# UI Tweaks — Open questions / decisions to confirm with user

Accumulated from exploration agents. Compile into batched AskUserQuestion once all agents report.

## GLOBAL
- **dqlake source access**: ANSWERED — dqlake (aka dqwatch) is at `/Users/oliver.gordon/Documents/Code/Other/databricks-dqwatch`, UI at `src/dqlake/ui` (git `main`). Every "faithfully like dqlake" item (21 schedule, 31/50 profiling version switcher, 35/51 apply-rules layout + big add button, 82 failed-records pager, 91 dq-quality filter buckets, 41 version picker, etc.) can diff against real source. Implementers MUST reference this.
- **Model for implementer subagents**: fable if no spend limit, else opus 4.8. Test fable first with a tiny probe.

## From agent 1 (nav / chrome / home)
- **11** Sidebar: final placement of Runs History / Results / Insights ("decide the rest"). Also: is "Review and Approve" a rename of the single "Drafts & Review" item, or a SPLIT into two separate items ("Drafts" + "Review and Approve")? (Item 11 text implies split: "Drafts and Review (which you rename to Review and Approve)".)
- **12** Username dropdown: layout is "agent's discretion" — OK to proceed on judgment. Note: Language currently lives on the Profile page, not Config (item premise slightly off) — fine, still moving it into the username dropdown sub-menu.
- **20** TOAST bottom-right CONTRADICTS an intentional prior fix (toasts were moved to top-center because bottom-right toasts block the sticky Save/Submit footer — the "can't save a rule" bug). Need sign-off + mitigation choice (offset the toaster / add margin above footer / closeButton / disable hover-pause). DECISION NEEDED.
- **26** Loading screen theme: app default disagrees in code — `__root.tsx` sets `defaultTheme="dark"`, `theme-provider.tsx` defaults to `"system"`. Which is the intended app default? (dark vs system)
- **54** Results icon: pick one (LineChart / Activity / TrendingUp). Must not collide with Profile's BarChart3. Recommend LineChart. (agent discretion likely fine)
- **55** Animated sidebar icons: lucide has no animated variants; would need custom motion work or a new dep. Lowest-value/highest-effort. DROP or keep minimal? Recommend drop.
- **10** Stale files policy: safe deletes = SidebarUserFooter.tsx, Navbar.tsx (dead). Redirect-stub routes exist on purpose (bookmark preservation) — keep or remove? Orphaned full pages: /profiler (2276 lines), /rules/create — dead or intentionally URL-reachable? DECISION on policy.
- **64** Homepage score "?" tooltip: divergence detection likely needs backend support (cached global vs permission-filtered). Confirm it can differ + how to detect. May be deferred if backend-heavy.
- **73** Home get-started Settings tile admin-gated 3x-wide — confirm non-admin sees only RR/MT/TS row.

## From agent 3 (runs history / profiling / insights)
- **15** ONE real backend gap: `job_run_id` is stored but not exposed → needed for "link to run in Databricks". Requires backend change + `make app-regen-api`. Approve backend change? Backend returns prebuilt `run_url` vs UI assembles from host+job_id? Everything else in 15 needs NO schema change.
- **15** Live "Time" column: validation runs have no duration field, profiling runs do. What's the time source for validation runs — elapsed from `created_at`? (reusable 1s-tick pattern exists in ProductRunsTab).
- **15** Review-status is currently in the run expansion; item says move it to the results UI "for the given run". Confirm target surface before removing from expansion (cross-cutting).
- **15** Hierarchy shape: TS→MT→runs when run via TS; MT→runs when run via MT directly. MT-direct runs still mint a run-set. Confirm the tree shape.
- **14** Keep the header Refresh button on Runs History (RR has none)? Match RR font-semibold + page-level FadeIn.
- **16** "Run by" dropdown scope: distinct users in current result set, or fuller principal list?
- **17** Runs-history-as-tab: in-page tab vs navigate-to-/runs-history-with-filter? (needs a new search param per surface). Recommend navigate-with-filter (reuses existing page).
- **49/96** Profiling slow: recommend adding a `table_fqn` filter to `GET /profiler/runs` (currently pulls ALL runs, limit 500, filtered client-side). Approve small backend filter?
- **50/31** Profiling version switcher: FULLY feasible, ZERO db changes (every run's full results already persisted per run_id). Switcher UI: dropdown vs prev/next? Page size for column pagination?
- **53** "last profiled" is broken because nothing ever writes `last_profiled_at` (always None). Fix by derive-on-read (correlated lookup to dq_profiling_results, self-healing) vs write-on-completion. Recommend derive-on-read.
- **62** Remove Insights: wide surface. MUST keep `getWorkspaceHost`/`_workspace_host` (item 15 needs it). Remove admin embedded-dashboard settings + stored key, or just hide nav? Confirm.

## From agent 8 (drafts & review — item 13)
- **13(c)** BIG: there is NO "Monitored Tables needing approval" card today. The 3rd section is per-table rule drafts. For "order RR/MT/TS", do we (i) build a new MT-approvals card, or (ii) treat existing per-table drafts as the MT slot? DECISION NEEDED.
- **13(e)** "Actual change" popout feasibility: only RR rules can diff previous-vs-proposed TODAY (full definition snapshots exist). MT versions omit `checks_json`; TS has no version-snapshot endpoint; per-table history is written to `dq_quality_rules_history` but has NO GET route. Scope: ship RR-only diff now, vs commit to new backend endpoints (MT checks_json per version, per-table rule-history GET, TS version snapshots) for full parity? DECISION NEEDED (backend lift).
- **13(d)** Consistency: per-table section has a rich toolbar/bulk-actions/expand; the two approval cards are simpler. Slim per-table to match, or keep its features? 
- **13(b)** Remove "create rules" button (2 spots) — straightforward.
- **11** "Review and Approve": there's no separate Review page — it's the single Drafts&Review page. Rename = i18n + nav label only. (So item 11's "Drafts and Review" likely stays ONE destination, just renamed — reconcile with the "split into two" reading.)
- **18** Review-status alignment really lives in runs-history (already its own column); on drafts it's the lifecycle Status column, already aligned. Item 18 may also want review status in MT/TS RESULTS UI (ties to 15).
- Good news: reusable header/anim recipe documented (FadeIn + PageBreadcrumb + h1.text-2xl font-semibold). Reusable rule renderers exist (RulesReview, RulesTable, RegistryRuleBadges) for styled diff popouts.

## From agent 2 (rules builder)
- **1** Confirm target order = Joins first, then Group-by. (Trivial swap; independent components.)
- **2** Confirm canonical sub-header = `text-sm font-semibold` `<h2>` (as MT/TS detail use). Do About-tab field labels (Name/Description) count, or only true section titles? No shared heading component — extract one?
- **3** GENUINE design pass (not one-liner): rows use `fr` grid tracks so controls stretch edge-to-edge. Target layout — left-packed content-width (dqlake-style) vs fixed-max-width? Wrap on narrow widths?
- **4** "Second sentence" = the `rulesRegistry.modeSwitch.*.body` strings (6 directions), all 4 locales. Coordinate SQL_TO_LOWCODE wording with item 5's outcome.
- **5** VERDICT: general reconstruction INFEASIBLE (no SQL parser; lossy AST↔SQL mapping). DQX-generated SQL round-trip is PARTIALLY feasible (predictable shape). Recommended: attempt reconstruction, fall back to blank-and-warn on unparseable. DECISIONS: (a) scope = DQX-generated only, or best-effort on hand-written too? (b) partial-parse-and-warn vs all-or-nothing? (c) add a SQL-parser dep vs hand-rolled restricted parser? This is a SUBSTANTIAL new module, not a tweak.
- **8** Remove for all cases or only "new rule"? (new+edit share the key). Must satisfy Radix DialogDescription a11y after removal (visually-hidden).
- **32** Reusable column name is likely ALREADY editable in the form (only the family is locked). Real ask probably = make renames survive the "As JSON" round-trip (`parseDqxCheckJson` re-canonicalizes today). Confirm.
- **56** Severity already EMITTED in JSON (criticality from severity). Gap is PARSE side: top-level `criticality` is ignored on import; reverse-map criticality→severity is ambiguous (many-to-one). DECISION: what exactly does "incorporate" mean — make criticality/severity authoritative on import? reconciliation policy vs user_metadata.severity? surface `severity` as first-class JSON field? Also: does bulk YAML import (rules.import.tsx) need severity handling?
- CONCURRENCY: items 1,2,4,8 all edit `RegistryRuleFormDialog.tsx` (+4/8 edit locale JSON) — CANNOT parallelize against each other. Item 3 isolated to lowcode/*. Items 32+56 both center on registry-rule-conversion.ts — one agent.

## From agent 5 (settings / permissions / approvals / entitlements / steward / comments)
- **43** Two-layer model: `require_role` is already the hard outer gate; object grants are additive (can't escalate past role) EXCEPT ADMIN/RULE_APPROVER explicitly BYPASS object grants, and everyone gets implicit SELECT+APPLY. DECISION: does 43 want to (a) REMOVE the admin/approver bypass, or (b) just guarantee+document+test that grants never exceed role? Rename "Role Management"→"Entitlements" (copy + 4 locales)?
- **86** Draft sampling: `0` currently = whole table (must change). % sampling is executed by an OUT-OF-REPO task-runner wheel — MUST verify it supports % (TABLESAMPLE) vs only row LIMIT before promising "% of data". Need new "entire table" sentinel (retire 0) + migration of existing stored 0s.
- **94** Approvals mode: no global setting exists today (approvals always-on, role-wired). New 3-state enum + wire every submit/approve path + RR/MT/TS buttons (broad). DECISIONS: exact "can edit+approve" predicate (role perm vs object MODIFY vs both)? who is recorded as approver in auto-approve mode? global or per-object-type?
- **37** Delete icon is ALREADY present next to edit in PermissionsTab (:532-547). Likely STALE/done — confirm, or does it mean the users-group default row (intentionally has no trash)?
- **38** Thread objectType into GrantDialog; MT vs TS wording (user gave draft copy). Trivial.
- **39** Pure CSS (grid columns + `select-none`). Confirm scope = grant-dialog hints.
- **33** TS already defaults steward→creator. MT + registry rules do NOT. UC table owner IS fetchable (TableInfo.owner) but only via OBO route (not SP executor). DECISIONS: owner lookup as OBO w/ fallback-to-creator on failure? what to store if owner is a GROUP/SP not a person? bulk-register does per-table owner lookup (N calls) or just creator?
- **19** Comments backend only supports run|rule today; add monitored_table|data_product entity types (trivial, no migration). Mount via 3-dot menu (ties to 17/44). Presentation dialog/sheet vs inline?
- **27** Settings: all content already componentized as cards → tabs+search is mechanical. DECISIONS: tab taxonomy (which settings under which tab)? AI-default-ON is a PRODUCT/SECURITY/COST call (AI runs OBO) — confirm. Copy rewrite sign-off + 4 locales. Role form swap Group/Role + padding trivial.

## From agent 6 (apply-rules / AI suggester / gateway)
- **KEY**: TWO separate AI systems — (1) RuleSuggester maps EXISTING published rules onto columns (emits rule_id+mapping only), (2) AiRulesService.generate_rule AUTHORS one new rule (dqx_native then sql fallback, NO lowcode). Item 89 conflates them.
- **89** LARGEST/HIGHEST-RISK. DECISIONS: which flow does 89 target — suggester, generator, or both? "fill ALL settings for ALL rule types" = all CHECK_FUNC_REGISTRY fns with populated args? lowcode-AST generation has NO existing prompt/validator (net-new, app-specific). "variable column names & types" scope? Recommend scoping this down hard or splitting into sub-phases.
- **89/5** `{{columns}}` templating: no plural/variable support today (fixed per-type slots). Semantics decision: does `{{columns}}` expand to a list or is it a single free placeholder? Requires lowcode AST/compile/validate changes.
- **23** Current defaults: gateway = `databricks-gpt-5-5` (app_settings, AI OFF by default); DQX lib = `databricks-claude-sonnet-4-5`. "5.4 nano/mini" → need EXACT endpoint name available in target workspace (don't guess). gpt-5 temperature/reasoning-budget handling is tuned per-family — a different family may need retry logic revisited. Change only gateway default, or also DQX_LLM_ENDPOINT?
- **35** dqlake layout: is "suggest rules" a 3rd pill segment or a separate button? Keep by-rule→by-column order. Removing top "+Add rules" couples to item 51 (must be replaced by big wide button in by-rule tab).
- **36** "apply rules" replaces both bulk `addRulesButton` and per-column `addRuleButton`? (recommend yes)
- **45** Confirm inverted by-column target (column header + compact rule rows reusing RuleGroupCard shell). Remove refresh (safe; prefetch still fires).
- **46** Both target buttons already have the gradient as BASE state; chip animation is hover-transition-into-gradient from neutral. Adopt chip feel or layer onto existing?
- **51** Page size per lens? wide add button above or below list? reset pagination on search/filter?
- **48** "g" clipped by bg-clip-text; local leading/padding fix (shared AI_TEXT_GRADIENT reused elsewhere).

## From agent 4 (results / scores / genie)
- Charting lib = Recharts 3.8.1 (no new dep needed). Shared files ScoreTrendChart.tsx + MultiTableResults.tsx feed MT/TS/global/RR — SINGLE-OWNER these in the plan (60/65/67/82/83/84/85 collide).
- **83** Zoom FEASIBLE (Recharts Brush or ReferenceArea drag-zoom, no dep). DECISION: Brush (mini-range) vs drag-select-to-zoom (matches dqlake wording) + reset affordance? Apply to all charts or score/trend only? Custom OverallLayer must re-clamp.
- **60** "wrap back on itself" already fixed (curve clamping). Overflow = missing `min-w-0` on grid children → CSS fix. Cap max width on very-wide global layout?
- **61** ROOT CAUSE confirmed: BindingResultsTab has NO local Suspense; 3 suspending hooks bubble to page-level Suspense → whole page flashes/needs refresh. FIX = add per-tab Suspense (TS page already does this via TabBoundary). Skeleton shape?
- **65** Needs BACKEND change (no version field on TrendPointOut/RunRowOut). DECISIONS: "version" = MT binding version or rule-registry version? tag visual (dot/flag/vertical line + "v2")? confirm TS product version exists.
- **67** Add `hideRunMode` prop to MultiTableResultsSection + force published. No backend change. Confirm RR scope = RuleResultsTab only (not other RR surfaces)?
- **82** MAX 200 already enforced; caption already "Showing {{range}} of {{total}}". Remaining = match exact dqlake caption string + keep 20/page client paging? (diff dqlake source)
- **84** Signal exists (`useProductRunSets().hasActive`, polls only while active). Banner easy; TRUE progress bar (X of Y tables) needs per-member status not in summary. Banner vs real progress bar?
- **85** Frontend-only (`run_mode` already on RunRowOut). Tag placement (inline badge vs suffix); only in Published+Draft mode?
- **75/79/87/68** DATA-FLOW root causes found:
  - 87+68 unified: `check_count` counts MATERIALIZED `dq_quality_rules` only. Unmaterialized→0 (68 fresh-install); ORDER BY updated_at DESC means recently-approved table is the visible non-zero one (87). DECISION: count materialized checks vs applied rules (`dq_applied_rules`)? show applied count for draft/pending instead of 0? Confirm materialization timing on this branch.
  - 79: "last run" reads `last_profiled_at` (profiler-only) → blank for validation-only tables. Needs backend to surface true last-run ts on MonitoredTableSummaryOut. DECISION: last run = last validation run (any status?) vs last successful; keep separate "last profiled"?
  - 75: score cache reads latest PUBLISHED run. Failed run → no published score → stale; draft-only table → None forever. DECISION: after failed run show last-good / 0% / "failed-stale" indicator? draft-only tables show draft score in overview? (ties to 64 "?")
- **63** Reuse existing `resultsUi.countInfo` `<Trans>` bolding pattern (already exists). Wording given. Tooltip vs inline?
- **66** Genie already suppresses trivial single facts; but emits chart+table together. DEFINE "helpful" precisely (thresholds; chart-only vs table-only vs neither).
- **80** Contexts already scoped per surface (MT/TS/global keys). GAPS: RR rule page has NO Genie provider (add `rule ${ruleId}` context+provider); optional callers omitting context fall to shared `__default__`. Clear-on-nav vs rely on per-context keys?
- **81** ROOT CAUSE: line chosen ONLY if label column NAME matches a time regex; everything else = bar. DECISION: new line-vs-bar rule (time-only / monotonic numeric / category-count threshold).
- **95** "pop" styling: reference (sample-data suggested questions) NOT located yet — implementer must find during impl. Define pop (hover-scale/shadow/spring)?
- **97** Icon+"Ask Genie" inset by bubble px-3 padding → misaligned with prose. CSS fix. Alignment target (bubble edge vs panel edge)?

## From agent 7 (overview pages / filters / buttons / dividers / schedule / sample-data)
- SINGLE-FILE SAFE WINS (parallelizable): 44 (dropdown-menu.tsx destructive focus bg — one-line, fixes RR/MT/TS at once), 24 (ProductTabsShell group arrays), 93 (reorder 2 SelectItems), 52 (MT label + tooltip), 22 (drop `title` attr).
- **6** Biggest divergence: RR Save/Submit are in a BOTTOM FOOTER; MT/TS are top-right header. Moving RR to top-right header = medium risk (no header-action component; must preserve disabled-reason tooltips + isEditing&&!isDirty branch). Canonical labels ("Save as draft"/"Submit for review")? Does RR footer disappear? One Submit icon (Send vs UploadCloud)?
- **25** Divider before Results: TS ✅, MT ✅, RR ❌ MISSING. Add divider before RR Results trigger. (RR groups Test/History with Results on right — divider before Results only, or restructure?)
- **34** MT split-button arrow only disabled on isPending; disable when no menu item actionable. Low.
- **58/69/76** ALL need a reliable per-binding run-in-progress signal. MT's current `trackedRunSetId` is session-local (unreliable). Build ONE `useMonitoredTableRunActivity` hook mirroring `useProductRunSets` (binding-scoped), consume in Run button (disable+spinner), 58, and Results banner. TS already correct. Confirm building this hook + retiring use-active-runs/use-job-polling?
- **58** "already exists" almost certainly = in-progress (not "any run ever"). Confirm.
- **59** Swap primary label to "Run draft" when draft/dirty (logic exists). When both approved+draft exist, draft wins?
- **76** Run row-action = medium (rows lack version context). Far-left running cog = NOT easy (needs 69 infra + colgroup change). RAW says "only if easy" → do Run action, defer cog?
- **7** New TS "Untitled/back" header redundant (PageBreadcrumb already gives back path). Mirror RR-new pattern or collapse to simple form? Keep pre-save Permissions tab.
- **29** RR filters w-36 vs MT/TS w-40. Standardize width (w-44?) + extract shared OverviewFilterBar?
- **30** HIGH effort (component redesign): revise Labels.tsx LabelFilter to key-first + per-key value sub-dropdown; RR currently filters `tag` SERVER-side as a string → moving to key/value multiselect needs client-side filtering or a new backend tag filter (affects pagination). Confirm LabelFilter redesign + all consumers move together; server→client tag filter acceptable?
- **42** RR version = plain text; MT/TS = filled secondary Badge + em-dash at v0. Make RR match MT/TS (recommended). RR v0 handling?
- **57** MT dropdowns are plain Select (no search). Replace with Command/Popover combobox or MultiSelectPopover. Apply across all overviews?
- **90** TS steward filter gated on options.length>0 → pops in after data. Render unconditionally. Trivial.
- **91** No DQ-quality filter today; score available. Bucket boundaries? null-score handling? client-side (simplest) vs backend param?
- **92** RR/TS steward filters client-side (look fine). MT sends `steward` as SERVER param — likely the broken one (backend may ignore it). VERIFY backend; fix backend or switch MT to client-side.
- **93** Swap Approved/Rejected in RR + MT status filters. Trivial. (TS has no status filter.)
- **40** Move #rules/Status labels into per-group header rows — REVERSES a prior intentional P25 decision. Confirm reversal.
- **41** Version picker: always show (drop selected.has reveal); invert light/dark fill; clicking picker also selects row (rework stopPropagation). Handle unselected v0/ineligible rows.
- **21** HIGH/needs data: MT schedule tab has no awareness of parent TS schedules. Needs table→table-spaces reverse lookup (backend endpoint or client scan of useListDataProducts members). Feature, not tweak. Copy dqlake layout.
- **22** Drop `title` attr on sample-data cells (also About/schema table uses title — in scope?). Replace with only-when-clipped Radix tooltip or fully remove?
- **70** Sample-data skeleton is one flat 256px block; build composed skeleton (input + chips + table).
- **18** AMBIGUOUS which screen — runs-history already has a Review column. Likely the RESULTS UI per-run review status (ties to 15). CLARIFY which screen.
- **78** Perf: MT overview fires TWO list queries (filtered + full unfiltered for facets); TS refetchOnMount:always; dqScore LEFT-JOINs dq_score_cache; TS sums members client-side w/ fully-populated members payload. Needs a dedicated perf pass / bisect. Removing MT's unfiltered facet query acceptable (restricts facet options)?

## VERIFICATION GAPS (need backend read or user):
- 92 backend `steward` param behavior; 78 backend list timing; 18 which screen; 86 out-of-repo runner % support; 65/79/75 backend model additions.

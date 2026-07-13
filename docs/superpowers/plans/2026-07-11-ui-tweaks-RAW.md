# UI Tweaks — Raw request list (captured 2026-07-11)

Source: user's overnight-implementation request. Branch `ui-tweaks` off `rules-registry`.
Process: subagent-driven development, parallelised, easy layout tweaks first. Model: fable if no limits, else opus 4.8.
DO NOT DROP ANY ITEM. Each has a stable ID.

## Rules builder
1. Low code -> advanced: swap joins and group bys (ordering).
2. Sub-headers in About / Implementation differ from sub-headers elsewhere in the app. Make consistent.
3. Low-code conditions are fully justified as full rows — looks terrible. Fix layout.
4. When swapping between dqx-native / sql / low-code-sql rules, remove the second sentence (of the mode description).
5. On sql -> low-code-sql swap: instead of always blanking low-code sql, try to RECONSTRUCT low-code sql; if not possible, show a warning that it can't and must be cleared. (Feasibility Q.)
8. Remove "This rule isn't tied to a table yet. When you apply it to a monitored table, you'll match its columns to real columns there." when making a new rule.
32. For dqx-native rules, allow changing the reusable column name.
56. Incorporate severities (now attached to dqx-native error/warn states) into the native rule JSON feature.
89. AI rule suggester upgrades (also see 45): suggest variable column names & types, fill ALL form settings for ALL rule types (dqlake did this); suggest low-code-sql FIRST, then dqx built-in, then raw sql; reuse built-in dqx logic if possible; low-code-sql editor params accept {{columns}} instead of fixed per-type.

## Buttons / behaviour consistency
6. Save-as-draft / save-and-submit / run / publish style buttons consistent in TEXT, BEHAVIOUR, POSITION (top-right) across Rules Registry (RR) / Monitored Tables (MT) / Table Spaces (TS).
34. Pass on enabled/disabled state of save/submit/save/run buttons. E.g. Run Now: when everything (incl dropdown) is disabled, the down-arrow still opens the dropdown — shouldn't.
52. Swap "Run now (V<x>)" for "Run now" with hover "Run the latest approved version (v<x>)".
58. "Run now" disabled when there is already a run (MT/TS).
59. If in draft state / unsaved changes, change "Run now" -> "Run draft".
69. Run-in-progress -> "Run now" disabled with spinner. Detection unreliable in Results page but better in Runs History — unify.
36. MT -> Apply Rules: buttons that say "add rules" should say "apply rules".

## Copy / text
9. Change RR/MT/TS descriptive text (e.g. "Apply registry rules to tables and materialize them into active checks.") to dqlake homepage "get started" style (e.g. "Apply rules to specific tables and monitor their data quality").
63. Change "<x> checks via <y> rules" explainer to: "A rule is a piece of reusable logic. A check is when a rule gets applied to a column/set of columns. A test is when a check runs against a data row" — include the bolding as in the results screen.

## Sidebar / nav / chrome
11. Restructure sidebar: RR / MT / TS, then horizontal divider, then Drafts and Review (rename Review -> "Review and Approve"). Move "Import rules" under the username dropdown. Decide display of the rest.
12. Remove the "DQX Studio - Data Quality explorer" bar. Put version + GitHub link inside username dropdown. Redesign order/layout of that dropdown (agent's discretion). Rename Configuration -> "Admin Settings". Bring Language OUT of Admin Settings into a sub-dropdown in the username dropdown.
28. Collapsed sidebar: keep icons + hover text (like dqlake).
55. Animated icon versions for sidebar (optional / if available).
54. Make the "results" section icon consistent across the UI — maybe a line-graph icon.
72. Homepage: remove the "home" tab; reach home via the DQX Studio icon (like before).

## Homepage
64. If overall DQ score on homepage differs from overall results screen (UC/permission differences), add a "?" with a 1-sentence explanation next to the overall score (only if the case).
73. Home -> Get started: only show RR/MT/TS. Show "Settings" (renamed) as a single 3x-wide tile only if the user is admin.
74. Swap order of "at a glance" widgets to: rules, tables, table spaces.
88. Clamp the home average-quality graph so it doesn't overflow.

## Drafts & Review
13. Design pass on Drafts & Review: too inconsistent, lacks header layout/page animations of e.g. RR. Remove the "create rules" button. Order cards: RR rules / MT / TS needing approval. Make contents/layout/style consistent across all 3. Use each item's history to display the ACTUAL change in a popout box; bonus if styled like the original RR/MT/TS UI.

## Runs History
14. Runs History header/animation style differs from RR/MT/TS — align.
15. Show BOTH Profiling (was surfaced in main-branch profile + generate->run history) and Validation run types. Table style consistent w/ RR overview. Remove "Rules","Total","Valid","Errors","Warnings" columns. Link to the run in Databricks itself (dqlake runs panel does this). Add a "Time" column that live-updates while running (UI does this elsewhere in main-branch profile+generate->run history). Remove expansion box except for FAILED runs. "History" now irrelevant. "review status" should show in the tables/table-space results UI for the given run instead. Support dqlake-style hierarchy: TS -> MT -> DQ validation/profile runs (if run via TS), or MT -> runs (if run via the MT itself).
16. Runs History search bubbles styled like RR/MT/TS. Remove "has failures"/"all labels"/"review" bubbles. Change "my runs" to a "Run by" user dropdown with top-level "Me". Swap "Requested By" -> "Run By".
17. Provide a filtered Runs History as its own tab via the existing 3-dot top-right menu in RR/MT/TS.

## Overview pages (RR/MT/TS) — columns, filters, actions, dividers, results tab
7. New Table Space uses old dqlake "Untitled table space / back to table spaces" UI — remove, make consistent.
24. Table Spaces: vertical separator currently between About/Permissions — move to between Permissions and Tables.
25. RR/MT/TS: vertical tab divider before Results — verify present.
29. Actions bar consistent across RR/MT/TS overview; make wider so it isn't cramped.
30. RR overview: change "tag" search to be like Runs History labels-filter dropdown; that dropdown itself changed so you multi-select from TAG KEYS only, then optionally expand a sub-dropdown per key to select label(s).
42. Columns consistency pass in RR/MT/TS overview — e.g. "versions" is text in RR but lightly-filled tags in MT/TS. Unify.
44. RR rules 3-dot menu: "delete" has different hover (no fill) vs other options; same in MT/TS.
57. Search inside MT overview dropdowns (catalogs/schemas/stewards).
77. Results tab for rules is in the wrong place. If the rule hasn't been applied anywhere, disable the tab + explanatory hover text.
91. MT/TS: add a DQ-quality filter (0-25% buckets etc, like dqlake).
92. Make the "all stewards" dropdown filters actually work in overview screens.
93. "All statuses" dropdown order: swap Approved and Rejected.
90. Table Spaces: "all stewards" filter appears with content load, not with the page (unlike other overviews). Fix timing.
19. Move "comments" and make them work in RR/MT/TS via the 3-dot top-right menu.

## Permissions UI
37. Add a "delete" icon next to the edit icon in permissions of RR/MT/TS.
38. "Inherit to child objects" explanation text: MT -> "When enabled, appropriate access will be given to underlying rules"; TS -> "...to underlying tables and rules".
39. Align permission explanation texts to a shared left-justified column; make explanation text non-highlightable.

## Settings (was Configuration)
27. Big design pass on Settings: dqlake-style grouped tabs, each setting in its own card, add a settings search. Default AI settings to ON. Text review — remove AI cringe/stream-of-consciousness/over-explanation (use a nitpicky UI/UX subagent; research; iterate until happy). Role Management: swap order of Databricks Group / Role; fix padding.
43. Role management / entitlements (rename "Role Management" -> "entitlements"?) act as HARD boundaries like Lakehouse — cannot be bypassed by granting in the object-level permissions.
86. Admin: draft-run setting renamed "Number of rows in draft runs". 0 must NOT mean whole table. Switch between number-of-rows and %-of-data; each disable-able by a toggle "Run against entire table". Make functional.
94. Admin: approvals logic mode — enabled / enabled-but-auto-bypass-if-admin(or user can edit+approve) / disabled(auto-approve everything). Update RR/MT/TS buttons to match. Default = enabled.

## Steward defaults
33. New rules/MT/TS default steward = the creator. For MT, default = UC table owner (if owner is none -> creator).

## Apply Rules (MT)
35. MT->Apply Rules: by rule / by column / suggest rules (remove "+add rules") faithfully look like dqlake, but KEEP current by-rule/by-column order (don't switch).
51. "by rule" tab: bring back dqlake big wide "add rules" button. Paginate by-column / by-rule once they reach bottom of screen.
71. Clicking a schema to skip to apply rules: whole row clickable, not just column name; remove helper tooltip.

## AI rule suggestions UI
45. Group-by "rule"/"column" -> capitalize to "Rule"/"Column". Remove the "refresh" button. By-rule UI is good; by-column should be the SAME UI but inverted (fix inconsistent card usage, esp the containing column).
46. Apply the subtle button animation (from MT sample questions) to the "suggest rules" and "add all checks" buttons on Apply Rules.
47. AI rules suggestion screen: scrollbar appears and interferes with the open animation. Fix.
48. Poppet screen: the "g" in "AI Rule Suggestions" is cut off at the bottom. Fix.

## Profiling
49. Profiling tab slow to load — stagger loading of background/results elements.
50. Paginate profiling results. Bring back dqlake previous-runs switcher (no DB modifications if possible). Remove the "profile history" section under the profile.
53. MT "About" tab: "last profiled" doesn't work.
96. "Profile" tab slow to open, esp when no profile available — investigate.
31. Faithfully implement dqlake UI to view previous versions of profiling results (relates to 50).

## Results pages
60. Clamp graphs so they don't overflow / wrap back on themselves.
61. Results page (esp MT) buggy on load, needs refresh — load sub-content in a separate skeleton.
62. Remove "insights" feature (superseded by new results).
65. MT/TS overall DQ-score graph: small tag at the next result after the version increments.
67. Global (all-tables) results: remove published/draft picker — published only. Same for RR.
82. "show a-b of x failed records": follow dqlake style exactly; MAX 200.
83. Allow zoom (select-to-zoom) on results graphs if widgets support it.
84. "Running" progress bar on TS results screen while any underlying table is running.
85. Results "latest" date dropdown: in published+draft state, tag draft dates with "draft".
75. DQ score broken for MT overview (doesn't update after a failed run / ever?).
79. "last run" field in overview tables broken after DQ results page introduced.
87. # of checks per table in MT overview broken — appears to display for the most-recently-changed table only.
68. MT list sometimes shows 0 checks even when fine — pre-feature / fresh-install artifact?

## Genie
66. Genie only returns graphs/tables if actively helpful (not for the sake of it).
80. Add Genie conversation history to the chat, scoped to the current rule/table/table-space/overall-results page; stop conversations carrying over between pages.
81. Genie only plots bar charts when a line would make sense — investigate why.
95. Add "pop" styling to the suggested Ask-Genie questions (like the sample-data suggested questions).
97. The icon + "Ask Genie" label isn't aligned with the responses in the text window — move left.

## Misc UI
20. Toast notifications: move from top/center to bottom-right.
21. Bring in dqlake schedule UI that shows, for a MT, whether a TS also has a schedule set up — copy the layout.
22. Remove native browser tooltip when hovering over sample data.
23. Change default AI gateway to Databricks 5.4 nano or mini — research the most appropriate.
26. Loading screen respects light/dark mode (incl. matching app default if not explicitly set).
40. Adding tables to a TS grouped by schema/catalog: move #rules and Status text to be part of the grouped table(s) immediately beneath them.
41. Same screen: always show the version picker (no row-select needed); invert fill for light/dark; clicking the version picker also selects the row.
70. Sample data: more accurately sized skeletons.
10. Stale UI pages pass — clean up old files.
76. Actions of MT/TS include "Run"; spinner cog on far-left of table row if a job runs against it (only if easy).
18. Review statuses justify/align as if in their own "column".
78. MT/TS pages loading slower now — investigate.

## Meta
- Start with easy layout tweaks, then bigger pieces.
- Parallelise via subagents where safe (avoid two agents editing the same file/area concurrently).
- i18n: all 4 locales (en/es/it/pt-BR) with real translations.
- GPG-sign commits; "Co-authored-by: Isaac".

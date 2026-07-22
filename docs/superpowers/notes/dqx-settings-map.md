# DQX Studio — current admin settings UI map

All in ONE file unless noted: `app/src/databricks_labs_dqx_app/ui/routes/_sidebar/config.tsx` (~3000 lines, route `/config`, admin-gated).
Almost every card is a **private local function component** in config.tsx — NOT exported. Only `RoleManagement`, `PrincipalPicker`, `PermissionsTab`, and shadcn `ui/*` are cross-file reusable.

## Tab bar & AI gradient
- Tab type (config.tsx:2831): `"general" | "ai" | "governance" | "tags" | "entitlements" | "compute" | "danger"`.
- Tabs array (2855-2866) id / i18n label / lucide icon:
  general→`config.tabGeneral`/`Globe`; ai→`config.tabAi`/`Sparkles`; governance→`config.tabGovernance`/`Scale`; tags→`config.tabTags`/`Tags`; entitlements→`config.tabEntitlements`/`KeyRound`; compute→`config.tabCompute`/`Cpu`; danger→`config.tabDanger`/`AlertTriangle`.
- Tab bar render (2954-2993): shadcn Tabs/TabsList/TabsTrigger. `TabsList "inline-flex h-auto items-center p-1"`; triggers `gap-1.5`, icon `h-4 w-4`. Danger trigger conditional red `cn(...)` (2972: `text-destructive dark:text-red-400`). `TabsContent className="mt-4 space-y-6 pb-8"` → space-y-6 = inter-card gap.
- Card→tab mapping: `entries` array (2868-2889) — {tab, title, keywords, render fn}. Authoritative card list.
- AI gradient tokens: `ui/lib/ai-style.ts` — `AI_GRADIENT_FROM_VIA_TO="from-violet-500 via-fuchsia-500 to-pink-500"`, `AI_BANNER_BG`, `AI_BANNER_BORDER`, `AI_ICON_COLOR="text-fuchsia-600 dark:text-fuchsia-400"`, `AI_BUTTON_BG`, `AI_TEXT_GRADIENT="bg-clip-text text-transparent"`, `AI_GRADIENT_URL="url(#dqx-ai-gradient)"`. SVG `<linearGradient id="dqx-ai-gradient">` in `routes/__root.tsx:33-37` (#8b5cf6→#d946ef→#ec4899). CSS `.ai-glow-mouse`, `.ai-text-shine` in `styles/globals.css:231+`.
- config.tsx imports (127): `AI_BUTTON_BG, AI_ICON_COLOR, AI_TEXT_GRADIENT`.

## Per-tab cards (all config.tsx unless noted)
- **General**: `TimezoneSettings` (187, Display timezone), `RunReviewStatusesSettings` (1329), `GlobalResultsSettingsCard` (2271).
- **AI**: `AiSettingsCard` (1813) — enable switch + `ServingEndpointSelect` (1767, shadcn Select from `useListServingEndpoints`, `NO_ENDPOINT_VALUE="__none__"` @1643). Border/title use AI gradient.
- **Governance**: `RulesRegistrySettingsCard` (1954) — 3 switches: `default_auto_upgrade`, `auto_upgrade_without_approval`, `tag_auto_apply`. `ApprovalsModeCard` (2056, `APPROVAL_MODES=["enabled","auto_bypass","disabled"]`@2054). `RequireDraftRunSettingsCard` (2135). `RetentionSettings` (945, two number inputs global + quarantine).
- **Tags**: `LabelDefinitionsSettings` (347, "Rule Labels") → list of `DefinitionEditorCard` (849) + add button (`config.addLabelDefinition`) + Save/Reset. Built-in vs custom via `is_builtin`/reserved keys (`RESERVED_WEIGHT_KEY`, `RESERVED_SEVERITY_KEY` @309-310). Draft helpers `lib/label-definition-drafts.ts`.
- **Entitlements**: `RoleManagement` in `components/RoleManagement.tsx` (imported @10). Has `AddRoleMappingForm` (308) with Role Select (347) + `GroupCombobox` (102, Popover+Command from `GET /api/v1/roles/groups?search=`); rows via `RoleMappingRow` (261); `useCreateRoleMapping`/`useDeleteRoleMapping`. Plus `PermissionsSettingsCard` (2202, single switch — inherit new grants).
- **Compute**: `ComputeSettingsCard` (2332) — `WarehouseSelect` (1652), jobs-compute kind Select, `ClusterSelect` (1707). SP warehouse-access warning + grant button. Sentinels `NO_WAREHOUSE_VALUE="__default__"`, `NO_CLUSTER_VALUE="__none__"` (1644-45). `DraftRunSampleLimitSettings` (1150, single number, 0=whole table).
- **Danger**: `DangerZoneCard` (2574, type-to-confirm, `RESET_DB_PHRASE="reset dqx studio"`@2572 must match backend `RESET_CONFIRMATION_PHRASE`). `DeployDemoCard` (2710, seed demo, danger tab).

## Save patterns
- **A. Auto-save on change (no button)**: `save(...)` from onCheckedChange/onValueChange + invalidate + toast. Cards: TimezoneSettings, RulesRegistrySettingsCard, ApprovalsModeCard, RequireDraftRunSettingsCard, PermissionsSettingsCard, GlobalResultsSettingsCard.
- **B. Save-changes button + dirty tracking + `border-t` divider**: local draft, isDirty, Save+Reset row `"... pt-2 border-t"`. Cards: RetentionSettings (buttons@1107), DraftRunSampleLimitSettings (1232), LabelDefinitionsSettings (484), RunReviewStatusesSettings, AiSettingsCard (1921, AI_BUTTON_BG), ComputeSettingsCard (2548).
- Mutation idiom (both): orval `useGetX`/`useSaveX` + `getGetXQueryKey` from `@/lib/api` & `@/lib/api-custom`, `queryClient.invalidateQueries`, toast with `AxiosError<{detail?}>` fallback. Gating `usePermissions().isAdmin` or `useCurrentUserRoleSuspense()`; disabled `!isAdmin || mutation.isPending`.
- **Spacing**: inter-card `space-y-6` on TabsContent; within-card `CardContent "space-y-4"` (compute space-y-5, labels space-y-3); toggle rows `flex items-center justify-between rounded-md border p-3`; hint `text-[11px] text-muted-foreground`. Each entry wrapped in `<FadeIn>` + `SettingSection` (2834, ErrorBoundary+Suspense, `<Skeleton className="h-40 w-full"/>`).
- CardTitle idiom: `flex items-center gap-2` + 20px lucide icon.

## Reuse targets
- **DQ Stewards selector** = `components/permissions/PrincipalPicker.tsx` (exported `PrincipalPicker` + `PickedPrincipal`). Popover+Command from debounced `useSearchPrincipals` (300ms, min 2 chars); users+groups (`kind:'user'|'group'`), optional pinned "Suggested". Consumed by `PermissionsTab.tsx` (464) + its GrantDialog (257). → reuse in Entitlements ("User/Group").
- **Random sample control** = `SampleSelector` in `components/rules/test/RuleTestPanel.tsx` (353-402), NOT exported. `SampleKind="records"|"percent"|"full"` (52); mode Select ("Random sample"/"Full table") + numeric Input (default 1000/10) + unit Select ("records"/"percent"). Parent state `sampleKind`/`sampleValue` (120-121). i18n: `ruleTest.randomSample`, `ruleTest.fullTable`, `ruleTest.records`, `ruleTest.percent`. → extract/export to reuse in Compute Draft runs.
- **Allowed values mini-section** = `AllowedValuesEditor` (config.tsx:612-847). Row-based: collapsed row (color dot+mono name+desc preview) expands (motion/AnimatePresence) into name/description inputs; "Add value" button (841, `config.addValue`). Severity (`RESERVED_SEVERITY_KEY`) renders warn/error criticality Select per value (766-788), highest-first via displayIndices reversal. Color = `ValueColorEditor` (518). Embedded in `DefinitionEditorCard` (930) under "Allowed values" label + "Allow custom values" checkbox (908-931).

## i18n (CRITICAL)
- All copy via `t("config.xxx")`. 4 locales: `lib/i18n/locales/{en,es,it,pt-BR}.json`.
- Parity test `make app-test` (`tests/test_i18n_locale_parity.py`): all 4 locales MUST have identical key SETS + identical `{{placeholder}}` sets per key. So: adding a key → add to ALL 4; removing → remove from ALL 4; changing English text → edit en.json only (others keep old translation, still parity-valid). Never leave English text in a non-en file for a NEW key though (silent partial-translation) — but for THIS redesign, English-only value edits to en.json are fine; new keys must exist in all 4 (can copy English value into the others).

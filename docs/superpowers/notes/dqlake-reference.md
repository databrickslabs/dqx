# dqlake reference — components to port into DQX admin settings

Source repo: `/Users/oliver.gordon/Documents/Code/Other/databricks-dqwatch`

## 1. Compute picker (source of truth for DQX compute options)

dqlake splits compute into **two separate cards** on one admin page:
- "Jobs compute" (where profile/check jobs run)
- "SQL Warehouse" (for rule tests/dry-runs)

Page: `src/dqlake/ui/routes/_sidebar/admin/compute.tsx` — renders `<ComputePickerCard>` then `<EvalWarehouseCard>` in `max-w-2xl space-y-4`.
- Jobs-compute setting key: `bindings_default_compute`; fallback `{ kind: "serverless" }`.

### A) Jobs compute — `components/admin/ComputePickerCard.tsx`
```ts
type ComputeKind = "serverless" | "cluster";
interface ComputeValue { kind: ComputeKind; cluster_id?: string | null; }
```
Legacy `"warehouse"` kind coerced away — jobs compute is **only** serverless or all-purpose cluster.
Options (Popover + Command combobox):
- Group "Serverless": single item `value="serverless"`, label "Serverless", hollow green dot.
- Group "All-purpose clusters": one item per cluster from `useList_clusters` sorted by name; status dot green if `state==="RUNNING"` else red. Loading spinner / "No clusters available".
Trigger label: "Serverless" or selected cluster name/id / "Select compute".
Description: "Pick where DQX profile and check jobs run. Serverless is the lowest-touch option; an all-purpose cluster gives you control over runtime and libraries."
Manage link: `href={`${workspaceUrl}/#setting/clusters`}`, text "Manage in Databricks" + ExternalLink icon.

### B) SQL Warehouse — `components/admin/EvalWarehouseCard.tsx`
Setting key: `eval_warehouse_id` (warehouse id string or null). Source: `useList_warehouses`.
Split into two Command groups by `serverless` flag:
- Group "Serverless": `w.serverless===true`, hollow green dot.
- Group "Classic": `w.serverless===false`, dot green if `w.running` else red.
- Plus `__saved_fallback__` item when saved id not in list: `{savedId} (not in workspace list)`.
Description: "Used to run rule tests and dry-runs. Serverless is strongly recommended for application performance."
Manage link: `href={`${host}/sql/warehouses`}`, text "Manage in Databricks" + ExternalLink.

**Net compute options:** Jobs compute → {Serverless, All-purpose cluster}. SQL Warehouse → {Serverless warehouse, Classic warehouse}. No model-serving option inside compute.

## 2. "Manage in AI Gateway" link
NOTE: literal string "Manage in AI Gateway" does NOT exist in dqlake. dqlake card is titled "AI Gateway endpoint", link text is "Manage in Databricks ↗".
File: `components/admin/AiEndpointCard.tsx`
Href construction (line ~188-201):
```tsx
{workspaceHost && aiEnabled && (
  <a
    href={ endpoint
        ? `${workspaceHost}/ml/endpoints/${encodeURIComponent(endpoint)}`
        : `${workspaceHost}/ml/endpoints` }
    target="_blank" rel="noopener noreferrer"
    className="text-xs text-primary hover:underline inline-block" >
    {endpoint ? "Manage in Databricks ↗" : "Open serving endpoints ↗"}
  </a>
)}
```
- Base URL: `me.workspace_host` (from `useGet_meSuspense`).
- Endpoint selected → `${workspaceHost}/ml/endpoints/${encodeURIComponent(endpoint)}`.
- No endpoint → `${workspaceHost}/ml/endpoints`.
- Uses trailing `↗` glyph inline (not ExternalLink icon). Shown only when workspaceHost present & ai_enabled true.

**DQX DECISION**: user explicitly asked for text "Manage in AI Gateway". Use that wording, with dqlake's href pattern (workspace host + /ml/endpoints/<endpoint>). Display as part of the option description.

## 3. Settings card / option layout patterns
No shared "SettingRow/SettingCard" abstraction — each card hand-built from shadcn `Card`.
Consistent copyable pattern:
- `Card, CardContent, CardHeader, CardTitle` from `@/components/ui/card`
- Layout: `<CardContent className="space-y-4 max-w-md">` (or space-y-6), header `className="pb-3"`.
- Description: `<p className="text-xs text-muted-foreground">…</p>` under the control group.
- Control: Popover+Command combobox OR Switch+Label.
- Manage deep-link at bottom: `text-xs text-muted-foreground hover:text-foreground inline-flex items-center gap-1` + ExternalLink (`h-3 w-3`).
- Saves via `useUpdate_app_setting({ params: { key }, data: { value_json: { value } } })` + toast.

Cleanest toggle example — `AiToggleCard.tsx`:
```tsx
<CardContent className="max-w-md">
  <div className="flex items-start gap-3">
    <Switch id="ai-enabled" checked={aiEnabled} onCheckedChange={...} className="mt-1.5" />
    <div className="space-y-1">
      <Label htmlFor="ai-enabled">Enable AI Features</Label>
      <p className="text-xs text-muted-foreground">When off, all AI features in DQLake are disabled.</p>
    </div>
  </div>
</CardContent>
```
Other admin cards: `RuleDefaultsCard.tsx`, `RunKindDefaultCard.tsx`, `AiTestDataRowsCard.tsx`, `DerivedDataCard.tsx`.

## 4. "Follow latest version" / version pinning governance
### A) `components/products/MemberVersionPin.tsx`
- `pinnedVersion===null` = "use latest" (auto-follow). A number pins.
- Control offers "Latest (vN)" plus v1..vN.
- Pin is governance metadata only — job still runs current published version; pin drives stale indicator.
- `isMemberStale = pinnedVersion!==null && pinnedVersion < bindingVersion`.
- Dropdown: first item "Use latest (v{bindingVersion})" (onPinChange(null)), separator, then vN..v1.
Consumed in `AddTablesDialog.tsx` (under "Version to track") and `ProductTablesTab.tsx`.
Governance description (AddTablesDialog.tsx): "Select monitored tables to include in this data product. Each table uses the latest published version of its monitor by default; pin a specific version below if you want to track it for governance."

### B) `components/bindings/RuleConfigCard.tsx` (`VersionPinDropdown`, line ~446-557)
Same idiom for a bound rule. Badge label `v{n} · {Latest|Pinned} ▾`. Dropdown first item "Latest" (onPinChange(null)), then versions.

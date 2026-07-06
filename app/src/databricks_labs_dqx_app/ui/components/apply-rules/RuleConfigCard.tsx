// RuleConfigCard — one applied registry rule's card in the by-rule lens.
// Ported 1:1 (structure/interactions) from dqlake's `bindings/RuleConfigCard.tsx`:
// a collapsible card with a chevron header, version-pin + severity-override
// dropdown badges, a "Rule logic" disclosure showing the rule's SQL/native
// definition read-only, and an overflow menu to remove the application.
// All controls mutate the tab's LOCAL staged row list ONLY (P16-F) — nothing
// here writes to the network. Persistence happens once, in a batch, when the
// caller hits Save-as-draft/Publish on the tab.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "@tanstack/react-router";
import { Check, ChevronDown, Loader2, MoreVertical } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";
import type { AppliedRuleOut, ColumnOut, RegistryRuleOut, RuleParameter, RuleSlot } from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import { paramValueToRaw } from "@/lib/registry-rule-conversion";
import { MultiColumnPicker, SingleColumnPicker } from "./ColumnPicker";
import { MappingChips } from "./MappingChips";
import { RESERVED_DIMENSION_KEY, RESERVED_SEVERITY_KEY, TagBadge, colorFor, getUsedColumnsForRule } from "./shared";

// ---------------------------------------------------------------------------
// Completeness status — derives whether every applied mapping group fills
// all of the rule's declared slots. Drives the yellow "incomplete" styling
// and the by-rule/by-column "needs attention" filters.
// ---------------------------------------------------------------------------

export interface RuleStatus {
  kind: "complete" | "incomplete" | "no-mapping-needed";
  incompleteGroupCount: number;
  totalGroupCount: number;
}

export function computeStatus(rule: AppliedRuleOut, slots: RuleSlot[]): RuleStatus {
  const slotNames = slots.map((s) => s.name);
  if (slotNames.length === 0) {
    return { kind: "no-mapping-needed", incompleteGroupCount: 0, totalGroupCount: 0 };
  }
  const groups = rule.column_mapping ?? [];
  if (groups.length === 0) {
    return { kind: "complete", incompleteGroupCount: 0, totalGroupCount: 0 };
  }
  let incomplete = 0;
  for (const g of groups) {
    const filled = slotNames.filter((s) => Boolean(g[s])).length;
    if (filled > 0 && filled < slotNames.length) incomplete++;
  }
  return {
    kind: incomplete === 0 ? "complete" : "incomplete",
    incompleteGroupCount: incomplete,
    totalGroupCount: groups.length,
  };
}

// ---------------------------------------------------------------------------
// Rule-logic disclosure — read-only render of the rule's native definition
// (function + arguments, or a SQL predicate/query). DQX rules don't carry a
// low-code AST or joins/group-by blocks the way dqlake's do, so this shows
// what the backend actually stores instead of approximating dqlake's
// SQL/Low-code toggle.
// ---------------------------------------------------------------------------

// Read-only parameter list — matches the Rules Registry form's own
// "Parameters" presentation (RegistryRuleFormDialog.tsx: muted-foreground
// mono label + value, two-column grid) so a check's non-column arguments
// look the same wherever they're shown, just without the editable inputs.
function RuleParametersView({ parameters }: { parameters: RuleParameter[] }) {
  const { t } = useTranslation();
  if (parameters.length === 0) return null;
  return (
    <div className="space-y-1.5">
      <div className="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">
        {t("rulesRegistry.parametersLabel")}
      </div>
      <div className="grid gap-x-4 gap-y-1.5 sm:grid-cols-2">
        {parameters.map((p) => (
          <div key={p.name} className="flex items-baseline gap-1.5 text-xs">
            <span className="font-mono text-muted-foreground shrink-0">{p.name}:</span>
            <span className="font-mono truncate">{paramValueToRaw(p.value) || "—"}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function RuleLogicBody({ registryRule }: { registryRule: RegistryRuleOut }) {
  const { t } = useTranslation();
  const body = (registryRule.definition.body ?? {}) as Record<string, unknown>;
  const fn = typeof body.function === "string" ? body.function : undefined;
  const args = body.arguments;
  const sql = typeof body.sql_query === "string" ? body.sql_query : undefined;
  const predicate = typeof body.predicate === "string" ? body.predicate : undefined;
  const parameters = registryRule.definition.parameters ?? [];

  if (!fn && !sql && !predicate) {
    return <p className="text-xs italic text-muted-foreground">{t("monitoredTables.ruleLogicUnavailable")}</p>;
  }

  const text = sql ?? predicate ?? `${fn}(${args ? JSON.stringify(args) : ""})`;

  return (
    <div className="space-y-3">
      <pre className="font-mono text-xs whitespace-pre-wrap rounded bg-muted/40 p-3 overflow-x-auto">
        {text}
      </pre>
      {/* Non-column parameters only apply to DQX-native (function-based)
          checks — SQL/predicate rules have no declared `parameters`. */}
      {fn && <RuleParametersView parameters={parameters} />}
    </div>
  );
}

// Exported so the AddRulesDialog map step can render the exact same
// disclosure (name, chevron, read-only rule body) for a not-yet-applied
// rule, instead of re-implementing its own "view rule logic" affordance —
// see AddRulesDialog.tsx's mapping step.
export function RuleLogicDisclosure({
  open,
  onToggle,
  registryRule,
}: {
  open: boolean;
  onToggle: () => void;
  registryRule: RegistryRuleOut | undefined;
}) {
  const { t } = useTranslation();
  return (
    <div className="rounded border">
      <button
        type="button"
        onClick={onToggle}
        className="w-full flex items-center gap-2 px-3 py-2 text-xs font-medium text-left hover:bg-muted/40 transition-colors"
        aria-expanded={open}
      >
        <ChevronDown
          className={cn("h-3.5 w-3.5 text-muted-foreground transition-transform shrink-0", open && "rotate-180")}
          aria-hidden
        />
        <span>{t("monitoredTables.ruleLogicLabel")}</span>
      </button>
      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-200 ease-out",
          open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="overflow-hidden">
          <div className="px-3 pb-3 border-t pt-3">
            {registryRule ? (
              <RuleLogicBody registryRule={registryRule} />
            ) : (
              <p className="text-xs italic text-muted-foreground">{t("monitoredTables.ruleLogicUnavailable")}</p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Version-pin dropdown badge
// ---------------------------------------------------------------------------

function VersionPinDropdown({
  currentVersion,
  pinned,
  onPinChange,
  readonly,
}: {
  currentVersion: number;
  pinned: boolean;
  onPinChange: (value: "latest" | "pinned") => void;
  readonly: boolean;
}) {
  const { t } = useTranslation();
  const label = pinned ? t("monitoredTables.pinnedBadge") : t("monitoredTables.latestBadge");

  if (readonly) {
    return (
      // Fixed min-width (matches dqlake's badge sizing) so the pin badge
      // doesn't reflow the card header when its label length changes.
      <div className="inline-flex items-center justify-end min-w-[140px] shrink-0">
        <Badge variant="outline" className="font-mono text-[10px]">
          v{currentVersion} &middot; {label}
        </Badge>
      </div>
    );
  }

  return (
    <div className="inline-flex items-center justify-end min-w-[140px] shrink-0">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <button type="button" onClick={(e) => e.stopPropagation()} className="focus:outline-none">
            <Badge variant="outline" className="font-mono text-[10px] cursor-pointer hover:bg-muted/60">
              v{currentVersion} &middot; {label} &#x25BE;
            </Badge>
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" onClick={(e) => e.stopPropagation()}>
          <DropdownMenuItem onClick={() => onPinChange("latest")} className="gap-2">
            {!pinned ? <Check className="h-3.5 w-3.5" /> : <span className="inline-block w-3.5" />}
            <span>{t("monitoredTables.pinFollowLatest")}</span>
          </DropdownMenuItem>
          <DropdownMenuItem onClick={() => onPinChange("pinned")} className="gap-2">
            {pinned ? <Check className="h-3.5 w-3.5" /> : <span className="inline-block w-3.5" />}
            <span className="font-mono">{t("monitoredTables.pinVersion", { version: currentVersion })}</span>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Severity override dropdown badge
// ---------------------------------------------------------------------------

function SeverityDropdown({
  severity,
  ruleSeverity,
  severityValues,
  labelDefinitions,
  onSeverityChange,
  readonly,
}: {
  /** Effective severity — `rule.severity_override ?? ruleSeverity`, resolved
   *  by the caller. Always shown as-is: the badge never falls back to a
   *  "no override" placeholder string, matching dqlake's effective-severity
   *  badge. */
  severity: string;
  ruleSeverity: string;
  severityValues: string[];
  labelDefinitions: LabelDefinition[];
  onSeverityChange: (value: string) => void;
  readonly: boolean;
}) {
  const { t } = useTranslation();
  const isOverridden = severity !== ruleSeverity && Boolean(severity);
  const label = severity;
  const color = colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, severity);

  const dot = (
    <span
      className="inline-block w-2 h-2 rounded-full shrink-0"
      style={{ background: color ?? "#888" }}
      aria-hidden
    />
  );

  if (readonly) {
    return (
      // Same fixed-width treatment as the version-pin badge — keeps both
      // badges aligned regardless of severity label length or override state.
      <div className="inline-flex items-center justify-end min-w-[110px] shrink-0">
        <Badge variant="outline" className="text-[10px] gap-1.5">
          {dot}
          {label}
          {isOverridden && <span className="text-muted-foreground ml-0.5">*</span>}
        </Badge>
      </div>
    );
  }

  return (
    <div className="inline-flex items-center justify-end min-w-[110px] shrink-0">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <button type="button" onClick={(e) => e.stopPropagation()} className="focus:outline-none">
            <Badge variant="outline" className="text-[10px] cursor-pointer hover:bg-muted/60 gap-1.5">
              {dot}
              {label}
              {isOverridden && <span className="text-muted-foreground ml-0.5">*</span>} &#x25BE;
            </Badge>
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" onClick={(e) => e.stopPropagation()}>
          {severityValues.map((v) => (
            <DropdownMenuItem
              key={v}
              // Selecting the rule's own default severity clears the
              // override (the "none" sentinel handleSeverityChange already
              // maps to `severity_override: null`) instead of writing the
              // default value back as an explicit override — matching
              // dqlake's `onSeverityChange(isDefault ? null : s.id)`.
              onClick={() => onSeverityChange(v === ruleSeverity ? "none" : v)}
              className="gap-2 py-1.5"
            >
              {severity === v ? <Check className="h-3.5 w-3.5" /> : <span className="inline-block w-3.5" />}
              <span
                className="inline-block w-2 h-2 rounded-full shrink-0"
                style={{ background: colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, v) ?? "#888" }}
              />
              <span>{v}</span>
              {v === ruleSeverity && (
                <span className="text-muted-foreground text-xs">{t("monitoredTables.defaultLabel")}</span>
              )}
            </DropdownMenuItem>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Inline "add mapping" form — replaces the old AddRulesDialog mapping step
// (RuleMappingCard). Rendered inside an already-open RuleConfigCard, either
// because the user clicked MappingChips's "+ Apply to another column"
// button, or automatically right after the rule was newly staged with an
// empty column_mapping (see monitored-tables.$bindingId.tsx's auto-expand
// after Add). Submitting stages a new fully-covering mapping group onto the
// tab's local row list via `onAddGroup` (P16-F) — nothing here writes to
// the network.
// ---------------------------------------------------------------------------

function FamilyBadge({ family }: { family: string }) {
  if (!family) return null;
  return (
    <span className="inline-block rounded bg-muted/60 border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground font-medium uppercase tracking-wide shrink-0">
      {family}
    </span>
  );
}

interface InlineAddMappingFormProps {
  slots: RuleSlot[];
  columns: ColumnOut[];
  excludeColumns: string[];
  onCancel: () => void;
  /** Appends *group* as a new mapping group for this rule — pure local
   *  state mutation on the tab's staged rows, no network call. */
  onAddGroup: (group: Record<string, string>) => void;
}

function InlineAddMappingForm({ slots, columns, excludeColumns, onCancel, onAddGroup }: InlineAddMappingFormProps) {
  const { t } = useTranslation();
  const [draft, setDraft] = useState<Record<string, string | string[]>>({});

  const draftComplete = slots.every((slot) => {
    const v = draft[slot.name];
    if (slot.cardinality === "many") return Array.isArray(v) && v.length > 0;
    return typeof v === "string" && v.length > 0;
  });

  const handleConfirm = () => {
    if (!draftComplete) return;
    const group: Record<string, string> = {};
    for (const slot of slots) {
      const v = draft[slot.name];
      group[slot.name] = Array.isArray(v) ? v.join(",") : (v as string);
    }
    onAddGroup(group);
  };

  return (
    <div className="rounded border border-dashed p-3 space-y-3 bg-muted/20">
      <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        {t("monitoredTables.addMappingInlineTitle")}
      </p>
      <div className="space-y-2">
        {slots.map((slot, slotIdx) => {
          const many = slot.cardinality === "many";
          return (
            <div key={slot.name} className="grid grid-cols-[160px_24px_1fr] items-center gap-3">
              <div className="flex items-center gap-2 min-w-0">
                <span className="font-mono text-xs truncate">{`{{${slot.name}}}`}</span>
                <FamilyBadge family={slot.family} />
              </div>
              <span className="text-muted-foreground text-xs justify-self-center self-center" aria-hidden>
                &rarr;
              </span>
              {many ? (
                <MultiColumnPicker
                  slot={slot}
                  columns={columns}
                  value={(draft[slot.name] as string[] | undefined) ?? []}
                  onChange={(next) => setDraft((d) => ({ ...d, [slot.name]: next }))}
                  excludeColumns={excludeColumns}
                />
              ) : (
                <SingleColumnPicker
                  slot={slot}
                  columns={columns}
                  value={draft[slot.name] as string | undefined}
                  onChange={(v) => setDraft((d) => ({ ...d, [slot.name]: v }))}
                  excludeColumns={excludeColumns}
                  // Auto-open the first slot's picker so the steward lands
                  // straight in it — mirrors dqlake's auto-open of a freshly
                  // added mapping-group's placeholder chip right after
                  // "+ Apply to another column" is clicked.
                  autoOpen={slotIdx === 0}
                />
              )}
            </div>
          );
        })}
      </div>
      {!draftComplete && <p className="text-xs text-amber-600">{t("monitoredTables.mappingIncomplete")}</p>}
      <div className="flex items-center justify-end gap-2">
        <Button variant="outline" size="sm" onClick={onCancel}>
          {t("common.cancel")}
        </Button>
        <Button size="sm" onClick={handleConfirm} disabled={!draftComplete}>
          {t("monitoredTables.applyButton")}
        </Button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main card
// ---------------------------------------------------------------------------

interface RuleConfigCardProps {
  /** Merged display rule for this rule_id — see `mergeRuleRowGroup`: its
   *  `column_mapping` is the concatenation of every underlying applied-rule
   *  row's mapping group for this rule_id. */
  rule: AppliedRuleOut;
  registryRule: RegistryRuleOut | undefined;
  labelDefinitions: LabelDefinition[];
  severityValues: string[];
  canEdit: boolean;
  busy: boolean;
  onPinChange: (value: string) => void;
  onSeverityChange: (value: string) => void;
  onRemove: () => void;
  onJumpToColumn?: (colName: string) => void;
  /** Removes the mapping group (and its owning staged row) at this
   *  combined-mapping index — a pure local `stagedRows` mutation (P16-F). */
  onRemoveMapping?: (groupIdx: number) => void;
  /** Reassigns one slot's column within one mapping group — a pure local
   *  `stagedRows` mutation. Wired straight through to `MappingChips`'
   *  `onChangeGroup`, making every chip clickable/editable. */
  onChangeMapping?: (groupIdx: number, slotName: string, colName: string) => void;
  /** Appends *group* as a brand-new mapping group for this rule (the
   *  "+ Apply to another column" flow) — a pure local `stagedRows`
   *  mutation, or fills in the rule's still-empty first group when it was
   *  staged without one yet. */
  onAddMapping: (group: Record<string, string>) => void;
  /** The table's real columns, threaded down to the inline mapping form's
   *  and editable chips' column pickers. */
  columns: ColumnOut[];
  /** Optional expand override — set by the by-column lens's "jump to rule"
   *  action, or right after a fresh "Add rules" apply, so the target card
   *  opens automatically instead of requiring an extra click. */
  forceOpen?: boolean;
}

export function RuleConfigCard({
  rule,
  registryRule,
  labelDefinitions,
  severityValues,
  canEdit,
  busy,
  onPinChange,
  onSeverityChange,
  onRemove,
  onJumpToColumn,
  onRemoveMapping,
  onChangeMapping,
  onAddMapping,
  columns,
  forceOpen,
}: RuleConfigCardProps) {
  const { t } = useTranslation();
  const [isOpen, setIsOpen] = useState(Boolean(forceOpen));
  const [logicOpen, setLogicOpen] = useState(false);
  const [addingMapping, setAddingMapping] = useState(false);

  const dimension = rule.rule_dimension || "";
  const ruleSeverity = rule.rule_severity || "";
  const effectiveSeverity = rule.severity_override ?? ruleSeverity;
  const slots = registryRule?.definition.slots ?? [];
  const status = computeStatus(rule, slots);
  const incomplete = status.kind === "incomplete";
  const currentVersion = rule.pinned_version ?? registryRule?.version ?? 1;
  const groupCount = (rule.column_mapping ?? []).length;
  const needsFirstMapping = slots.length > 0 && groupCount === 0;

  // The by-column lens's "jump to rule" action, and a freshly-staged rule
  // right after "Add rules", re-render this card with forceOpen=true (see
  // monitored-tables.$bindingId.tsx) — keep it in sync if it flips after
  // mount instead of only honoring it at initial state. When the rule has
  // no mapping groups yet, also auto-open the inline mapping form so the
  // user lands directly on it instead of needing an extra click on
  // "+ Apply to another column".
  useEffect(() => {
    if (forceOpen) {
      setIsOpen(true);
      if (needsFirstMapping) setAddingMapping(true);
    }
  }, [forceOpen, needsFirstMapping]);

  return (
    <div
      id={`rule-card-${rule.rule_id}`}
      className={cn(
        "rounded-lg border mb-2 transition-colors overflow-hidden",
        incomplete && "border-l-yellow-500 border-l-[3px]",
        isOpen && "bg-card",
      )}
    >
      <div className="flex items-center gap-3 px-4 py-3 hover:bg-muted/40">
        <button
          type="button"
          onClick={() => setIsOpen((p) => !p)}
          className="flex items-center gap-3 flex-1 min-w-0 text-left"
          aria-expanded={isOpen}
        >
          {incomplete && <span className="h-2.5 w-2.5 rounded-full bg-yellow-500 shrink-0" aria-hidden />}

          <div className="min-w-0 flex-1">
            <Link
              to="/registry-rules/$ruleId"
              params={{ ruleId: rule.rule_id }}
              target="_blank"
              onClick={(e) => e.stopPropagation()}
              className="font-semibold text-sm leading-snug hover:underline focus:underline focus:outline-none"
            >
              {rule.rule_name || rule.rule_id}
            </Link>
            <div className="flex flex-wrap gap-1 mt-1">
              <TagBadge label={dimension} color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, dimension)} />
            </div>
            {incomplete && (
              <div className="text-xs text-yellow-600 dark:text-yellow-500 leading-snug mt-0.5">
                &#x26A0;{" "}
                {t("monitoredTables.incompleteMappingStatus", {
                  incomplete: status.incompleteGroupCount,
                  count: status.totalGroupCount,
                })}
              </div>
            )}
          </div>
        </button>

        <div className="flex items-center gap-1.5 shrink-0">
          {busy ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
          ) : (
            <>
              <VersionPinDropdown
                currentVersion={currentVersion}
                pinned={rule.pinned_version != null}
                onPinChange={onPinChange}
                readonly={!canEdit}
              />
              <SeverityDropdown
                severity={effectiveSeverity}
                ruleSeverity={ruleSeverity}
                severityValues={severityValues}
                labelDefinitions={labelDefinitions}
                onSeverityChange={onSeverityChange}
                readonly={!canEdit}
              />
            </>
          )}
        </div>

        {canEdit && !busy && (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <button
                type="button"
                onClick={(e) => e.stopPropagation()}
                className="shrink-0 focus:outline-none text-muted-foreground hover:text-foreground"
                aria-label={t("monitoredTables.moreOptionsLabel")}
              >
                <MoreVertical className="h-4 w-4" />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" onClick={(e) => e.stopPropagation()}>
              <DropdownMenuItem
                // `text-destructive`/`focus:text-destructive` render as dark
                // red-on-near-black in the dark theme (`--destructive` is a
                // deliberately dark token meant for text *on top of* a
                // destructive background, not as foreground text on the
                // popover's own background) — low contrast, hard to read.
                // `text-red-600 dark:text-red-400` is the existing app
                // convention for destructive text on a neutral background
                // (see rules.drafts.tsx, profiler.tsx, runs-history.tsx) and
                // keeps good contrast in both themes.
                className="text-red-600 dark:text-red-400 focus:text-red-700 dark:focus:text-red-300 gap-2"
                onClick={onRemove}
              >
                {t("monitoredTables.removeRuleFromMonitorMenuItem")}
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        )}

        <button
          type="button"
          onClick={() => setIsOpen((p) => !p)}
          className="shrink-0 focus:outline-none"
          aria-label={isOpen ? t("monitoredTables.collapseLabel") : t("monitoredTables.expandLabel")}
        >
          <ChevronDown className={cn("h-4 w-4 text-muted-foreground transition-transform", isOpen && "rotate-180")} aria-hidden />
        </button>
      </div>

      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-200 ease-out",
          isOpen ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="overflow-hidden">
          <div className="border-t px-4 py-4 space-y-3">
            <RuleLogicDisclosure open={logicOpen} onToggle={() => setLogicOpen((p) => !p)} registryRule={registryRule} />
            <MappingChips
              columnMapping={rule.column_mapping ?? []}
              slots={slots}
              columns={columns}
              onJumpToColumn={onJumpToColumn}
              onChangeGroup={canEdit ? onChangeMapping : undefined}
              onRemoveGroup={canEdit ? onRemoveMapping : undefined}
              onAddGroup={canEdit && !addingMapping ? () => setAddingMapping(true) : undefined}
            />
            {canEdit && addingMapping && (
              <InlineAddMappingForm
                slots={slots}
                columns={columns}
                excludeColumns={getUsedColumnsForRule(rule)}
                onCancel={() => setAddingMapping(false)}
                onAddGroup={(group) => {
                  setAddingMapping(false);
                  onAddMapping(group);
                }}
              />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

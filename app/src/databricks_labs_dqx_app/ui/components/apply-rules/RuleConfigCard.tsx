// RuleConfigCard — one applied registry rule's card in the by-rule lens.
// Ported 1:1 (structure/interactions) from dqlake's `bindings/RuleConfigCard.tsx`:
// a collapsible card with a chevron header, version-pin + severity-override
// dropdown badges, a "Rule logic" disclosure showing the rule's SQL/native
// definition read-only, and an overflow menu to remove the application.
// All controls STAGE changes on the monitored-table binding — they never
// touch the live checks until the table is published.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "@tanstack/react-router";
import { Check, ChevronDown, Loader2, MoreVertical } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";
import type { AppliedRuleOut, RegistryRuleOut, RuleSlot } from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import { MappingChips } from "./MappingChips";
import { RESERVED_DIMENSION_KEY, RESERVED_SEVERITY_KEY, TagBadge, colorFor } from "./shared";

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

function RuleLogicBody({ registryRule }: { registryRule: RegistryRuleOut }) {
  const { t } = useTranslation();
  const body = (registryRule.definition.body ?? {}) as Record<string, unknown>;
  const fn = typeof body.function === "string" ? body.function : undefined;
  const args = body.arguments;
  const sql = typeof body.sql_query === "string" ? body.sql_query : undefined;
  const predicate = typeof body.predicate === "string" ? body.predicate : undefined;

  if (!fn && !sql && !predicate) {
    return <p className="text-xs italic text-muted-foreground">{t("monitoredTables.ruleLogicUnavailable")}</p>;
  }

  const text = sql ?? predicate ?? `${fn}(${args ? JSON.stringify(args) : ""})`;

  return (
    <pre className="font-mono text-xs whitespace-pre-wrap rounded bg-muted/40 p-3 overflow-x-auto">
      {text}
    </pre>
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
  severity: string;
  ruleSeverity: string;
  severityValues: string[];
  labelDefinitions: LabelDefinition[];
  onSeverityChange: (value: string) => void;
  readonly: boolean;
}) {
  const { t } = useTranslation();
  const isOverridden = severity !== ruleSeverity && Boolean(severity);
  const label = severity || t("monitoredTables.severityOverrideNone");
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
            <DropdownMenuItem key={v} onClick={() => onSeverityChange(v)} className="gap-2 py-1.5">
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
  /** Removes the mapping group (and its owning applied-rule row) at this
   *  combined-mapping index. */
  onRemoveMapping?: (groupIdx: number) => void;
  /** Combined-mapping index of the group currently being removed, if any. */
  busyMappingGroupIdx?: number | null;
  /** Opens the "apply this rule to another column" flow — stages a new
   *  mapping group as its own applied-check entry. */
  onAddMapping?: () => void;
  /** Optional expand override — set by the by-column lens's "jump to rule"
   *  action so the target card opens automatically instead of requiring an
   *  extra click. */
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
  busyMappingGroupIdx = null,
  onAddMapping,
  forceOpen,
}: RuleConfigCardProps) {
  const { t } = useTranslation();
  const [isOpen, setIsOpen] = useState(Boolean(forceOpen));
  const [logicOpen, setLogicOpen] = useState(false);

  // The by-column lens's "jump to rule" action re-renders this card with
  // forceOpen=true (see monitored-tables.$bindingId.tsx) — keep it in sync
  // if it flips after mount instead of only honoring it at initial state.
  useEffect(() => {
    if (forceOpen) setIsOpen(true);
  }, [forceOpen]);

  const dimension = rule.rule_dimension || "";
  const ruleSeverity = rule.rule_severity || "";
  const effectiveSeverity = rule.severity_override ?? ruleSeverity;
  const slots = registryRule?.definition.slots ?? [];
  const status = computeStatus(rule, slots);
  const incomplete = status.kind === "incomplete";
  const currentVersion = rule.pinned_version ?? registryRule?.version ?? 1;

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
                className="text-destructive focus:text-destructive gap-2"
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
              onJumpToColumn={onJumpToColumn}
              onRemoveGroup={canEdit ? onRemoveMapping : undefined}
              busyGroupIdx={busyMappingGroupIdx}
              onAddGroup={canEdit ? onAddMapping : undefined}
            />
          </div>
        </div>
      </div>
    </div>
  );
}

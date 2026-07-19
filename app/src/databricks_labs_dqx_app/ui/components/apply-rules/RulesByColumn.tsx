// RulesByColumn — the "by column" lens for a monitored table's applied
// rules: pivots the same AppliedRuleOut[] so each real table column shows
// which rules (and slots) are checking it, ported 1:1 from dqlake's
// column-centric accordion (databricks-dqwatch RulesByColumn.tsx).
//
// Unlike a naive pivot over `column_mapping`, this renders one card per REAL
// table column (fetched via useGetTableColumns) — including columns with no
// rules applied yet — and gives every column its own "+ Add rule" CTA that
// carries the column's name/family through to the caller so the Add Rules
// dialog can preselect it. This is what makes "+ Add rule" from the
// by-column view actually target the clicked column instead of silently
// doing nothing (or opening a fully-blank dialog with no column context).

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { ChevronDown, Plus } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Pagination } from "@/components/Pagination";
import { cn } from "@/lib/utils";
import { useGetTableColumns, type AppliedRuleOut, type ColumnOut } from "@/lib/api";
import { computeMatchedTagsForSlot } from "@/lib/registry-rule-conversion";
import { familyForType, type ColumnFamily } from "./ColumnPicker";
import { paletteAt } from "./MappingChips";
import { ThresholdPill } from "./ThresholdPill";

// Client-side page size for the by-column lens — column rows are compact
// (single-line headers), so a screenful holds more than the taller by-rule
// cards before the list runs off the bottom (item 51).
const COLUMN_PAGE_SIZE = 15;

export interface ColumnRef {
  name: string;
  family: ColumnFamily;
}

interface RuleEntry {
  ruleId: string;
  ruleName: string;
  slot: string;
  mappingIndex: number;
  /** Governed tags that matched (slot suggestion ∩ column tags). Empty = no match. */
  matchedTags: string[];
  /** This column's per-column threshold override for this rule, or null if not set. */
  columnThreshold: number | null;
  /** Resolved effective default for this column+rule: rule.pass_threshold ?? rule.rule_pass_threshold ?? adminDefault. */
  effectiveDefault: number;
}

interface RulesByColumnProps {
  appliedRules: AppliedRuleOut[];
  tableFqn: string;
  canEdit: boolean;
  onAddRule?: (column: ColumnRef) => void;
  onJumpToRule?: (ruleId: string) => void;
  /** Filter columns by name OR by the name of any rule applied to them
   *  (case-insensitive substring) — shared with the by-rule lens's search
   *  box in the parent toolbar. */
  search?: string;
  /** Controlled open column name — mirrors dqlake's RulesByColumn. When the
   *  parent owns this (e.g. to auto-expand a column right after a rule was
   *  added to it via the "+ Add rule" CTA), pass it down here instead of
   *  letting this component track its own uncontrolled state. */
  openColumn?: string | null;
  /** Fired when a column card is toggled — only used in controlled mode. */
  onOpenColumnChange?: (name: string | null) => void;
  /** Applied governed tags per column for the table, keyed by column name.
   *  From `useGetTableTags`. Used to compute matched governed tag chips for
   *  each rule-entry row. */
  columnTags?: Record<string, string[]>;
  /** Governed slot-tag suggestions per rule, keyed by rule_id.
   *  Each value is `{slotName: [tag, ...]}` parsed from `user_metadata.slot_tags`.
   *  Used alongside `columnTags` to show matched tag chips inline. */
  ruleSlotTagsById?: Map<string, Record<string, string[]>>;
  /** Admin-level default pass threshold — used as the fallback when neither
   *  a per-rule nor registry-rule threshold is set. */
  adminDefault: number;
  /** Called when the user edits a per-column threshold in the pill popover.
   *  `value === null` means "clear the override" (revert to rule default). */
  onColumnThresholdChange?: (ruleId: string, column: string, value: number | null) => void;
  /** Whether the pass-threshold feature is enabled. When false, per-column
   *  threshold pills are hidden. Defaults to true (fail-open). */
  thresholdEnabled?: boolean;
}

function useRulesByColumn(
  appliedRules: AppliedRuleOut[],
  columnTags: Record<string, string[]> | undefined,
  ruleSlotTagsById: Map<string, Record<string, string[]>> | undefined,
  adminDefault: number,
): Map<string, RuleEntry[]> {
  return useMemo(() => {
    const map = new Map<string, RuleEntry[]>();
    // Tracks the next combined mapping-group index per rule_id — mirrors
    // `mergeRuleRowGroup`'s `rows.flatMap(r => r.column_mapping)` ordering
    // so a chip's color dot here matches its color thread in the by-rule
    // lens's MappingChips, even though each applied-rule ROW here only
    // knows its own (always-0) local group index.
    const nextIndexByRuleId = new Map<string, number>();
    for (const rule of appliedRules) {
      const ruleName = rule.rule_name || rule.rule_id;
      const slotTags = ruleSlotTagsById?.get(rule.rule_id) ?? {};
      // Effective default for this rule's per-column pill: per-rule override
      // ?? registry-rule default ?? admin default. Matches Task 4's resolver.
      const effectiveDefault = rule.pass_threshold ?? rule.rule_pass_threshold ?? adminDefault;
      (rule.column_mapping ?? []).forEach((group) => {
        const mappingIndex = nextIndexByRuleId.get(rule.rule_id) ?? 0;
        nextIndexByRuleId.set(rule.rule_id, mappingIndex + 1);
        for (const [slot, column] of Object.entries(group)) {
          if (!column) continue;
          const list = map.get(column) ?? [];
          // Keyed by rule_id (not the row id) — the by-rule lens groups
          // every applied-rule ROW for a rule_id into one card (see
          // `groupAppliedRulesByRuleId`), so this must match the card's
          // `rule-card-${rule_id}` DOM id regardless of which underlying row
          // the clicked column's mapping group came from.
          const matchedTags =
            columnTags ? computeMatchedTagsForSlot(slotTags, columnTags, slot, column) : [];
          // Per-column threshold override for this specific column, or null.
          const columnThreshold = rule.column_pass_thresholds?.[column] ?? null;
          list.push({ ruleId: rule.rule_id, ruleName, slot, mappingIndex, matchedTags, columnThreshold, effectiveDefault });
          map.set(column, list);
        }
      });
    }
    return map;
  }, [appliedRules, columnTags, ruleSlotTagsById, adminDefault]);
}

function FamilyBadge({ family }: { family: ColumnFamily }) {
  return (
    <span className="inline-block rounded bg-muted/60 border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground font-medium uppercase tracking-wide shrink-0">
      {family}
    </span>
  );
}

interface EmptyColumnRowProps {
  column: ColumnOut;
  family: ColumnFamily;
  canEdit: boolean;
  onAddRule?: (column: ColumnRef) => void;
}

function EmptyColumnRow({ column, family, canEdit, onAddRule }: EmptyColumnRowProps) {
  const { t } = useTranslation();
  return (
    <div id={`column-card-${column.name}`} className="rounded-lg border bg-card text-card-foreground flex items-center gap-3 px-4 py-2.5">
      <span className="font-mono text-sm font-semibold">{column.name}</span>
      <FamilyBadge family={family} />
      <span className="text-xs text-muted-foreground">{column.type_name}</span>
      {canEdit && onAddRule ? (
        <Button
          variant="ghost"
          size="sm"
          className="ml-auto h-7 text-xs text-muted-foreground hover:text-foreground px-2"
          onClick={() => onAddRule({ name: column.name, family })}
        >
          <Plus className="h-3.5 w-3.5 mr-1" />
          {t("monitoredTables.addRuleButton")}
        </Button>
      ) : (
        <span className="ml-auto text-xs text-muted-foreground">{t("monitoredTables.columnNoRules")}</span>
      )}
    </div>
  );
}

interface ColumnCardProps {
  column: ColumnOut;
  family: ColumnFamily;
  entries: RuleEntry[];
  isOpen: boolean;
  onToggle: () => void;
  canEdit: boolean;
  onAddRule?: (column: ColumnRef) => void;
  onJumpToRule?: (ruleId: string) => void;
  onColumnThresholdChange?: (ruleId: string, column: string, value: number | null) => void;
  thresholdEnabled?: boolean;
}

function ColumnCard({ column, family, entries, isOpen, onToggle, canEdit, onAddRule, onJumpToRule, onColumnThresholdChange, thresholdEnabled = true }: ColumnCardProps) {
  const { t } = useTranslation();
  return (
    <div id={`column-card-${column.name}`} className="rounded-lg border bg-card text-card-foreground">
      <button
        type="button"
        onClick={onToggle}
        className="w-full flex items-center gap-3 px-4 py-3 hover:bg-muted/40 transition-colors text-left"
        aria-expanded={isOpen}
      >
        <span className="font-mono text-sm font-semibold">{column.name}</span>
        <FamilyBadge family={family} />
        <span className="text-xs text-muted-foreground">{column.type_name}</span>
        <span className="ml-auto text-xs text-muted-foreground mr-2">
          {t("monitoredTables.columnChecksCount", { count: entries.length })}
        </span>
        <ChevronDown
          className={cn("h-3.5 w-3.5 text-muted-foreground transition-transform shrink-0", isOpen && "rotate-180")}
          aria-hidden
        />
      </button>

      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-200 ease-out",
          isOpen ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="overflow-hidden">
          {/* Always rendered (never gated by `isOpen`) — the grid-rows
              transition above needs the content present in both directions
              to animate the collapse, not just the expand (item 25). An
              `{isOpen && ...}` guard here would unmount the content the
              instant the track starts shrinking, so there'd be nothing left
              to visually collapse. */}
          <div className="px-4 py-3 border-t space-y-2">
            {entries.map((entry, i) => (
              <button
                key={`${entry.ruleId}-${entry.slot}-${entry.mappingIndex}-${i}`}
                type="button"
                onClick={() => onJumpToRule?.(entry.ruleId)}
                className="flex items-center gap-2.5 w-full text-left rounded px-1 -mx-1 py-0.5 hover:bg-muted/50 transition-colors"
              >
                <span
                  className={cn(
                    "w-2.5 h-2.5 rounded-full border shrink-0",
                    paletteAt(entry.mappingIndex),
                  )}
                />
                <span className="text-sm font-medium truncate flex-1">{entry.ruleName}</span>
                {/* Trailing group: tags + pill + slot — ml-auto pushes the
                    whole group to the right edge so pills line up across
                    all rows regardless of name length. */}
                <span className="flex items-center gap-1.5 shrink-0 ml-auto">
                  {entry.matchedTags.length > 0 && (
                    <span className="flex items-center gap-1">
                      {entry.matchedTags.map((tag) => (
                        <Badge
                          key={tag}
                          variant="secondary"
                          className="text-[10px] font-mono px-1.5 py-0 h-auto opacity-75"
                        >
                          {tag}
                        </Badge>
                      ))}
                    </span>
                  )}
                  {/* Per-column threshold pill — stopPropagation so clicking
                      the pill/popover doesn't trigger the row's onJumpToRule.
                      Mirror how SeverityDropdown stops propagation in
                      RuleConfigCard. Hidden when the threshold feature is
                      disabled. */}
                  {thresholdEnabled && (
                    <span
                      onClick={(e) => e.stopPropagation()}
                      onKeyDown={(e) => e.stopPropagation()}
                    >
                      <ThresholdPill
                        value={entry.columnThreshold}
                        effectiveDefault={entry.effectiveDefault}
                        onChange={(v) => onColumnThresholdChange?.(entry.ruleId, column.name, v)}
                        readonly={!canEdit}
                        hintOverride={t("monitoredTables.columnThresholdPopoverHint", { pct: entry.effectiveDefault })}
                      />
                    </span>
                  )}
                  <span className="font-mono text-xs text-muted-foreground">{`{{${entry.slot}}}`}</span>
                </span>
              </button>
            ))}

            {canEdit && onAddRule && (
              <div className="pt-1">
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 text-xs text-muted-foreground hover:text-foreground px-2"
                  onClick={() => onAddRule({ name: column.name, family })}
                >
                  <Plus className="h-3.5 w-3.5 mr-1" />
                  {t("monitoredTables.addRuleButton")}
                </Button>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export function RulesByColumn({
  appliedRules,
  tableFqn,
  canEdit,
  onAddRule,
  onJumpToRule,
  search,
  openColumn: controlledOpenColumn,
  onOpenColumnChange,
  columnTags,
  ruleSlotTagsById,
  adminDefault,
  onColumnThresholdChange,
  thresholdEnabled = true,
}: RulesByColumnProps) {
  const { t } = useTranslation();
  const rulesByColumn = useRulesByColumn(appliedRules, columnTags, ruleSlotTagsById, adminDefault);
  const [internalOpenColumn, setInternalOpenColumn] = useState<string | null>(null);
  // 1-indexed page (item 51). Reset to the first page whenever the search
  // narrows the column set so it can't strand the view on an empty page.
  const [page, setPage] = useState(1);
  useEffect(() => {
    setPage(1);
  }, [search]);

  // Support both controlled (parent owns state) and uncontrolled modes —
  // mirrors dqlake's RulesByColumn.
  const isControlled = controlledOpenColumn !== undefined;
  const openColumn = isControlled ? controlledOpenColumn : internalOpenColumn;
  const setOpenColumn = (name: string | null) => {
    if (isControlled) {
      onOpenColumnChange?.(name);
    } else {
      setInternalOpenColumn(name);
    }
  };

  const parts = tableFqn.split(".");
  const columnsQuery = useGetTableColumns(parts[0] ?? "", parts[1] ?? "", parts[2] ?? "", {
    query: { enabled: parts.length === 3 },
  });
  const columns: ColumnOut[] = columnsQuery.data?.data ?? [];

  const visible = useMemo(() => {
    const q = search?.trim().toLowerCase() ?? "";
    if (!q) return columns;
    return columns.filter((col) => {
      if (col.name.toLowerCase().includes(q)) return true;
      const entries = rulesByColumn.get(col.name) ?? [];
      return entries.some((e) => e.ruleName.toLowerCase().includes(q));
    });
  }, [columns, rulesByColumn, search]);

  // Keep an auto-opened column (About-tab deep link, or a card expanded right
  // after a rule was added to it) on-screen by jumping to its page instead of
  // leaving it hidden behind pagination.
  useEffect(() => {
    if (!openColumn) return;
    const idx = visible.findIndex((c) => c.name === openColumn);
    if (idx >= 0) setPage(Math.floor(idx / COLUMN_PAGE_SIZE) + 1);
  }, [openColumn, visible]);

  if (columns.length === 0) {
    return (
      <div className="rounded-lg border border-dashed p-8 text-center text-sm text-muted-foreground">
        {t("monitoredTables.columnLensUnavailable")}
      </div>
    );
  }

  if (visible.length === 0) {
    return (
      <div className="rounded-lg border border-dashed p-8 text-center text-sm text-muted-foreground">
        {t("monitoredTables.noRulesMatchFilter")}
      </div>
    );
  }

  const totalPages = Math.max(1, Math.ceil(visible.length / COLUMN_PAGE_SIZE));
  const safePage = Math.min(page, totalPages);
  const pagedColumns = visible.slice((safePage - 1) * COLUMN_PAGE_SIZE, safePage * COLUMN_PAGE_SIZE);

  return (
    <div className="space-y-2">
      {pagedColumns.map((column) => {
        const family = familyForType(column.type_name);
        const entries = rulesByColumn.get(column.name) ?? [];

        if (entries.length === 0) {
          return (
            <EmptyColumnRow key={column.name} column={column} family={family} canEdit={canEdit} onAddRule={onAddRule} />
          );
        }

        return (
          <ColumnCard
            key={column.name}
            column={column}
            family={family}
            entries={entries}
            isOpen={openColumn === column.name}
            onToggle={() => setOpenColumn(openColumn === column.name ? null : column.name)}
            canEdit={canEdit}
            onAddRule={onAddRule}
            onJumpToRule={onJumpToRule}
            onColumnThresholdChange={onColumnThresholdChange}
            thresholdEnabled={thresholdEnabled}
          />
        );
      })}
      <Pagination
        page={safePage}
        totalItems={visible.length}
        pageSize={COLUMN_PAGE_SIZE}
        onPageChange={setPage}
        className="flex items-center justify-between pt-1"
      />
    </div>
  );
}

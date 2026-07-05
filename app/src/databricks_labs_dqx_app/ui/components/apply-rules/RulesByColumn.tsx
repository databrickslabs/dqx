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

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { ChevronDown, Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useGetTableColumns, type AppliedRuleOut, type ColumnOut } from "@/lib/api";
import { familyForType, type ColumnFamily } from "./ColumnPicker";
import { paletteAt } from "./MappingChips";

export interface ColumnRef {
  name: string;
  family: ColumnFamily;
}

interface RuleEntry {
  ruleId: string;
  ruleName: string;
  slot: string;
  mappingIndex: number;
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
}

function useRulesByColumn(appliedRules: AppliedRuleOut[]): Map<string, RuleEntry[]> {
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
          list.push({ ruleId: rule.rule_id, ruleName, slot, mappingIndex });
          map.set(column, list);
        }
      });
    }
    return map;
  }, [appliedRules]);
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
}

function ColumnCard({ column, family, entries, isOpen, onToggle, canEdit, onAddRule, onJumpToRule }: ColumnCardProps) {
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
          {isOpen && (
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
                  <span className="font-mono text-xs text-muted-foreground shrink-0">{`{{${entry.slot}}}`}</span>
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
          )}
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
}: RulesByColumnProps) {
  const { t } = useTranslation();
  const rulesByColumn = useRulesByColumn(appliedRules);
  const [internalOpenColumn, setInternalOpenColumn] = useState<string | null>(null);

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

  if (columns.length === 0) {
    return (
      <div className="rounded-lg border border-dashed p-8 text-center text-sm text-muted-foreground">
        {t("monitoredTables.columnLensUnavailable")}
      </div>
    );
  }

  const q = search?.trim().toLowerCase() ?? "";
  const visible = q
    ? columns.filter((col) => {
        if (col.name.toLowerCase().includes(q)) return true;
        const entries = rulesByColumn.get(col.name) ?? [];
        return entries.some((e) => e.ruleName.toLowerCase().includes(q));
      })
    : columns;

  if (visible.length === 0) {
    return (
      <div className="rounded-lg border border-dashed p-8 text-center text-sm text-muted-foreground">
        {t("monitoredTables.noRulesMatchFilter")}
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {visible.map((column) => {
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
          />
        );
      })}
    </div>
  );
}

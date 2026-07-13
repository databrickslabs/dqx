// RulesPicker — a selectable, mini variant of RulesTable used inside
// AddRulesDialog's rule-selection step. Ported from dqlake's
// `bindings/RulesPicker.tsx` (itself forked from dqlake's RulesTable): a
// compact table with a search box + Edit Columns control, so picking a rule
// to apply means scanning a real table view instead of a plain search-box
// list of buttons.
//
// Reuses DQX's own column-layout infrastructure (`useColumnLayout` /
// `EditColumnsDropdown`, shared with the main Rules Registry list) rather
// than reimplementing dqlake's hand-rolled dnd-kit column reordering — the
// project's constraint to reuse existing DQX helpers over inventing
// parallel ones takes precedence over copying dqlake's plumbing verbatim.
//
// One deliberate deviation from dqlake's picker, forced by DQX's data model:
//   - No steward filter dropdown: DQX has no rule-stewards listing endpoint
//     analogous to dqlake's `useList_rule_stewardsSuspense`.
//
// Multi-select via a leading checkbox column, mirroring dqlake's picker —
// AddRulesDialog no longer maps columns inline (that moved to the by-rule
// applied-rule card), so applying one rule per pass is no longer a
// constraint and picking several rules to add at once is supported.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { ChevronDown, ChevronLeft, ChevronRight, ChevronUp, Loader2, RotateCcw, Search, Sparkles } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { useColumnLayout, type ColumnLayoutDef } from "@/components/data-table/column-layout";
import { EditColumnsDropdown } from "@/components/data-table/EditColumnsDropdown";
import { formatDateShort } from "@/lib/format-utils";
import { AI_GRADIENT_URL } from "@/lib/ai-style";
import { AuthorKindIcon } from "@/components/rules/AuthorKindIcon";
import {
  RESERVED_DESCRIPTION_KEY,
  RESERVED_DIMENSION_KEY,
  RESERVED_NAME_KEY,
  RESERVED_SEVERITY_KEY,
  TagBadge,
  SeverityBadge,
  colorFor,
  getTag,
  type LabelColorDefinition,
} from "@/components/RegistryRuleBadges";
import type { RegistryRuleOut } from "@/lib/api";

type ColumnKey =
  | "aiAuthorship"
  | "name"
  | "description"
  | "slots"
  | "version"
  | "dimension"
  | "severity"
  | "steward"
  | "createdBy"
  | "updated"
  | "mode";

interface RenderCtx {
  labelDefinitions: LabelColorDefinition[];
  /** Rule ids already applied to the target table — rendered checked +
   *  disabled with an inline "already applied" badge (B2-115). */
  appliedIds: Set<string>;
  /** Translated "Already applied" badge label. */
  appliedLabel: string;
}

interface ColumnDef extends ColumnLayoutDef {
  labelKey: string;
  sortable: boolean;
  headClassName?: string;
  renderHeader(label: string): React.ReactNode;
  renderCell(r: RegistryRuleOut, ctx: RenderCtx): React.ReactNode;
  sortValue(r: RegistryRuleOut): string | number;
}

function TruncatedCell({ text, className }: { text: string; className?: string }) {
  return <span className={cn("block truncate", className)}>{text}</span>;
}

function FamilyBadge({ family }: { family: string }) {
  return (
    <span className="inline-block rounded bg-muted/60 border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground font-medium uppercase tracking-wide shrink-0">
      {family}
    </span>
  );
}

// Picker-specific defaults, mirroring dqlake's PICKER_DEFAULT_VISIBLE: a
// leaner set than the full Rules Registry table since the steward picks
// rules to apply, not by who owns them. Status has no column at all here —
// the caller already filtered the rows to published/approved rules, so
// every row would show the same badge (dqlake hides it for the same reason).
const COLUMNS: Record<ColumnKey, ColumnDef> = {
  aiAuthorship: {
    labelKey: "rulesRegistry.colAiAuthorship",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 56,
    sortable: true,
    resizable: false,
    headClassName: "w-10",
    renderHeader: (label) => (
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="inline-flex">
            <Sparkles className="h-4 w-4" stroke={AI_GRADIENT_URL} aria-label={label} />
          </span>
        </TooltipTrigger>
        <TooltipContent>{label}</TooltipContent>
      </Tooltip>
    ),
    renderCell: (r) => <AuthorKindIcon kind={r.author_kind} />,
    sortValue: (r) => r.author_kind ?? "",
  },
  name: {
    labelKey: "rulesRegistry.colName",
    toggleable: false,
    defaultVisible: true,
    defaultWidth: 240,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r, ctx) => {
      const applied = ctx.appliedIds.has(r.rule_id);
      return (
        <span className="flex items-center gap-1.5 min-w-0">
          <TruncatedCell text={getTag(r, RESERVED_NAME_KEY) || r.rule_id} className="font-medium text-sm" />
          {applied && (
            <span className="shrink-0 rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
              {ctx.appliedLabel}
            </span>
          )}
        </span>
      );
    },
    sortValue: (r) => (getTag(r, RESERVED_NAME_KEY) || r.rule_id).toLowerCase(),
  },
  description: {
    labelKey: "rulesRegistry.colDescription",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 320,
    sortable: true,
    headClassName: "max-w-xs",
    renderHeader: (label) => label,
    renderCell: (r) => (
      <TruncatedCell text={getTag(r, RESERVED_DESCRIPTION_KEY) || "—"} className="text-muted-foreground" />
    ),
    sortValue: (r) => getTag(r, RESERVED_DESCRIPTION_KEY).toLowerCase(),
  },
  slots: {
    labelKey: "rulesRegistry.colSlots",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 140,
    sortable: false,
    resizable: true,
    renderHeader: (label) => label,
    renderCell: (r) => {
      const slots = r.definition.slots ?? [];
      if (slots.length === 0) return <span className="text-muted-foreground">—</span>;
      return (
        <div className="flex flex-wrap gap-1">
          {slots.map((slot, i) => (
            <FamilyBadge key={`${slot.name}-${i}`} family={slot.family} />
          ))}
        </div>
      );
    },
    sortValue: (r) => (r.definition.slots ?? []).length,
  },
  version: {
    labelKey: "rulesRegistry.colVersion",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 90,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <span className="text-xs text-muted-foreground font-mono">v{r.version}</span>,
    sortValue: (r) => r.version,
  },
  dimension: {
    labelKey: "rulesRegistry.colDimension",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 130,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r, ctx) => {
      const dimension = getTag(r, RESERVED_DIMENSION_KEY);
      return <TagBadge label={dimension} color={colorFor(ctx.labelDefinitions, RESERVED_DIMENSION_KEY, dimension)} />;
    },
    sortValue: (r) => getTag(r, RESERVED_DIMENSION_KEY).toLowerCase(),
  },
  severity: {
    labelKey: "rulesRegistry.colSeverity",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 120,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r, ctx) => {
      const severity = getTag(r, RESERVED_SEVERITY_KEY);
      return <SeverityBadge severity={severity} color={colorFor(ctx.labelDefinitions, RESERVED_SEVERITY_KEY, severity)} />;
    },
    sortValue: (r) => getTag(r, RESERVED_SEVERITY_KEY).toLowerCase(),
  },
  steward: {
    labelKey: "rulesRegistry.colSteward",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 180,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <TruncatedCell text={r.steward || "—"} className="text-muted-foreground" />,
    sortValue: (r) => (r.steward ?? "").toLowerCase(),
  },
  createdBy: {
    labelKey: "rulesRegistry.colCreatedBy",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 180,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <TruncatedCell text={r.created_by || "—"} className="text-muted-foreground" />,
    sortValue: (r) => (r.created_by ?? "").toLowerCase(),
  },
  updated: {
    labelKey: "rulesRegistry.colUpdated",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 140,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => (
      <span className="text-xs text-muted-foreground" title={r.updated_at ?? undefined}>
        {r.updated_at ? formatDateShort(r.updated_at) : "—"}
      </span>
    ),
    sortValue: (r) => r.updated_at ?? "",
  },
  mode: {
    labelKey: "rulesRegistry.colMode",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 100,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <span className="text-xs text-muted-foreground font-mono">{r.mode}</span>,
    sortValue: (r) => r.mode,
  },
};

const DEFAULT_ORDER: ColumnKey[] = [
  "slots",
  "aiAuthorship",
  "name",
  "description",
  "version",
  "dimension",
  "severity",
  "steward",
  "createdBy",
  "updated",
  "mode",
];

// Separate localStorage key so the picker layout doesn't fight the main
// Rules Registry table's layout — mirrors dqlake's dedicated
// `dqlake.rulespicker.layout` key.
const LS_KEY_LAYOUT = "dqx.rulesPicker.layout";

// Picker pages at 10 so the table fits comfortably inside the dialog
// without dominating the viewport — same page size dqlake uses.
const PAGE_SIZE = 10;

export interface RulesPickerProps {
  rules: RegistryRuleOut[];
  labelDefinitions: LabelColorDefinition[];
  /** Currently checked rule ids. */
  selectedIds: Set<string>;
  /** Rule ids already applied to the target table. These rows render
   *  checked + disabled (can't be toggled or re-picked) with an inline
   *  "Already applied" badge (B2-115). */
  appliedIds?: Set<string>;
  /** Fired when a row's checkbox (or the row itself) is toggled. */
  onToggle: (rule: RegistryRuleOut) => void;
  /** The published-rules query is still loading (no data yet). Shown as a
   *  loading row rather than the misleading "no rules" empty state. */
  isLoading?: boolean;
  /** The published-rules query errored (e.g. a transient auth blip). Shown as
   *  an explicit failure + retry rather than a false "no rules match" — the
   *  root cause of A3, where an empty `rules` from a failed fetch read as
   *  "zero rules available" for a column that actually has valid rules. */
  isError?: boolean;
  /** Refetch the published-rules query (wired to the error-state retry). */
  onRetry?: () => void;
}

export function RulesPicker({ rules, labelDefinitions, selectedIds, appliedIds, onToggle, isLoading, isError, onRetry }: RulesPickerProps) {
  const { t } = useTranslation();
  const applied = useMemo(() => appliedIds ?? new Set<string>(), [appliedIds]);
  const appliedLabel = t("monitoredTables.ruleAlreadyApplied");
  const ctx = useMemo<RenderCtx>(
    () => ({ labelDefinitions, appliedIds: applied, appliedLabel }),
    [labelDefinitions, applied, appliedLabel],
  );
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const [sortKey, setSortKey] = useState<ColumnKey | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc" | null>(null);

  const { colOrder, colWidths, visibleKeys, toggleColumn, handleDragEnd, sensors, onResizeStart } =
    useColumnLayout<ColumnKey>({
      storageKey: LS_KEY_LAYOUT,
      defaultOrder: DEFAULT_ORDER,
      columns: COLUMNS as Record<ColumnKey, ColumnLayoutDef>,
    });

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return rules;
    return rules.filter((r) => {
      const name = getTag(r, RESERVED_NAME_KEY).toLowerCase();
      return name.includes(q) || r.rule_id.toLowerCase().includes(q);
    });
  }, [rules, search]);

  const sorted = useMemo(() => {
    if (!sortKey || !sortDir) return filtered;
    const def = COLUMNS[sortKey];
    const copy = [...filtered];
    copy.sort((a, b) => {
      const av = def.sortValue(a);
      const bv = def.sortValue(b);
      if (av < bv) return sortDir === "asc" ? -1 : 1;
      if (av > bv) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
    return copy;
  }, [filtered, sortKey, sortDir]);

  useEffect(() => setPage(0), [search]);

  const totalPages = Math.max(1, Math.ceil(sorted.length / PAGE_SIZE));
  const pageRows = sorted.slice(page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE);

  function handleHeaderClick(key: ColumnKey) {
    if (!COLUMNS[key].sortable) return;
    if (sortKey !== key) {
      setSortKey(key);
      setSortDir("asc");
      return;
    }
    if (sortDir === "asc") {
      setSortDir("desc");
      return;
    }
    setSortKey(null);
    setSortDir(null);
  }

  const totalWidth = visibleKeys.reduce((acc, k) => acc + (colWidths[k] ?? COLUMNS[k].defaultWidth), 0);
  const visibleColCount = visibleKeys.length;

  return (
    <div className="min-w-0 space-y-3">
      <div className="flex items-center gap-2 flex-wrap">
        <div className="relative max-w-xs">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
          <Input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder={t("monitoredTables.searchRulesPlaceholder")}
            className="pl-7 h-8 text-xs"
          />
        </div>

        <EditColumnsDropdown
          order={colOrder}
          labelOf={(key) => t(COLUMNS[key].labelKey)}
          toggleableOf={(key) => COLUMNS[key].toggleable}
          isChecked={(key) => visibleKeys.includes(key)}
          onToggle={toggleColumn}
          onDragEnd={handleDragEnd}
          sensors={sensors}
        />
      </div>

      {/* Single scroll owner for both axes. shadcn's `Table` wraps its own
          `<table>` in a second `overflow-x-auto` container (`data-slot=
          "table-container"`) — nested inside THIS div, that inner container
          becomes the actual horizontal-scroll owner (it's the innermost
          ancestor with both overflow-x-auto and a width narrower than the
          table), while this outer div (with the visible max-height) owns
          only the vertical scroll. The result: the horizontal scrollbar
          renders at the bottom of the inner container's full (unclamped)
          content height — below every row — instead of hugging the visible
          viewport (item 32). `containerClassName="overflow-visible"` turns
          the inner container's horizontal overflow off so this outer div is
          the only element that scrolls, in both directions. */}
      <div className="overflow-x-auto min-h-[20rem] max-h-[26rem] overflow-y-auto border rounded-md">
        <Table
          containerClassName="overflow-visible"
          className="table-fixed"
          style={{ width: totalWidth + 36, minWidth: totalWidth + 36 }}
        >
          <colgroup>
            <col style={{ width: 36 }} />
            {visibleKeys.map((k) => (
              <col key={k} style={{ width: colWidths[k] ?? COLUMNS[k].defaultWidth }} />
            ))}
          </colgroup>
          <TableHeader>
            <TableRow>
              <TableHead className="w-9 px-2" />
              {visibleKeys.map((k) => {
                const def = COLUMNS[k];
                const width = colWidths[k] ?? def.defaultWidth;
                const isSorted = sortKey === k && sortDir !== null;
                const isResizable = def.resizable !== false;
                const label = t(def.labelKey);
                return (
                  <TableHead
                    key={k}
                    className={cn(
                      "relative h-10 px-2",
                      def.headClassName,
                      def.sortable && "cursor-pointer select-none",
                    )}
                    style={{ width, minWidth: width, maxWidth: width }}
                    onClick={def.sortable ? () => handleHeaderClick(k) : undefined}
                    aria-sort={isSorted ? (sortDir === "asc" ? "ascending" : "descending") : undefined}
                  >
                    <span className="inline-flex items-center gap-1">
                      {def.renderHeader(label)}
                      {isSorted &&
                        (sortDir === "asc" ? (
                          <ChevronUp className="h-3 w-3" aria-hidden />
                        ) : (
                          <ChevronDown className="h-3 w-3" aria-hidden />
                        ))}
                    </span>
                    {isResizable && (
                      <span
                        role="separator"
                        aria-orientation="vertical"
                        className="absolute right-0 top-0 h-full w-1 cursor-col-resize select-none hover:bg-border"
                        onMouseDown={(e) => onResizeStart(k, e)}
                        onClick={(e) => e.stopPropagation()}
                      />
                    )}
                  </TableHead>
                );
              })}
            </TableRow>
          </TableHeader>
          <TableBody>
            {pageRows.map((r) => {
              const name = getTag(r, RESERVED_NAME_KEY) || r.rule_id;
              const isApplied = applied.has(r.rule_id);
              // Already-applied rules render checked + disabled so they can't
              // be re-picked (B2-115); the checkbox reflects that visually.
              const checked = isApplied || selectedIds.has(r.rule_id);
              const rowLabel = isApplied
                ? `${t("monitoredTables.selectRuleLabel", { name })} — ${appliedLabel}`
                : t("monitoredTables.selectRuleLabel", { name });
              return (
                <TableRow
                  key={r.rule_id}
                  className={cn(
                    isApplied ? "cursor-not-allowed opacity-60" : "cursor-pointer hover:bg-muted/50",
                  )}
                  onClick={isApplied ? undefined : () => onToggle(r)}
                  aria-label={rowLabel}
                  aria-disabled={isApplied || undefined}
                  data-state={checked ? "selected" : undefined}
                >
                  <TableCell className="w-9 px-2" onClick={(e) => e.stopPropagation()}>
                    <Checkbox
                      checked={checked}
                      disabled={isApplied}
                      onCheckedChange={() => onToggle(r)}
                      aria-label={rowLabel}
                    />
                  </TableCell>
                  {visibleKeys.map((k) => {
                    const width = colWidths[k] ?? COLUMNS[k].defaultWidth;
                    return (
                      <TableCell key={k} style={{ width, minWidth: width, maxWidth: width }} className="overflow-hidden p-2">
                        {COLUMNS[k].renderCell(r, ctx)}
                      </TableCell>
                    );
                  })}
                </TableRow>
              );
            })}
            {sorted.length === 0 && (
              <TableRow>
                <TableCell colSpan={visibleColCount + 1} className="text-center text-muted-foreground py-8">
                  {isLoading ? (
                    <span className="inline-flex items-center gap-2">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      {t("common.loading")}
                    </span>
                  ) : isError ? (
                    <span className="inline-flex items-center gap-2">
                      {t("common.loadFailed")}
                      {onRetry && (
                        <Button variant="outline" size="sm" className="gap-1.5 h-7" onClick={onRetry}>
                          <RotateCcw className="h-3 w-3" />
                          {t("common.retry")}
                        </Button>
                      )}
                    </span>
                  ) : (
                    t("monitoredTables.noPublishedRules")
                  )}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      {sorted.length > PAGE_SIZE && (
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>{t("monitoredTables.rulesPickerRuleCount", { count: sorted.length })}</span>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              disabled={page === 0}
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              aria-label={t("monitoredTables.previousPageLabel")}
            >
              <ChevronLeft className="h-3.5 w-3.5" />
            </Button>
            <span>
              {page + 1} / {totalPages}
            </span>
            <Button
              variant="outline"
              size="sm"
              disabled={page >= totalPages - 1}
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              aria-label={t("monitoredTables.nextPageLabel")}
            >
              <ChevronRight className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}

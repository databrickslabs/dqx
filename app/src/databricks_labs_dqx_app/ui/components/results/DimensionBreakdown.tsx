import { useRef, useState } from "react";
import type * as React from "react";
import { useTranslation } from "react-i18next";
import { ArrowDown, ArrowUp, ChevronDown, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { CollapseRegion } from "./CollapseRegion";

/**
 * Client-side page slice for the breakdown table. Without *pageSize* the
 * table is unpaginated (every row on one "page" — dqlake behaviour). The
 * page is clamped so a shrink (filter/search change) can never strand the
 * view on an out-of-range page — the same idiom as FailingRecordsTable's
 * pager. Exported for the pager-math tests.
 */
export function paginateRows<T>(
  rows: T[],
  page: number,
  pageSize?: number,
): { pageRows: T[]; safePage: number; totalPages: number } {
  if (!pageSize || pageSize <= 0 || rows.length <= pageSize) {
    return { pageRows: rows, safePage: 0, totalPages: 1 };
  }
  const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
  const safePage = Math.min(Math.max(page, 0), totalPages - 1);
  const start = safePage * pageSize;
  return { pageRows: rows.slice(start, start + pageSize), safePage, totalPages };
}

/** A label that truncates with an ellipsis and reveals the full text in a
 *  tooltip — but only when it's actually clipped (re-checked on hover, the
 *  only time it matters). Mirrors the TruncatedCell used in the rules
 *  registry / tables / data-product tables. */
export function TruncatedText({ text, className }: { text: string; className?: string }) {
  const ref = useRef<HTMLSpanElement>(null);
  const [overflow, setOverflow] = useState(false);
  const checkOverflow = () => {
    const el = ref.current;
    if (el) setOverflow(el.scrollWidth > el.clientWidth);
  };
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          ref={ref}
          className={cn("block truncate", className)}
          onPointerEnter={checkOverflow}
        >
          {text}
        </span>
      </TooltipTrigger>
      {overflow && (
        <TooltipContent side="top" className="max-w-md break-words">
          {text}
        </TooltipContent>
      )}
    </Tooltip>
  );
}

export type BreakdownRow = {
  label: string | null;
  /** The facet VALUE clicking this row toggles (and that `selected` is
   *  matched against); defaults to the label. The By rule box passes the
   *  row's registry rule_id here so the filter follows the rule's stable
   *  identity across renames while the label shows the newest name. */
  value?: string | null;
  pass_rate: number | null;
  failed_tests: number | null;
  rule_count?: number | null;
  check_count?: number | null;
  total_tests?: number | null;
};

type SortKey =
  | "label"
  | "pass_rate"
  | "failed_tests"
  | "rule_count"
  | "check_count"
  | "total_tests";
type SortState = { key: SortKey; dir: "asc" | "desc" } | null;

/** Compare two rows on a key; nulls always sort last regardless of direction. */
function compareRows(a: BreakdownRow, b: BreakdownRow, key: SortKey): number {
  const av = a[key];
  const bv = b[key];
  const an = av == null;
  const bn = bv == null;
  if (an && bn) return 0;
  if (an) return 1;
  if (bn) return -1;
  if (typeof av === "string" && typeof bv === "string") {
    return av.localeCompare(bv);
  }
  return (av as number) - (bv as number);
}

/**
 * Presentational breakdown table: one row per group, showing pass rate,
 * failed count, distinct rules and total checks. Headers cycle a tri-state
 * client-side sort (asc -> desc -> default order). Rows are clickable filters
 * when `onSelect` is given; `selected` marks the active (multi-select) rows.
 * No data fetching — callers pass the rows in.
 */
export function DimensionBreakdown({
  title,
  rows,
  colorMap,
  onSelect,
  selected,
  valueHeader,
  headerRight,
  mutedLabels,
  loading,
  collapsed,
  onToggleCollapse,
  countMode = "rules",
  emptyText,
  defaultSort,
  pageSize,
  rowLink,
  renderLabel,
}: {
  title: string;
  rows: Array<BreakdownRow>;
  /** Optional label → hex colour map. When a row's label has a colour,
   *  a small swatch is shown next to it. */
  colorMap?: Record<string, string>;
  /** When provided, each labelled row becomes clickable and fires this
   *  with the row's facet value (its `value`, defaulting to the label —
   *  the parent toggles the filter). */
  onSelect?: (value: string) => void;
  /** Facet values currently active as filters (matched against each row's
   *  `value ?? label`); marks matching rows aria-pressed. */
  selected?: string[];
  /** Header text for the first (value) column. Defaults to "Label". */
  valueHeader?: string;
  /** Rendered at the top-right of the box, inline with the title. */
  headerRight?: React.ReactNode;
  /** Labels to render muted/thin ("All" mode: base rows that fell outside the
   *  active facet filter). Their numbers still show, dimmed. */
  mutedLabels?: string[];
  /** When true, dim the table and show a small centred spinner over it (a
   *  background refetch is in flight for this box only). */
  loading?: boolean;
  /** When `onToggleCollapse` is provided, a chevron follows the title and the
   *  table body animates open/closed by `collapsed`. */
  collapsed?: boolean;
  onToggleCollapse?: () => void;
  /** First numeric column: "rules" (# Rules, default) or "checks" (# Checks).
   *  By Rule uses "checks" — its # Rules is always 1, so checks is the useful
   *  count there. */
  countMode?: "rules" | "checks";
  /** Message shown when there are no rows. Defaults to "No data yet.". */
  emptyText?: string;
  /** Order to apply when the user has NOT chosen a sort (no header clicked).
   *  Once the user clicks a header their choice takes precedence, and clicking
   *  back to the default state reverts to this order. Persists across refetches
   *  and row swaps. Defaults to incoming (API) order when omitted. */
  defaultSort?: { key: SortKey; dir: "asc" | "desc" };
  /** Client-side page size. When set and the (sorted) rows exceed it, the
   *  table paginates with the compact Prev/Next pager (FailingRecordsTable's
   *  idiom). Unset = unpaginated (dqlake behaviour). */
  pageSize?: number;
  /** Optional per-row link node (e.g. an icon-link to the row's detail
   *  page), rendered after the label. The node must handle its own click
   *  (row clicks still fire `onSelect`; wrap with stopPropagation). */
  rowLink?: (label: string) => React.ReactNode;
  /** Optional replacement for the label text itself — e.g. wrap the name in a
   *  navigating `<Link>`. Given the row's label and facet value, returns the
   *  node to render in place of the default truncated text, or null to fall
   *  back to it. The returned node MUST stopPropagation on click so the name
   *  navigates while a click elsewhere on the row still toggles the facet. */
  renderLabel?: (label: string, value: string | null) => React.ReactNode;
}) {
  const { t } = useTranslation();
  const [sort, setSort] = useState<SortState>(null);
  // Client-side pagination over the sorted rows (only when pageSize is set).
  const [page, setPage] = useState(0);

  const displayRows = rows;
  const mutedSet = new Set(mutedLabels ?? []);

  // Numeric columns start descending (highest first is the useful default);
  // the label column starts ascending. Click cycle: first dir -> opposite ->
  // default order.
  const cycle = (key: SortKey) =>
    setSort((cur) => {
      const first: "asc" | "desc" = key === "label" ? "asc" : "desc";
      if (!cur || cur.key !== key) return { key, dir: first };
      if (cur.dir === first) return { key, dir: first === "asc" ? "desc" : "asc" };
      return null; // back to default order
    });

  // The user's explicit click wins; otherwise fall back to the caller's
  // defaultSort; otherwise incoming (API) order.
  const effectiveSort = sort ?? defaultSort ?? null;
  const sortedRows = effectiveSort
    ? [...displayRows].sort((a, b) => {
        const c = compareRows(a, b, effectiveSort.key);
        return effectiveSort.dir === "asc" ? c : -c;
      })
    : displayRows;

  // Slice the current page out of the full sorted set; the clamp keeps the
  // page in range when a refetch/search shrinks the rows.
  const { pageRows, safePage, totalPages } = paginateRows(sortedRows, page, pageSize);

  const selectedSet = new Set(selected ?? []);

  const sortIcon = (key: SortKey) => {
    if (!sort || sort.key !== key) return null;
    return sort.dir === "asc" ? (
      <ArrowUp className="ml-1 inline h-3 w-3" />
    ) : (
      <ArrowDown className="ml-1 inline h-3 w-3" />
    );
  };

  const headerCell = (key: SortKey, text: string, align: "left" | "right") => (
    <th
      className={`${align === "left" ? "text-left truncate max-w-0" : "text-right whitespace-nowrap"} px-3 py-2 uppercase tracking-wide text-[10px] text-muted-foreground`}
    >
      <button
        type="button"
        onClick={() => cycle(key)}
        className={`inline-flex max-w-full items-center uppercase tracking-wide hover:text-foreground ${align === "right" ? "flex-row-reverse" : ""}`}
      >
        <span className={align === "left" ? "truncate" : "whitespace-nowrap"}>
          {text}
        </span>
        {sortIcon(key)}
      </button>
    </th>
  );

  const collapsible = onToggleCollapse != null;
  const titleEl = collapsible ? (
    <button
      type="button"
      onClick={onToggleCollapse}
      aria-expanded={!collapsed}
      className="group flex items-center gap-1.5 text-left"
    >
      <span className="text-xs uppercase tracking-wide text-muted-foreground">
        {title}
      </span>
      <ChevronDown
        className={`h-4 w-4 shrink-0 text-muted-foreground transition-transform ${
          collapsed ? "-rotate-90" : ""
        }`}
      />
    </button>
  ) : (
    <div className="text-xs uppercase tracking-wide text-muted-foreground">
      {title}
    </div>
  );

  const tableBody =
    displayRows.length === 0 ? (
      <p className="text-sm text-muted-foreground">
        {emptyText ?? t("resultsUi.noDataYet")}
      </p>
    ) : (
      <div className="space-y-2">
        <div className="relative rounded-md border overflow-x-auto">
          {loading && (
            <div className="pointer-events-none absolute inset-0 z-10 flex items-center justify-center">
              <Loader2
                className="h-5 w-5 animate-spin text-muted-foreground"
                aria-label={t("resultsUi.updatingAria")}
              />
            </div>
          )}
          <table
            className={`w-full table-fixed text-xs ${loading ? "opacity-60" : ""}`}
          >
            <colgroup>
              <col style={{ width: "auto" }} />
              <col style={{ width: "88px" }} />
              <col style={{ width: "88px" }} />
              <col style={{ width: "88px" }} />
              <col style={{ width: "88px" }} />
            </colgroup>
            <thead className="bg-muted/30 border-b">
              <tr>
                {headerCell("label", valueHeader ?? t("resultsUi.labelHeader"), "left")}
                {countMode === "checks"
                  ? headerCell("check_count", t("resultsUi.checksHeader"), "right")
                  : headerCell("rule_count", t("resultsUi.rulesHeader"), "right")}
                {headerCell("failed_tests", t("resultsUi.failedTestsHeader"), "right")}
                {headerCell("total_tests", t("resultsUi.totalTestsHeader"), "right")}
                {headerCell("pass_rate", t("resultsUi.scoreHeader"), "right")}
              </tr>
            </thead>
            <tbody>
              {pageRows.map((r, i) => {
                // The facet value the row toggles/matches on — the rule box
                // passes the registry rule_id; every other box defaults to
                // the label.
                const facetValue = r.value ?? r.label;
                const isSelected = facetValue != null && selectedSet.has(facetValue);
                // A muted row ("All" mode: a base row excluded by the active
                // facet filter): render it dimmed + thinner so it reads as
                // out-of-scope, not a live result.
                const isMuted = r.label != null && mutedSet.has(r.label);
                return (
                  <tr
                    key={i}
                    className={`border-b last:border-b-0${
                      onSelect ? " cursor-pointer hover:bg-muted/40" : ""
                    }${isSelected ? " bg-muted/60" : ""}${
                      isMuted ? " text-muted-foreground opacity-60 font-light" : ""
                    }`}
                    {...(onSelect
                      ? {
                          role: "button",
                          "aria-pressed": isSelected,
                          onClick: () => facetValue != null && onSelect(facetValue),
                        }
                      : {})}
                  >
                    <td className="px-3 py-2 max-w-0">
                      {r.label == null ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        <span className="flex items-center gap-2">
                          {colorMap?.[r.label] && (
                            <span
                              className="inline-block h-2.5 w-2.5 rounded-full shrink-0"
                              style={{ backgroundColor: colorMap[r.label] }}
                            />
                          )}
                          {renderLabel?.(r.label, facetValue) ?? (
                            <TruncatedText text={r.label} className="min-w-0" />
                          )}
                          {rowLink?.(r.label)}
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {countMode === "checks"
                        ? (r.check_count == null ? "—" : r.check_count)
                        : (r.rule_count == null ? "—" : r.rule_count)}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {r.pass_rate == null && r.failed_tests == null
                        ? "—"
                        : (r.failed_tests ?? 0)}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {r.total_tests == null ? "—" : r.total_tests}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {r.pass_rate == null
                        ? "—"
                        : `${(r.pass_rate * 100).toFixed(1)}%`}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {totalPages > 1 && (
          <div className="flex items-center justify-end gap-2 text-xs text-muted-foreground">
            <Button
              variant="outline"
              size="sm"
              className="h-7"
              disabled={safePage <= 0}
              onClick={() => setPage(safePage - 1)}
            >
              {t("resultsUi.prevPage")}
            </Button>
            <span>
              {t("resultsUi.pageOf", { page: safePage + 1, pages: totalPages })}
            </span>
            <Button
              variant="outline"
              size="sm"
              className="h-7"
              disabled={safePage >= totalPages - 1}
              onClick={() => setPage(safePage + 1)}
            >
              {t("resultsUi.nextPage")}
            </Button>
          </div>
        )}
      </div>
      );

  return (
    <TooltipProvider delayDuration={200}>
      <div className="space-y-2">
        <div className="flex items-center justify-between gap-2 min-h-9">
          {titleEl}
          {/* The search box (and any other headerRight content) only makes
              sense while the table is visible; hide it when collapsed so the
              header is just the title + chevron. */}
          {!collapsed && headerRight}
        </div>
        {collapsible ? (
          <CollapseRegion open={!collapsed}>{tableBody}</CollapseRegion>
        ) : (
          tableBody
        )}
      </div>
    </TooltipProvider>
  );
}

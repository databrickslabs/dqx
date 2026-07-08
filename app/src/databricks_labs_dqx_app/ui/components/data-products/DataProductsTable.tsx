import { useRef, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Check, ChevronDown, ChevronUp, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { useColumnLayout, type ColumnLayoutDef } from "@/components/data-table/column-layout";
import { EditColumnsDropdown } from "@/components/data-table/EditColumnsDropdown";
import { RelativeTimeCell } from "@/components/data-table/RelativeTimeCell";
import type { DataProductOut } from "@/lib/api";

/** Column keys that carry a comparable value and can drive client sort.
 *  NO DQ Score column — dqlake's `dqScore` column is intentionally omitted
 *  (design spec §8: no quality-score dashboard in this port). */
export type DataProductsSortKey =
  | "name"
  | "description"
  | "status"
  | "steward"
  | "tables"
  | "rules"
  | "checks"
  | "lastRun"
  | "schedule";

interface ColumnDef {
  labelKey: string;
  toggleable: boolean;
  defaultVisible: boolean;
  defaultWidth: number;
  sortable: boolean;
  resizable?: boolean;
  headClassName?: string;
  renderHeader(label: string): ReactNode;
  renderCell(p: DataProductOut): ReactNode;
}

/** Renders text with a tooltip that only appears when the text is actually
 *  clipped — shared pattern with RulesTable/MonitoredTablesTable's
 *  TruncatedCell, ported from dqlake's `DataProductsTable`. */
function TruncatedCell({
  text,
  className,
  tooltipText,
}: {
  text: string;
  className?: string;
  tooltipText?: string;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  const [overflow, setOverflow] = useState(false);

  const checkOverflow = () => {
    const el = ref.current;
    if (!el) return;
    setOverflow(el.scrollWidth > el.clientWidth);
  };

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span ref={ref} className={cn("block truncate", className)} onPointerEnter={checkOverflow}>
          {text}
        </span>
      </TooltipTrigger>
      {overflow && (
        <TooltipContent side="top" className="max-w-md text-wrap break-words text-left">
          {tooltipText ?? text}
        </TooltipContent>
      )}
    </Tooltip>
  );
}

/** Status badge for a product's `display_status` ('draft' | 'modified' |
 *  'published' — dqlake display logic, see backend `data_product_service.
 *  display_status`). */
function DataProductStatusBadge({ status }: { status: string }) {
  const { t } = useTranslation();
  switch (status) {
    case "published":
      return <Badge variant="default" className="text-[10px]">{t("dataProducts.statusPublished")}</Badge>;
    case "modified":
      return (
        <Badge variant="outline" className="text-[10px] border-amber-500 text-amber-600">
          {t("dataProducts.statusModified")}
        </Badge>
      );
    default:
      return <Badge variant="secondary" className="text-[10px]">{t("dataProducts.statusDraft")}</Badge>;
  }
}

/** "# Tables" cell — runnable count, plus a localized "(N not ready)" hint
 *  when some members aren't yet approved/versioned. */
function TablesCell({ product }: { product: DataProductOut }) {
  const { t } = useTranslation();
  const member = product.member_count ?? 0;
  const runnable = product.runnable_count ?? 0;
  const notReady = member - runnable;
  return (
    <span className="tabular-nums">
      {runnable}
      {notReady > 0 && (
        <span className="text-muted-foreground"> {t("dataProducts.notReadySuffix", { count: notReady })}</span>
      )}
    </span>
  );
}

/** Schedule cell — check/x icon, same treatment as dqlake's DataProductsTable. */
function ScheduleCell({ product }: { product: DataProductOut }) {
  const { t } = useTranslation();
  const scheduled = Boolean(product.schedule_cron);
  return scheduled ? (
    <span title={t("dataProducts.scheduledTooltip")} aria-label={t("dataProducts.scheduledTooltip")} className="inline-flex">
      <Check className="h-4 w-4 text-green-600" aria-hidden />
    </span>
  ) : (
    <span title={t("dataProducts.onDemandTooltip")} aria-label={t("dataProducts.onDemandTooltip")} className="inline-flex">
      <X className="h-4 w-4 text-muted-foreground" aria-hidden />
    </span>
  );
}

/** Sums a per-member counter across a product's resolved members. The list
 *  endpoint always returns `members` fully populated (see
 *  `DataProductService.list_products`), so no extra fetch is needed. */
function sumMembers(p: DataProductOut, pick: (rulesCount: number, checksCount: number) => number): number {
  return (p.members ?? []).reduce((acc, m) => acc + pick(m.rules_count, m.checks_count), 0);
}

const COLUMNS: Record<DataProductsSortKey, ColumnDef> = {
  name: {
    labelKey: "dataProducts.colName",
    toggleable: false,
    defaultVisible: true,
    defaultWidth: 220,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (p) => <TruncatedCell text={p.name} className="font-medium" />,
  },
  description: {
    labelKey: "dataProducts.colDescription",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 280,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (p) =>
      p.description ? (
        <TruncatedCell text={p.description} className="text-muted-foreground" />
      ) : (
        <span className="text-muted-foreground">—</span>
      ),
  },
  status: {
    labelKey: "dataProducts.colStatus",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 140,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (p) => <DataProductStatusBadge status={p.display_status} />,
  },
  steward: {
    labelKey: "dataProducts.colSteward",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 180,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (p) =>
      p.steward ? <TruncatedCell text={p.steward} /> : <span className="text-muted-foreground">—</span>,
  },
  tables: {
    labelKey: "dataProducts.colTables",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 150,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (p) => <TablesCell product={p} />,
  },
  rules: {
    labelKey: "dataProducts.colRules",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 90,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (p) => <span className="tabular-nums">{sumMembers(p, (r) => r)}</span>,
  },
  checks: {
    labelKey: "dataProducts.colChecks",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 90,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (p) => <span className="tabular-nums">{sumMembers(p, (_r, c) => c)}</span>,
  },
  lastRun: {
    labelKey: "dataProducts.colLastRun",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 120,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (p) => <RelativeTimeCell iso={p.last_run_at} />,
  },
  schedule: {
    labelKey: "dataProducts.colSchedule",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 110,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (p) => <ScheduleCell product={p} />,
  },
};

/** Returns the sortable value for a given column + row — shared between
 *  this component's click-to-sort handling and any caller that needs to
 *  pre-sort rows. */
export function getDataProductsSortValue(key: DataProductsSortKey, p: DataProductOut): string | number {
  switch (key) {
    case "name":
      return p.name.toLowerCase();
    case "description":
      return (p.description ?? "").toLowerCase();
    case "status":
      return p.display_status ?? "";
    case "steward":
      return (p.steward ?? "").toLowerCase();
    case "tables":
      return p.member_count ?? 0;
    case "rules":
      return sumMembers(p, (r) => r);
    case "checks":
      return sumMembers(p, (_r, c) => c);
    case "lastRun":
      return p.last_run_at ? new Date(p.last_run_at).getTime() : -1;
    case "schedule":
      return p.schedule_cron ? 1 : 0;
  }
}

const DEFAULT_ORDER: DataProductsSortKey[] = [
  "name",
  "description",
  "status",
  "steward",
  "tables",
  "rules",
  "checks",
  "lastRun",
  "schedule",
];

const LS_KEY_LAYOUT = "dqx.products.layout.v1";

export interface DataProductsTableProps {
  /** Rows to render — already filtered, sorted, and paginated by the caller. */
  rows: DataProductOut[];
  sortKey: DataProductsSortKey | null;
  sortDir: "asc" | "desc";
  onHeaderClick: (key: DataProductsSortKey) => void;
  onRowClick: (row: DataProductOut) => void;
  /** Rendered to the left of the "Edit Columns" trigger — the filter row. */
  toolbarExtra?: ReactNode;
  emptyState?: ReactNode;
}

/**
 * The Data Products list table: selectable + drag-reorderable columns
 * (persisted to localStorage), stable widths across sort clicks. Ported
 * from dqlake's `DataProductsTable` and adapted to DQX's `DataProductOut`
 * shape — the DQ Score column is cut (design spec §8) and #Rules/#Checks
 * are summed client-side from `members` (DQX's list endpoint doesn't
 * surface aggregate rule/check counts on the product itself the way
 * dqlake's `DataProductOutBrief` does).
 */
export function DataProductsTable({
  rows,
  sortKey,
  sortDir,
  onHeaderClick,
  onRowClick,
  toolbarExtra,
  emptyState,
}: DataProductsTableProps) {
  const { t } = useTranslation();

  const {
    colOrder,
    colWidths,
    visibleKeys,
    toggleColumn,
    handleDragEnd,
    sensors,
    onResizeStart,
  } = useColumnLayout<DataProductsSortKey>({
    storageKey: LS_KEY_LAYOUT,
    defaultOrder: DEFAULT_ORDER,
    columns: COLUMNS as Record<DataProductsSortKey, ColumnLayoutDef>,
  });

  function handleHeaderClick(key: DataProductsSortKey) {
    if (!COLUMNS[key].sortable) return;
    onHeaderClick(key);
  }

  const totalWidth = visibleKeys.reduce((acc, k) => acc + (colWidths[k] ?? COLUMNS[k].defaultWidth), 0);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        {toolbarExtra}
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

      <div className="overflow-x-auto">
        <Table className="table-fixed" style={{ width: totalWidth, minWidth: totalWidth }}>
          <colgroup>
            {visibleKeys.map((k) => (
              <col key={k} style={{ width: colWidths[k] ?? COLUMNS[k].defaultWidth }} />
            ))}
          </colgroup>
          <TableHeader>
            <TableRow className="bg-muted/50 hover:bg-muted/50">
              {visibleKeys.map((k) => {
                const def = COLUMNS[k];
                const width = colWidths[k] ?? def.defaultWidth;
                const isSorted = sortKey === k;
                const isResizable = def.resizable !== false;
                const label = t(def.labelKey);
                return (
                  <TableHead
                    key={k}
                    className={cn(
                      "relative text-xs font-medium px-2",
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
            {rows.map((p) => (
              <TableRow key={p.product_id} className="cursor-pointer" onClick={() => onRowClick(p)}>
                {visibleKeys.map((k) => {
                  const width = colWidths[k] ?? COLUMNS[k].defaultWidth;
                  return (
                    <TableCell
                      key={k}
                      style={{ width, minWidth: width, maxWidth: width }}
                      className="overflow-hidden p-2 align-middle"
                    >
                      {COLUMNS[k].renderCell(p)}
                    </TableCell>
                  );
                })}
              </TableRow>
            ))}
            {rows.length === 0 && emptyState && (
              <TableRow>
                <TableCell colSpan={visibleKeys.length} className="text-center py-16 px-2">
                  {emptyState}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

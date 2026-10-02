import { type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";
import { AlertTriangle, ChevronDown, ChevronUp, Loader2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { useColumnLayout, type ColumnLayoutDef } from "@/components/data-table/column-layout";
import { EditColumnsDropdown, type SortableToggleListConfig } from "@/components/data-table/EditColumnsDropdown";
import { RelativeTimeCell } from "@/components/data-table/RelativeTimeCell";
import {
  ACTIONS_COL_WIDTH,
  STICKY_ACTIONS_CELL_CLASS,
  STICKY_ACTIONS_HEAD_CLASS,
} from "@/components/data-table/sticky-actions";
import type { SortColumnConfig, SortDirection } from "@/components/data-table/sort";
import {
  getRelativeFutureParts,
  scheduleHealth,
  scheduleRowKey,
  type ScheduleFilterKey,
  type ScheduleSortKey,
} from "@/components/schedules/schedule-list";
import type { ScheduleOverviewOut } from "@/lib/api";
import { cronHint } from "@/lib/cron";
import { formatDateTime } from "@/lib/format-utils";
import { cn } from "@/lib/utils";

/** Per-row context the table needs beyond the row itself. */
interface CellContext {
  t: TFunction;
  /** True when this table row's FQN is also run by a scheduled collection. */
  duplicate: boolean;
}

interface ColumnDef {
  labelKey: string;
  toggleable: boolean;
  defaultVisible: boolean;
  defaultWidth: number;
  /** First-click sort direction. Defaults to "asc" when omitted. */
  defaultSortDir?: SortDirection;
  /** Missing values sort to the TOP regardless of direction when true. */
  nullsFirst?: boolean;
  renderCell(row: ScheduleOverviewOut, ctx: CellContext): ReactNode;
}

function Muted() {
  return <span className="text-muted-foreground">—</span>;
}

function NameCell({ row, duplicate }: { row: ScheduleOverviewOut; duplicate: boolean }) {
  const { t } = useTranslation();
  return (
    <div className="min-w-0">
      <span className="block truncate font-medium" title={row.name}>
        {row.name}
      </span>
      <span className="flex min-w-0 items-center gap-1 text-xs text-muted-foreground">
        <span className="truncate" title={row.target}>
          {row.target}
        </span>
        {duplicate && (
          <Tooltip>
            <TooltipTrigger asChild>
              <AlertTriangle
                className="h-3.5 w-3.5 shrink-0 text-amber-500"
                aria-label={t("schedules.duplicateWarning")}
              />
            </TooltipTrigger>
            <TooltipContent>{t("schedules.duplicateWarning")}</TooltipContent>
          </Tooltip>
        )}
      </span>
    </div>
  );
}

export function ScheduleTypeBadge({ type }: { type: ScheduleOverviewOut["source_type"] }) {
  const { t } = useTranslation();
  const labelKey =
    type === "table" ? "schedules.typeTable" : type === "collection" ? "schedules.typeCollection" : "schedules.typeScope";
  return (
    <Badge variant="outline" className="text-[10px]">
      {t(labelKey)}
    </Badge>
  );
}

function HealthBadge({ row }: { row: ScheduleOverviewOut }) {
  const { t } = useTranslation();
  switch (scheduleHealth(row)) {
    case "paused":
      return (
        <Badge variant="secondary" className="text-[10px]">
          {t("schedules.paused")}
        </Badge>
      );
    case "failed":
      return (
        <Badge variant="outline" className="text-[10px] border-red-500 text-red-600">
          {t("schedules.failed")}
        </Badge>
      );
    case "partial":
      return (
        <Badge variant="outline" className="text-[10px] border-amber-500 text-amber-600">
          {t("schedules.partial")}
        </Badge>
      );
    case "active":
      return (
        <Badge variant="outline" className="text-[10px] border-emerald-500 text-emerald-600">
          {t("schedules.active")}
        </Badge>
      );
  }
}

/** Time-until cell for the next scheduled run ("in 3h"), with the absolute
 *  timestamp in an in-app tooltip — the forward-looking twin of
 *  `RelativeTimeCell`. */
function NextRunCell({ iso }: { iso: string | null | undefined }) {
  const { t } = useTranslation();
  const rel = getRelativeFutureParts(iso);
  if (!rel) return <Muted />;
  const label =
    rel.key === "dueNow"
      ? t("schedules.relativeDueNow")
      : t(`schedules.relative${rel.key[0].toUpperCase()}${rel.key.slice(1)}`, { count: rel.count });
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="cursor-default">{label}</span>
      </TooltipTrigger>
      <TooltipContent side="top">{formatDateTime(iso)}</TooltipContent>
    </Tooltip>
  );
}

function cadenceText(row: ScheduleOverviewOut, t: TFunction): string | null {
  if (row.cron) return cronHint(row.cron, row.timezone, t);
  return row.frequency ?? null;
}

const COLUMNS: Record<ScheduleSortKey, ColumnDef> = {
  name: {
    labelKey: "schedules.name",
    toggleable: false,
    defaultVisible: true,
    defaultWidth: 260,
    renderCell: (row, ctx) => <NameCell row={row} duplicate={ctx.duplicate} />,
  },
  type: {
    labelKey: "schedules.type",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 120,
    renderCell: (row) => <ScheduleTypeBadge type={row.source_type} />,
  },
  cadence: {
    labelKey: "schedules.cadence",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 240,
    renderCell: (row, { t }) => {
      const text = cadenceText(row, t);
      return text ? (
        <span className="block truncate" title={text}>
          {text}
        </span>
      ) : (
        <Muted />
      );
    },
  },
  kind: {
    labelKey: "schedules.kind",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 180,
    renderCell: (row, { t }) =>
      row.schedule_kind ? (
        <span className="block truncate">{t(`schedule.kindOption.${row.schedule_kind}`)}</span>
      ) : (
        <Muted />
      ),
  },
  owner: {
    labelKey: "schedules.owner",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 180,
    renderCell: (row) =>
      row.owner ? (
        <span className="block truncate" title={row.owner}>
          {row.owner}
        </span>
      ) : (
        <Muted />
      ),
  },
  health: {
    labelKey: "schedules.health",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 130,
    renderCell: (row) => <HealthBadge row={row} />,
  },
  lastRun: {
    labelKey: "schedules.lastRun",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 110,
    // Most recent first; never-run schedules sort last.
    defaultSortDir: "desc",
    renderCell: (row) => <RelativeTimeCell iso={row.last_run_at} />,
  },
  nextRun: {
    labelKey: "schedules.nextRun",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 110,
    // Soonest first; schedules with nothing queued sort last.
    renderCell: (row) => <NextRunCell iso={row.next_run_at} />,
  },
  updatedAt: {
    labelKey: "schedules.updatedAt",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 120,
    defaultSortDir: "desc",
    renderCell: (row) => <RelativeTimeCell iso={row.updated_at} />,
  },
};

const DEFAULT_ORDER: ScheduleSortKey[] = [
  "name",
  "type",
  "cadence",
  "kind",
  "owner",
  "health",
  "lastRun",
  "nextRun",
  "updatedAt",
];

const LS_KEY_LAYOUT = "dqx.schedules.layout.v1";

export function getScheduleSortConfig(key: ScheduleSortKey): SortColumnConfig {
  const def = COLUMNS[key];
  return { dir: def.defaultSortDir ?? "asc", nullsFirst: def.nullsFirst ?? false };
}

export interface SchedulesTableSelection {
  selectedKeys: Set<string>;
  selectableKeys: Set<string>;
  onToggle: (key: string) => void;
  onToggleAll: () => void;
}

export interface SchedulesTableProps {
  /** Rows to render — already filtered, sorted, and paginated by the caller. */
  rows: ScheduleOverviewOut[];
  sortKey: ScheduleSortKey | null;
  sortDir: SortDirection;
  onHeaderClick: (key: ScheduleSortKey) => void;
  /** Opens the schedule's owning table/collection; omitted rows aren't clickable. */
  onRowClick?: (row: ScheduleOverviewOut) => void;
  renderActions: (row: ScheduleOverviewOut) => ReactNode;
  /** Row key whose action cell shows a spinner while a row action runs. */
  pendingKey?: string | null;
  /** Table FQNs also run by a scheduled collection (duplicate-run warning). */
  duplicateTargets: Set<string>;
  /** Rendered to the left of the "Edit Columns" trigger — the filter row. */
  toolbarExtra?: ReactNode;
  /** Filters view of the Edit Columns menu (see `useFilterLayout`). */
  filterMenu?: SortableToggleListConfig<ScheduleFilterKey>;
  emptyState?: ReactNode;
  /** When set, renders a leading checkbox column for bulk actions. */
  selection?: SchedulesTableSelection;
}

/**
 * The Schedules overview table — same building blocks as the Rules, Tables
 * and Collections overviews: persisted drag-reorderable/toggleable columns
 * via `useColumnLayout` + `EditColumnsDropdown`, resizable headers,
 * click-to-sort, optional bulk-selection column, and the sticky Actions column.
 */
export function SchedulesTable({
  rows,
  sortKey,
  sortDir,
  onHeaderClick,
  onRowClick,
  renderActions,
  pendingKey,
  duplicateTargets,
  toolbarExtra,
  filterMenu,
  emptyState,
  selection,
}: SchedulesTableProps) {
  const { t } = useTranslation();
  const { colOrder, colWidths, visibleKeys, toggleColumn, handleDragEnd, sensors, onResizeStart } =
    useColumnLayout<ScheduleSortKey>({
      storageKey: LS_KEY_LAYOUT,
      defaultOrder: DEFAULT_ORDER,
      columns: COLUMNS as Record<ScheduleSortKey, ColumnLayoutDef>,
    });

  const selectableCount = selection?.selectableKeys.size ?? 0;
  const selectedCount = selection?.selectedKeys.size ?? 0;
  const allSelected = selectableCount > 0 && selectedCount === selectableCount;
  const someSelected = selectedCount > 0 && !allSelected;

  const totalWidth =
    (selection ? 40 : 0) +
    visibleKeys.reduce((acc, k) => acc + (colWidths[k] ?? COLUMNS[k].defaultWidth), 0) +
    ACTIONS_COL_WIDTH;

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
          filters={filterMenu}
        />
      </div>

      <div className="overflow-x-auto">
        <Table className="table-fixed" style={{ width: totalWidth, minWidth: totalWidth }}>
          <colgroup>
            {selection && <col style={{ width: 40, minWidth: 40, maxWidth: 40 }} />}
            {visibleKeys.map((k) => (
              <col key={k} style={{ width: colWidths[k] ?? COLUMNS[k].defaultWidth }} />
            ))}
            <col style={{ width: ACTIONS_COL_WIDTH }} />
          </colgroup>
          <TableHeader>
            <TableRow className="bg-muted/50 hover:bg-muted/50">
              {selection && (
                <TableHead className="w-10 px-2">
                  <Checkbox
                    checked={allSelected ? true : someSelected ? "indeterminate" : false}
                    onCheckedChange={() => selection.onToggleAll()}
                    aria-label={t("common.selectAll")}
                    disabled={selectableCount === 0}
                  />
                </TableHead>
              )}
              {visibleKeys.map((k) => {
                const def = COLUMNS[k];
                const width = colWidths[k] ?? def.defaultWidth;
                const isSorted = sortKey === k;
                return (
                  <TableHead
                    key={k}
                    className="relative cursor-pointer select-none px-2 text-xs font-medium"
                    style={{ width, minWidth: width, maxWidth: width }}
                    onClick={() => onHeaderClick(k)}
                    aria-sort={isSorted ? (sortDir === "asc" ? "ascending" : "descending") : undefined}
                  >
                    <span className="inline-flex items-center gap-1">
                      {t(def.labelKey)}
                      {isSorted &&
                        (sortDir === "asc" ? (
                          <ChevronUp className="h-3 w-3" aria-hidden />
                        ) : (
                          <ChevronDown className="h-3 w-3" aria-hidden />
                        ))}
                    </span>
                    <span
                      role="separator"
                      aria-orientation="vertical"
                      className="absolute right-0 top-0 h-full w-1 cursor-col-resize select-none hover:bg-border"
                      onMouseDown={(e) => onResizeStart(k, e)}
                      onClick={(e) => e.stopPropagation()}
                    />
                  </TableHead>
                );
              })}
              <TableHead
                className={cn("px-2 text-right text-xs font-medium", STICKY_ACTIONS_HEAD_CLASS)}
                style={{ width: ACTIONS_COL_WIDTH }}
              >
                {t("schedules.actions")}
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => {
              const key = scheduleRowKey(row);
              const ctx: CellContext = {
                t,
                duplicate: row.source_type === "table" && duplicateTargets.has(row.target),
              };
              const clickable = !!onRowClick && row.source_type !== "scope";
              const isSelected = selection?.selectedKeys.has(key) ?? false;
              return (
                <TableRow
                  key={key}
                  className={cn("group", clickable && "cursor-pointer")}
                  onClick={clickable ? () => onRowClick(row) : undefined}
                >
                  {selection && (
                    <TableCell className="w-10 p-2 align-middle" onClick={(e) => e.stopPropagation()}>
                      {selection.selectableKeys.has(key) ? (
                        <Checkbox
                          checked={isSelected}
                          onCheckedChange={() => selection.onToggle(key)}
                          aria-label={t("schedules.selectRowAria", { name: row.name })}
                          className={cn(
                            "transition-opacity",
                            !isSelected &&
                              selectedCount === 0 &&
                              "opacity-0 group-hover:opacity-100 focus-visible:opacity-100",
                          )}
                        />
                      ) : null}
                    </TableCell>
                  )}
                  {visibleKeys.map((k) => {
                    const width = colWidths[k] ?? COLUMNS[k].defaultWidth;
                    return (
                      <TableCell
                        key={k}
                        style={{ width, minWidth: width, maxWidth: width }}
                        className="overflow-hidden p-2 align-middle"
                      >
                        {COLUMNS[k].renderCell(row, ctx)}
                      </TableCell>
                    );
                  })}
                  <TableCell
                    style={{ width: ACTIONS_COL_WIDTH }}
                    className={cn("p-2 text-right", STICKY_ACTIONS_CELL_CLASS)}
                    onClick={(e) => e.stopPropagation()}
                  >
                    {pendingKey === key ? (
                      <Loader2 className="inline-block h-3.5 w-3.5 animate-spin text-muted-foreground" />
                    ) : (
                      renderActions(row)
                    )}
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </div>
      {rows.length === 0 && emptyState && (
        <div className="flex flex-col items-center justify-center py-16 text-center">{emptyState}</div>
      )}
    </div>
  );
}

import { useMemo, useRef, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ChevronDown, ChevronUp, Lock, Sparkles } from "lucide-react";
import { cn } from "@/lib/utils";
import { useColumnLayout, type ColumnLayoutDef } from "@/components/data-table/column-layout";
import { EditColumnsDropdown } from "@/components/data-table/EditColumnsDropdown";
import { RelativeTimeCell } from "@/components/data-table/RelativeTimeCell";
import { STICKY_ACTIONS_HEAD_CLASS, STICKY_ACTIONS_CELL_CLASS } from "@/components/data-table/sticky-actions";
import { AI_GRADIENT_URL } from "@/lib/ai-style";
import { AuthorKindIcon } from "./AuthorKindIcon";
import {
  RESERVED_NAME_KEY,
  RESERVED_DESCRIPTION_KEY,
  RESERVED_DIMENSION_KEY,
  RESERVED_SEVERITY_KEY,
  getTag,
  colorFor,
  TagBadge,
  SeverityBadge,
  StatusBadge,
  ModifiedBadge,
  ModeBadge,
  type LabelColorDefinition,
} from "@/components/RegistryRuleBadges";
import type { RegistryRuleOut } from "@/lib/api";

/** Column keys that carry a comparable value and can drive client sort. */
export type RulesTableSortKey =
  | "aiAuthorship"
  | "name"
  | "description"
  | "dimension"
  | "severity"
  | "status"
  | "version"
  | "steward"
  | "createdBy"
  | "updated"
  | "mode";

type ColumnKey = RulesTableSortKey | "actions";

interface ColumnDef {
  labelKey: string;
  toggleable: boolean;
  defaultVisible: boolean;
  defaultWidth: number;
  sortable: boolean;
  /** Icon-only columns stay locked to their natural width — no resize handle. */
  resizable?: boolean;
  headClassName?: string;
  renderHeader(label: string): ReactNode;
  renderCell(r: RegistryRuleOut, ctx: RulesTableRenderContext): ReactNode;
}

interface RulesTableRenderContext {
  labelDefinitions: LabelColorDefinition[];
}

/**
 * Renders text with a tooltip that only appears when the text is actually
 * clipped. Ported from dqlake's RulesTable — detection runs on pointer
 * enter (not at mount) since the cell's final width isn't known until the
 * table has settled into its column widths.
 */
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
        <TooltipContent side="top" className="max-w-md break-words">
          {tooltipText ?? text}
        </TooltipContent>
      )}
    </Tooltip>
  );
}

function NameCell({ r }: { r: RegistryRuleOut }) {
  const { t } = useTranslation();
  const name = getTag(r, RESERVED_NAME_KEY) || r.rule_id;
  return (
    <div className="flex items-center gap-1.5 min-w-0">
      {r.is_builtin && (
        <span title={t("rulesRegistry.builtinTooltip")}>
          <Lock className="h-3 w-3 text-muted-foreground shrink-0" />
        </span>
      )}
      <TruncatedCell text={name} className="font-medium text-sm" />
    </div>
  );
}

const COLUMNS: Record<ColumnKey, ColumnDef> = {
  aiAuthorship: {
    labelKey: "rulesRegistry.colAiAuthorship",
    toggleable: true,
    // Hidden by default — the list row's top area shows Status + Version
    // instead of the type/authorship chips. Still available via Edit
    // Columns for anyone who wants it back.
    defaultVisible: false,
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
  },
  name: {
    labelKey: "rulesRegistry.colName",
    toggleable: false,
    defaultVisible: true,
    defaultWidth: 240,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <NameCell r={r} />,
  },
  description: {
    labelKey: "rulesRegistry.colDescription",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 320,
    sortable: true,
    headClassName: "max-w-xs",
    renderHeader: (label) => label,
    renderCell: (r) => {
      const description = getTag(r, RESERVED_DESCRIPTION_KEY);
      return (
        <TruncatedCell
          text={description || "—"}
          tooltipText={description || undefined}
          className="text-muted-foreground"
        />
      );
    },
  },
  status: {
    labelKey: "rulesRegistry.colStatus",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 140,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => (
      <span className="flex flex-wrap items-center gap-1">
        <StatusBadge status={r.status} />
        {r.display_status === "modified" && <ModifiedBadge version={r.version} />}
      </span>
    ),
  },
  version: {
    labelKey: "rulesRegistry.colVersion",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 90,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <span className="text-xs text-muted-foreground font-mono">v{r.version}</span>,
  },
  steward: {
    labelKey: "rulesRegistry.colSteward",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 180,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <TruncatedCell text={r.steward || "—"} className="text-muted-foreground" />,
  },
  createdBy: {
    labelKey: "rulesRegistry.colCreatedBy",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 180,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <TruncatedCell text={r.created_by || "—"} className="text-muted-foreground" />,
  },
  updated: {
    labelKey: "rulesRegistry.colUpdated",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 140,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <RelativeTimeCell iso={r.updated_at} />,
  },
  mode: {
    labelKey: "rulesRegistry.colMode",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 110,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <ModeBadge mode={r.mode} />,
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
  },
  actions: {
    labelKey: "rulesRegistry.colActions",
    // Pinned: always visible, since it's the only surface for the rule
    // lifecycle actions (submit/approve/reject/deprecate/delete).
    toggleable: false,
    defaultVisible: true,
    defaultWidth: 140,
    sortable: false,
    resizable: false,
    headClassName: "text-right",
    renderHeader: (label) => label,
    renderCell: () => null,
  },
};

const DEFAULT_ORDER: ColumnKey[] = [
  "aiAuthorship",
  "name",
  "description",
  "status",
  "version",
  "steward",
  "createdBy",
  "updated",
  "mode",
  "dimension",
  "severity",
  "actions",
];

/** Returns the sortable value for a given column + rule, shared between
 *  this component's own click-to-sort handling and any caller that needs
 *  to pre-sort rows before pagination. */
export function getRulesTableSortValue(key: RulesTableSortKey, r: RegistryRuleOut): string | number {
  switch (key) {
    case "aiAuthorship":
      return r.author_kind ?? "";
    case "name":
      return (getTag(r, RESERVED_NAME_KEY) || r.rule_id).toLowerCase();
    case "description":
      return getTag(r, RESERVED_DESCRIPTION_KEY).toLowerCase();
    case "dimension":
      return getTag(r, RESERVED_DIMENSION_KEY).toLowerCase();
    case "severity":
      return getTag(r, RESERVED_SEVERITY_KEY).toLowerCase();
    case "status":
      return r.status;
    case "version":
      return r.version;
    case "steward":
      return (r.steward ?? "").toLowerCase();
    case "createdBy":
      return (r.created_by ?? "").toLowerCase();
    case "updated":
      return r.updated_at ?? "";
    case "mode":
      return r.mode;
  }
}

// Column visibility/order/width bookkeeping (persisted to localStorage,
// reconciled against the current ColumnKey set) lives in the shared
// `useColumnLayout` hook — see components/data-table/column-layout.ts.
const LS_KEY_LAYOUT = "dqx.rulesRegistry.layout";

export interface RulesTableProps {
  /** Rows to render — already filtered, sorted, and paginated by the caller. */
  rows: RegistryRuleOut[];
  labelDefinitions: LabelColorDefinition[];
  sortKey: RulesTableSortKey | null;
  sortDir: "asc" | "desc";
  onHeaderClick: (key: RulesTableSortKey) => void;
  onRowClick: (rule: RegistryRuleOut) => void;
  renderActions: (rule: RegistryRuleOut) => ReactNode;
  /** Rendered to the left of the "Edit Columns" trigger — the filter row. */
  toolbarExtra?: ReactNode;
  /**
   * Message shown as a single spanning row inside the table body when
   * `rows` is empty, instead of swapping out the whole table for a
   * separate empty-state panel. Matches dqlake's RulesTable, which always
   * renders the table shell (filters + Edit Columns + headers) and only
   * replaces the body with a text row when there's nothing to show.
   */
  emptyMessage?: ReactNode;
}

/**
 * The Rules Registry list table: selectable + drag-reorderable columns
 * (persisted to localStorage), stable widths across sort clicks, and
 * optional per-column resize. Ported from dqlake's RulesTable and adapted
 * to DQX's registry-rule fields (dimension/severity/tags live in
 * `user_metadata` rather than as top-level columns).
 */
export function RulesTable({
  rows,
  labelDefinitions,
  sortKey,
  sortDir,
  onHeaderClick,
  onRowClick,
  renderActions,
  toolbarExtra,
  emptyMessage,
}: RulesTableProps) {
  const { t } = useTranslation();
  const ctx = useMemo<RulesTableRenderContext>(() => ({ labelDefinitions }), [labelDefinitions]);

  const {
    colOrder,
    colWidths,
    visibleKeys,
    toggleColumn,
    handleDragEnd,
    sensors,
    onResizeStart,
  } = useColumnLayout<ColumnKey>({
    storageKey: LS_KEY_LAYOUT,
    defaultOrder: DEFAULT_ORDER,
    columns: COLUMNS as Record<ColumnKey, ColumnLayoutDef>,
  });

  function handleHeaderClick(key: ColumnKey) {
    if (key === "actions" || !COLUMNS[key].sortable) return;
    onHeaderClick(key);
  }

  const totalWidth = visibleKeys.reduce((acc, k) => acc + (colWidths[k] ?? COLUMNS[k].defaultWidth), 0);

  // Actions is pinned last: excluded from the Edit Columns list/reorder
  // entirely (it's the only surface for rule lifecycle actions and must
  // never be hidden or moved), and rendered as the final, sticky-right
  // column regardless of where it happens to sit in the persisted column
  // order — so a stale/corrupted layout payload can't move it.
  const editableOrder = colOrder.filter((k) => k !== "actions");
  const orderedKeys: ColumnKey[] = visibleKeys.includes("actions")
    ? [...visibleKeys.filter((k) => k !== "actions"), "actions"]
    : visibleKeys;

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        {toolbarExtra}
        <EditColumnsDropdown
          order={editableOrder}
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
            {orderedKeys.map((k) => (
              <col key={k} style={{ width: colWidths[k] ?? COLUMNS[k].defaultWidth }} />
            ))}
          </colgroup>
          <TableHeader>
            <TableRow className="bg-muted/50 hover:bg-muted/50">
              {orderedKeys.map((k) => {
                const def = COLUMNS[k];
                const width = colWidths[k] ?? def.defaultWidth;
                const isSorted = k !== "actions" && sortKey === k;
                const isResizable = def.resizable !== false;
                const label = t(def.labelKey);
                return (
                  <TableHead
                    key={k}
                    className={cn(
                      // Matches MonitoredTablesTable's header styling, and
                      // both are condensed to dqlake's compact density
                      // (px-2) rather than the shared Table primitive's
                      // default (px-3) — see TableCell below.
                      "relative text-xs font-medium px-2",
                      def.headClassName,
                      def.sortable && "cursor-pointer select-none",
                      // Pinned right, frozen under horizontal scroll — same
                      // treatment as the Drafts & Review table's Actions
                      // column (routes/_sidebar/rules.drafts.tsx), shared
                      // with MonitoredTablesTable via sticky-actions.ts.
                      k === "actions" && STICKY_ACTIONS_HEAD_CLASS,
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
            {rows.map((r) => (
              <TableRow key={r.rule_id} className="group cursor-pointer" onClick={() => onRowClick(r)}>
                {orderedKeys.map((k) => {
                  const width = colWidths[k] ?? COLUMNS[k].defaultWidth;
                  return (
                    <TableCell
                      key={k}
                      style={{ width, minWidth: width, maxWidth: width }}
                      // Condensed to dqlake's compact row density (p-2
                      // instead of the shared primitive's default p-3).
                      // align-middle keeps badge cells (status/dimension/
                      // severity/mode) vertically centered in the row.
                      // The actions cell is pinned right and frozen under
                      // horizontal scroll — same treatment as the Drafts &
                      // Review table (routes/_sidebar/rules.drafts.tsx).
                      className={cn(
                        "overflow-hidden p-2 align-middle",
                        k === "actions" && STICKY_ACTIONS_CELL_CLASS,
                      )}
                      onClick={k === "actions" ? (e) => e.stopPropagation() : undefined}
                    >
                      {k === "actions" ? renderActions(r) : COLUMNS[k].renderCell(r, ctx)}
                    </TableCell>
                  );
                })}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      {/* Empty state renders OUTSIDE the overflow-x-auto container (P23
          item 17): inside it, the state centered on the table's fixed
          column-width sum and scrolled horizontally with the table instead
          of sitting centered in the viewport. */}
      {rows.length === 0 && emptyMessage && (
        <div className="flex flex-col items-center justify-center py-16 text-center text-muted-foreground">
          {emptyMessage}
        </div>
      )}
    </div>
  );
}

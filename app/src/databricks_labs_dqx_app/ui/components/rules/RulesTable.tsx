import { Fragment, useMemo, useRef, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Checkbox } from "@/components/ui/checkbox";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Badge } from "@/components/ui/badge";
import { AlertTriangle, ChevronDown, ChevronRight, ChevronUp, ListTree, Lock, Sparkles } from "lucide-react";
import { cn } from "@/lib/utils";
import { useColumnLayout, type ColumnLayoutDef } from "@/components/data-table/column-layout";
import { EditColumnsDropdown, type SortableToggleListConfig } from "@/components/data-table/EditColumnsDropdown";
import { FilterToolbar } from "@/components/data-table/FilterToolbar";
import { GroupHeaderRow, countByGroup, toggleGroupKey } from "@/components/data-table/GroupHeaderRow";
import { RuleAppliedTablesPanel } from "@/components/monitored-tables/ImplementedRulesExplorer";
import {
  STICKY_ACTIONS_HEAD_CLASS,
  STICKY_ACTIONS_CELL_CLASS,
  ACTIONS_COL_WIDTH,
} from "@/components/data-table/sticky-actions";
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
  RuleSourceBadge,
  normalizeRuleSource,
  type LabelColorDefinition,
} from "@/components/RegistryRuleBadges";
import type { SortColumnConfig, SortDirection, SortValue } from "@/components/data-table/sort";
import { formatDateTime, getRelativeTimeParts } from "@/lib/format-utils";
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
  | "owner"
  | "createdBy"
  | "source"
  | "modified"
  | "mode";

type ColumnKey = RulesTableSortKey | "appliedTables" | "actions";

interface ColumnDef {
  labelKey: string;
  toggleable: boolean;
  defaultVisible: boolean;
  defaultWidth: number;
  sortable: boolean;
  /** First-click sort direction for this column's header (B2-92). Defaults
   *  to "asc" when omitted. */
  defaultSortDir?: SortDirection;
  /** When true, rows with a missing ("never") value for this column sort to
   *  the TOP regardless of direction; omitted/false → they sort to the
   *  bottom (B2-92). */
  nullsFirst?: boolean;
  /** Icon-only columns stay locked to their natural width — no resize handle. */
  resizable?: boolean;
  headClassName?: string;
  renderHeader(label: string): ReactNode;
  renderCell(r: RegistryRuleOut, ctx: RulesTableRenderContext): ReactNode;
}

interface RulesTableRenderContext {
  labelDefinitions: LabelColorDefinition[];
  isExpanded: (ruleId: string) => boolean;
  onToggleExpanded: (ruleId: string) => void;
}

/** Chevron that expands a rule row to list the tables it is applied to (fetched on expand). */
function AppliedTablesToggle({ r, ctx }: { r: RegistryRuleOut; ctx: RulesTableRenderContext }) {
  const { t } = useTranslation();
  const expanded = ctx.isExpanded(r.rule_id);
  return (
    <button
      type="button"
      className="inline-flex h-6 w-6 items-center justify-center rounded-md hover:bg-muted"
      onClick={(e) => {
        e.stopPropagation();
        ctx.onToggleExpanded(r.rule_id);
      }}
      aria-label={t(expanded ? "rulesRegistry.collapseAppliedTablesAria" : "rulesRegistry.expandAppliedTablesAria", {
        name: getTag(r, RESERVED_NAME_KEY) || r.rule_id,
      })}
      aria-expanded={expanded}
    >
      <ChevronRight className={cn("h-4 w-4 transition-transform", expanded && "rotate-90")} />
    </button>
  );
}

/**
 * Renders text with a tooltip that only appears when the text is actually
 * clipped. Ported from dqlake's RulesTable — detection runs on pointer
 * enter (not at mount) since the cell's final width isn't known until the
 * table has settled into its column widths.
 */
/** Owner name, with a warning when the owner matched no Databricks principal. */
function OwnerCell({ rule }: { rule: RegistryRuleOut }) {
  const { t } = useTranslation();
  const text = rule.owner_display_name || rule.owner || "—";
  if (!rule.owner_unverified) {
    return <TruncatedCell text={text} className="text-muted-foreground" />;
  }
  return (
    <span className="flex min-w-0 items-center gap-1">
      <TruncatedCell text={text} className="min-w-0 text-muted-foreground" />
      <Tooltip>
        <TooltipTrigger asChild>
          <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-amber-500" aria-label={t("permissions.ownerNotFound")} />
        </TooltipTrigger>
        <TooltipContent side="top" className="max-w-xs">
          {t("permissions.ownerNotFound")}
        </TooltipContent>
      </Tooltip>
    </span>
  );
}

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

/** Relative modified time with absolute timestamp + author in the tooltip. */
function ModifiedTimestampCell({
  iso,
  by,
}: {
  iso: string | null | undefined;
  by: string | null | undefined;
}) {
  const { t } = useTranslation();
  const rel = getRelativeTimeParts(iso);
  if (!rel) return <span className="text-muted-foreground">—</span>;
  const label =
    rel.key === "justNow"
      ? t("common.relativeJustNow")
      : t(`common.relative${rel.key[0].toUpperCase()}${rel.key.slice(1)}`, { count: rel.count });
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="cursor-default text-muted-foreground">{label}</span>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs">
        <div>{formatDateTime(iso)}</div>
        {by ? (
          <div className="text-xs text-muted-foreground mt-0.5">
            {t("rulesRegistry.modifiedBy", { user: by })}
          </div>
        ) : null}
      </TooltipContent>
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
  appliedTables: {
    // Optional row expander listing the monitored tables the rule is applied
    // to. Hidden by default; the tables are fetched per row on expand.
    labelKey: "rulesRegistry.colAppliedTables",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 48,
    sortable: false,
    resizable: false,
    renderHeader: (label) => (
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="inline-flex">
            <ListTree className="h-4 w-4" aria-label={label} />
          </span>
        </TooltipTrigger>
        <TooltipContent>{label}</TooltipContent>
      </Tooltip>
    ),
    renderCell: (r, ctx) => <AppliedTablesToggle r={r} ctx={ctx} />,
  },
  aiAuthorship: {
    labelKey: "rulesRegistry.colAiAuthorship",
    toggleable: true,
    // Shown by default so a rule's AI-vs-human authorship is visible at a
    // glance on the overview. Still toggleable via Edit Columns.
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
    // B2-92: plain A→Z; rules with no description sort last.
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
    // B2-28: when an approved rule has unpublished edits (display_status
    // "modified") show ONLY the "modified since vN" badge — the "approved"
    // StatusBadge alongside it is redundant. Otherwise show the normal
    // StatusBadge for the current lifecycle status.
    renderCell: (r) =>
      r.display_status === "modified" ? (
        <span className="flex flex-wrap items-center gap-1">
          <ModifiedBadge version={r.version} />
        </span>
      ) : (
        <span className="flex flex-wrap items-center gap-1">
          <StatusBadge status={r.status} />
        </span>
      ),
  },
  version: {
    labelKey: "rulesRegistry.colVersion",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 90,
    sortable: true,
    // Latest version first (B2-92); never-approved (v0) rows sort last.
    defaultSortDir: "desc",
    renderHeader: (label) => label,
    renderCell: (r) =>
      r.version <= 0 ? (
        <span className="text-muted-foreground">—</span>
      ) : (
        <Badge variant="secondary" className="font-mono text-[10px]">
          v{r.version}
        </Badge>
      ),
  },
  owner: {
    labelKey: "rulesRegistry.colOwner",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 180,
    sortable: true,
    // A→Z through the named owners (B2-92); unowned rules sort last.
    renderHeader: (label) => label,
    renderCell: (r) => <OwnerCell rule={r} />,
  },
  source: {
    labelKey: "rulesRegistry.colSource",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 100,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <RuleSourceBadge source={r.source} />,
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
  modified: {
    labelKey: "rulesRegistry.colModified",
    // Always visible — owners need the modified timestamp at a glance.
    toggleable: false,
    defaultVisible: true,
    defaultWidth: 140,
    sortable: true,
    // Most recently modified first (B2-92); rows with no timestamp sort last.
    defaultSortDir: "desc",
    renderHeader: (label) => label,
    renderCell: (r) => <ModifiedTimestampCell iso={r.updated_at} by={r.updated_by} />,
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
    // A→Z through the assigned dimensions (B2-92); uncategorized rules sort last.
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
    // Shared with the other overview tables so the Actions column width is
    // consistent app-wide (item B2-43).
    defaultWidth: ACTIONS_COL_WIDTH,
    sortable: false,
    resizable: false,
    headClassName: "text-right",
    renderHeader: (label) => label,
    renderCell: () => null,
  },
};

const DEFAULT_ORDER: ColumnKey[] = [
  "appliedTables",
  "aiAuthorship",
  "name",
  "description",
  "status",
  "version",
  "owner",
  "source",
  "modified",
  "createdBy",
  "mode",
  "dimension",
  "severity",
  "actions",
];

/** Lifecycle-status sort rank (B2-92): a first-click ASC sort leads with the
 *  live/approved rules, then work in progress (pending approval, draft),
 *  with rejected and retired (deprecated) rules sinking to the bottom.
 *  Unset/unknown statuses sort after all known ones (and nulls last). */
const STATUS_RANK: Record<string, number> = {
  approved: 0,
  pending_approval: 1,
  draft: 2,
  rejected: 3,
  deprecated: 4,
};

/** Severity sort rank (B2-92): a first-click ASC sort leads with the
 *  highest-priority rules (Critical → Low), matching the app's established
 *  severity display order (see `orderSeverityValuesForDisplay`, which surfaces
 *  most-severe-first in filters and dropdowns). Unset severity is returned as
 *  `null` by the getter and pinned last. */
const SEVERITY_RANK: Record<string, number> = {
  critical: 0,
  high: 1,
  medium: 2,
  low: 3,
};

/** Returns the sortable value for a given column + rule, shared between
 *  this component's own click-to-sort handling and any caller that needs
 *  to pre-sort rows before pagination. `null` marks a missing/"never" value
 *  that {@link compareSortValues} pins per the column's `nullsFirst` flag. */
export function getRulesTableSortValue(key: RulesTableSortKey, r: RegistryRuleOut): SortValue {
  switch (key) {
    case "aiAuthorship":
      return r.author_kind || null;
    case "name":
      return (getTag(r, RESERVED_NAME_KEY) || r.rule_id).toLowerCase();
    case "description":
      return getTag(r, RESERVED_DESCRIPTION_KEY).toLowerCase() || null;
    case "dimension":
      return getTag(r, RESERVED_DIMENSION_KEY).toLowerCase() || null;
    case "severity": {
      const severity = getTag(r, RESERVED_SEVERITY_KEY).toLowerCase();
      if (!severity) return null;
      return SEVERITY_RANK[severity] ?? Object.keys(SEVERITY_RANK).length;
    }
    case "status":
      return STATUS_RANK[r.status] ?? Object.keys(STATUS_RANK).length;
    case "version":
      return r.version > 0 ? r.version : null;
    case "owner":
      return (r.owner ?? "").toLowerCase() || null;
    case "source":
      return normalizeRuleSource(r.source);
    case "createdBy":
      return (r.created_by ?? "").toLowerCase() || null;
    case "modified":
      return r.updated_at || null;
    case "mode":
      return r.mode || null;
  }
}

/** Resolves a column's first-click direction + null placement (B2-92) from
 *  its declarative `COLUMNS` config. Consumed by the overview route to seed
 *  the sort direction on a fresh header click and to drive the null-aware
 *  comparator. */
export function getRulesTableSortConfig(key: RulesTableSortKey): SortColumnConfig {
  const def = COLUMNS[key];
  return { dir: def.defaultSortDir ?? "asc", nullsFirst: def.nullsFirst ?? false };
}

// Column visibility/order/width bookkeeping (persisted to localStorage,
// reconciled against the current ColumnKey set) lives in the shared
// `useColumnLayout` hook — see components/data-table/column-layout.ts.
const LS_KEY_LAYOUT = "dqx.rulesRegistry.layout";

export interface RulesTableSelection {
  selectedIds: Set<string>;
  /** Rule ids that may be toggled (e.g. exclude built-ins). */
  selectableRuleIds: Set<string>;
  onToggle: (ruleId: string) => void;
  onToggleAll: () => void;
}

export interface RulesTableGroup {
  key: string;
  label: string;
}

export interface RulesTableProps<F extends string = string> {
  /** Rows to render — already filtered, sorted, and paginated by the caller. */
  rows: RegistryRuleOut[];
  labelDefinitions: LabelColorDefinition[];
  /** Inserts collapsible group headers; rows must arrive sorted by group. Omit for the flat table. */
  groupForRule?: (rule: RegistryRuleOut) => RulesTableGroup;
  /** Expands every group (e.g. while searching, so matches are never hidden). */
  forceGroupsExpanded?: boolean;
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
  /** When set, renders a leading checkbox column for bulk lifecycle actions. */
  selection?: RulesTableSelection;
  /** Filters view of the Edit Columns menu (see `useFilterLayout`). */
  filterLayout?: SortableToggleListConfig<F>;
}

/**
 * The Rules Registry list table: selectable + drag-reorderable columns
 * (persisted to localStorage), stable widths across sort clicks, and
 * optional per-column resize. Ported from dqlake's RulesTable and adapted
 * to DQX's registry-rule fields (dimension/severity/tags live in
 * `user_metadata` rather than as top-level columns).
 */
export function RulesTable<F extends string = string>({
  rows,
  labelDefinitions,
  groupForRule,
  forceGroupsExpanded = false,
  sortKey,
  sortDir,
  onHeaderClick,
  onRowClick,
  renderActions,
  toolbarExtra,
  emptyMessage,
  selection,
  filterLayout,
}: RulesTableProps<F>) {
  const { t } = useTranslation();
  const [expandedRuleIds, setExpandedRuleIds] = useState<Set<string>>(new Set());
  const ctx = useMemo<RulesTableRenderContext>(
    () => ({
      labelDefinitions,
      isExpanded: (ruleId) => expandedRuleIds.has(ruleId),
      onToggleExpanded: (ruleId) =>
        setExpandedRuleIds((prev) => {
          const next = new Set(prev);
          if (next.has(ruleId)) next.delete(ruleId);
          else next.add(ruleId);
          return next;
        }),
    }),
    [labelDefinitions, expandedRuleIds],
  );
  const showSelection = !!selection;
  const selectableCount = selection?.selectableRuleIds.size ?? 0;
  const allSelected =
    showSelection && selectableCount > 0 && selection!.selectedIds.size === selectableCount;
  const someSelected = showSelection && selection!.selectedIds.size > 0 && !allSelected;
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());

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
    if (key === "actions" || key === "appliedTables" || !COLUMNS[key].sortable) return;
    onHeaderClick(key);
  }

  const totalWidth =
    (showSelection ? 40 : 0) +
    visibleKeys.reduce((acc, k) => acc + (colWidths[k] ?? COLUMNS[k].defaultWidth), 0);

  // Actions is pinned last: excluded from the Edit Columns list/reorder
  // entirely (it's the only surface for rule lifecycle actions and must
  // never be hidden or moved), and rendered as the final, sticky-right
  // column regardless of where it happens to sit in the persisted column
  // order — so a stale/corrupted layout payload can't move it.
  const editableOrder = colOrder.filter((k) => k !== "actions");
  const orderedKeys: ColumnKey[] = visibleKeys.includes("actions")
    ? [...visibleKeys.filter((k) => k !== "actions"), "actions"]
    : visibleKeys;

  const groups = useMemo(() => rows.map((r) => groupForRule?.(r)), [rows, groupForRule]);
  const groupCounts = useMemo(() => countByGroup(groups), [groups]);

  function toggleGroup(key: string) {
    setExpandedGroups((prev) => toggleGroupKey(prev, key));
  }

  return (
    <div className="space-y-4">
      <FilterToolbar
        filters={toolbarExtra}
        editColumns={
          <EditColumnsDropdown
            order={editableOrder}
            labelOf={(key) => t(COLUMNS[key].labelKey)}
            toggleableOf={(key) => COLUMNS[key].toggleable}
            isChecked={(key) => visibleKeys.includes(key)}
            onToggle={toggleColumn}
            onDragEnd={handleDragEnd}
            sensors={sensors}
            filters={filterLayout}
          />
        }
      />

      <div className="overflow-x-auto">
        <Table className="table-fixed" style={{ width: totalWidth, minWidth: totalWidth }}>
          <colgroup>
            {showSelection && <col style={{ width: 40, minWidth: 40, maxWidth: 40 }} />}
            {orderedKeys.map((k) => (
              <col key={k} style={{ width: colWidths[k] ?? COLUMNS[k].defaultWidth }} />
            ))}
          </colgroup>
          <TableHeader>
            <TableRow className="bg-muted/50 hover:bg-muted/50">
              {showSelection && (
                <TableHead className="w-10 px-2">
                  <Checkbox
                    checked={allSelected ? true : someSelected ? "indeterminate" : false}
                    onCheckedChange={() => selection!.onToggleAll()}
                    aria-label={t("common.selectAll")}
                    disabled={selectableCount === 0}
                  />
                </TableHead>
              )}
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
                    {/* Block-level flex so icon-only headers centre in the
                        row like text ones (an inline box with no text would
                        sit on the text baseline, riding high). */}
                    <span className={cn("flex items-center gap-1", k === "actions" && "justify-end")}>
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
            {rows.map((r, rowIndex) => {
              const group = groups[rowIndex];
              const showGroupHeader = !!group && group.key !== groups[rowIndex - 1]?.key;
              const expanded = !group || forceGroupsExpanded || expandedGroups.has(group.key);
              const showAppliedTables = expanded && orderedKeys.includes("appliedTables") && expandedRuleIds.has(r.rule_id);
              return (
              <Fragment key={r.rule_id}>
              {showGroupHeader && group && (
                <GroupHeaderRow
                  label={group.label}
                  count={groupCounts.get(group.key) ?? 0}
                  expanded={expanded}
                  onToggle={() => toggleGroup(group.key)}
                  colSpan={orderedKeys.length + (showSelection ? 1 : 0)}
                />
              )}
              {expanded && (
              <TableRow className="group cursor-pointer" onClick={() => onRowClick(r)}>
                {showSelection && (
                  <TableCell
                    className="w-10 p-2 align-middle"
                    onClick={(e) => e.stopPropagation()}
                  >
                    {selection!.selectableRuleIds.has(r.rule_id) ? (
                      <Checkbox
                        checked={selection!.selectedIds.has(r.rule_id)}
                        onCheckedChange={() => selection!.onToggle(r.rule_id)}
                        aria-label={t("rulesRegistry.selectRowAria", {
                          name: getTag(r, RESERVED_NAME_KEY) || r.rule_id,
                        })}
                        className={cn(
                          "transition-opacity",
                          !selection!.selectedIds.has(r.rule_id) &&
                            selection!.selectedIds.size === 0 &&
                            "opacity-0 group-hover:opacity-100 focus-visible:opacity-100",
                        )}
                      />
                    ) : null}
                  </TableCell>
                )}
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
              )}
              {showAppliedTables && (
                <TableRow className="hover:bg-transparent">
                  <TableCell
                    colSpan={orderedKeys.length + (showSelection ? 1 : 0)}
                    className="bg-muted/15 px-10 py-3"
                  >
                    <RuleAppliedTablesPanel ruleId={r.rule_id} />
                  </TableCell>
                </TableRow>
              )}
              </Fragment>
              );
            })}
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

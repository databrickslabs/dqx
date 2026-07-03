import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  DndContext,
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  SortableContext,
  arrayMove,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { useTranslation } from "react-i18next";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ChevronDown, ChevronUp, Columns3, GripVertical, Lock, Sparkles } from "lucide-react";
import { cn } from "@/lib/utils";
import { formatDateShort } from "@/lib/format-utils";
import { AI_GRADIENT_URL } from "@/lib/ai-style";
import { AuthorKindIcon } from "./AuthorKindIcon";
import {
  RESERVED_NAME_KEY,
  RESERVED_DESCRIPTION_KEY,
  RESERVED_DIMENSION_KEY,
  RESERVED_SEVERITY_KEY,
  getTag,
  freeTags,
  colorFor,
  TagBadge,
  StatusBadge,
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
  const tags = freeTags(r);
  return (
    <div className="min-w-0">
      <div className="flex items-center gap-1.5 min-w-0">
        {r.is_builtin && (
          <span title={t("rulesRegistry.builtinTooltip")}>
            <Lock className="h-3 w-3 text-muted-foreground shrink-0" />
          </span>
        )}
        <TruncatedCell text={name} className="font-medium text-sm" />
      </div>
      {Object.keys(tags).length > 0 && (
        <div className="flex flex-wrap gap-1 mt-1">
          {Object.entries(tags).slice(0, 3).map(([k, v]) => (
            <Badge key={k} variant="outline" className="text-[9px] font-normal px-1 py-0">
              {k}={v}
            </Badge>
          ))}
        </div>
      )}
    </div>
  );
}

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
    defaultVisible: false,
    defaultWidth: 140,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <StatusBadge status={r.status} />,
  },
  version: {
    labelKey: "rulesRegistry.colVersion",
    toggleable: true,
    defaultVisible: false,
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
    renderCell: (r) => (
      <span className="text-xs text-muted-foreground" title={r.updated_at ?? undefined}>
        {r.updated_at ? formatDateShort(r.updated_at) : "—"}
      </span>
    ),
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
      return <TagBadge label={severity} color={colorFor(ctx.labelDefinitions, RESERVED_SEVERITY_KEY, severity)} />;
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

// A single combined payload for column visibility + order, reconciled
// against the current set of ColumnKeys on load — see dqlake's
// `dqlake.rules.layout` for the pattern this mirrors:
//   - Columns in storage no longer known in code -> dropped.
//   - Columns known in code but missing from storage (newly added) ->
//     appended in DEFAULT_ORDER position, with the current defaultVisible.
//   - Everything the user explicitly set keeps its value.
const LS_KEY_LAYOUT = "dqx.rulesRegistry.layout";

type StoredLayout = {
  visibility: Partial<Record<ColumnKey, boolean>>;
  order: ColumnKey[];
};

type LoadedLayout = {
  visibility: Record<ColumnKey, boolean>;
  order: ColumnKey[];
};

function defaultVisibility(): Record<ColumnKey, boolean> {
  return Object.fromEntries(DEFAULT_ORDER.map((k) => [k, COLUMNS[k].defaultVisible])) as Record<ColumnKey, boolean>;
}

function defaultWidths(): Record<ColumnKey, number> {
  return Object.fromEntries(DEFAULT_ORDER.map((k) => [k, COLUMNS[k].defaultWidth])) as Record<ColumnKey, number>;
}

function loadLayout(): LoadedLayout {
  let stored: Partial<StoredLayout> = {};
  try {
    const raw = localStorage.getItem(LS_KEY_LAYOUT);
    if (raw) stored = JSON.parse(raw) as Partial<StoredLayout>;
  } catch {
    // localStorage unavailable or malformed payload — fall back to defaults
  }

  const storedOrder = Array.isArray(stored.order) ? stored.order : [];
  const known = storedOrder.filter((k): k is ColumnKey => k in COLUMNS);
  const missing = DEFAULT_ORDER.filter((k) => !known.includes(k));
  const order = [...known, ...missing];

  const storedVis = (stored.visibility ?? {}) as Partial<Record<ColumnKey, boolean>>;
  const visibility = { ...defaultVisibility() } as Record<ColumnKey, boolean>;
  for (const key of order) {
    if (Object.prototype.hasOwnProperty.call(storedVis, key)) {
      const v = storedVis[key];
      if (typeof v === "boolean") visibility[key] = v;
    }
  }

  return { visibility, order };
}

function persistLayout(visibility: Record<ColumnKey, boolean>, order: ColumnKey[]): void {
  try {
    localStorage.setItem(LS_KEY_LAYOUT, JSON.stringify({ visibility, order }));
  } catch {
    // ignore
  }
}

interface SortableColumnItemProps {
  id: ColumnKey;
  label: string;
  checked: boolean;
  onCheckedChange: () => void;
  disabled?: boolean;
}

function SortableColumnItem({ id, label, checked, onCheckedChange, disabled }: SortableColumnItemProps) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  return (
    <div ref={setNodeRef} style={style} className="flex items-center">
      <span
        {...attributes}
        {...listeners}
        className="px-1 cursor-grab active:cursor-grabbing text-muted-foreground"
      >
        <GripVertical className="h-4 w-4" />
      </span>
      <DropdownMenuCheckboxItem
        className="flex-1"
        checked={checked}
        onCheckedChange={disabled ? undefined : onCheckedChange}
        disabled={disabled}
        onSelect={(e) => e.preventDefault()}
      >
        {label}
      </DropdownMenuCheckboxItem>
    </div>
  );
}

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
}: RulesTableProps) {
  const { t } = useTranslation();
  const ctx = useMemo<RulesTableRenderContext>(() => ({ labelDefinitions }), [labelDefinitions]);

  const initialLayout = useMemo(() => loadLayout(), []);
  const [colVisibility, setColVisibility] = useState<Record<ColumnKey, boolean>>(() => initialLayout.visibility);
  const [colOrder, setColOrder] = useState<ColumnKey[]>(() => initialLayout.order);
  const [colWidths, setColWidths] = useState<Record<ColumnKey, number>>(() => defaultWidths());

  useEffect(() => {
    persistLayout(colVisibility, colOrder);
  }, [colVisibility, colOrder]);

  function toggleColumn(id: ColumnKey) {
    setColVisibility((prev) => ({ ...prev, [id]: !prev[id] }));
  }

  const show = (id: ColumnKey) => colVisibility[id] ?? false;

  const visibleKeys = colOrder.filter((k) => (COLUMNS[k].toggleable ? show(k) : true));

  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 5 } }));

  function handleDragEnd(event: DragEndEvent) {
    const { active, over } = event;
    if (over && active.id !== over.id) {
      setColOrder((prev) => {
        const oldIdx = prev.indexOf(active.id as ColumnKey);
        const newIdx = prev.indexOf(over.id as ColumnKey);
        return arrayMove(prev, oldIdx, newIdx);
      });
    }
  }

  function handleHeaderClick(key: ColumnKey) {
    if (key === "actions" || !COLUMNS[key].sortable) return;
    onHeaderClick(key);
  }

  // -- Column resizing (session-only; not persisted, matching dqlake) -----
  const resizingRef = useRef<{ key: ColumnKey; startX: number; startW: number } | null>(null);

  const onResizeMove = useCallback((e: MouseEvent) => {
    const rCtx = resizingRef.current;
    if (!rCtx) return;
    const next = Math.max(50, rCtx.startW + (e.clientX - rCtx.startX));
    setColWidths((prev) => ({ ...prev, [rCtx.key]: next }));
  }, []);

  const onResizeEnd = useCallback(() => {
    resizingRef.current = null;
    document.removeEventListener("mousemove", onResizeMove);
    document.removeEventListener("mouseup", onResizeEnd);
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  }, [onResizeMove]);

  const onResizeStart = useCallback(
    (key: ColumnKey, e: React.MouseEvent) => {
      e.preventDefault();
      e.stopPropagation();
      resizingRef.current = { key, startX: e.clientX, startW: colWidths[key] ?? COLUMNS[key].defaultWidth };
      document.addEventListener("mousemove", onResizeMove);
      document.addEventListener("mouseup", onResizeEnd);
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
    },
    [colWidths, onResizeMove, onResizeEnd],
  );

  useEffect(() => {
    return () => {
      document.removeEventListener("mousemove", onResizeMove);
      document.removeEventListener("mouseup", onResizeEnd);
    };
  }, [onResizeMove, onResizeEnd]);

  const totalWidth = visibleKeys.reduce((acc, k) => acc + (colWidths[k] ?? COLUMNS[k].defaultWidth), 0);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        {toolbarExtra}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="outline" size="sm" className="h-8 text-xs ml-auto gap-1.5">
              <Columns3 className="h-3.5 w-3.5" />
              {t("rulesRegistry.editColumns")}
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" onCloseAutoFocus={(e) => e.preventDefault()}>
            <DndContext sensors={sensors} onDragEnd={handleDragEnd}>
              <SortableContext items={colOrder} strategy={verticalListSortingStrategy}>
                {colOrder.map((key) => (
                  <SortableColumnItem
                    key={key}
                    id={key}
                    label={t(COLUMNS[key].labelKey)}
                    checked={COLUMNS[key].toggleable ? show(key) : true}
                    onCheckedChange={() => toggleColumn(key)}
                    disabled={!COLUMNS[key].toggleable}
                  />
                ))}
              </SortableContext>
            </DndContext>
          </DropdownMenuContent>
        </DropdownMenu>
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
                const isSorted = k !== "actions" && sortKey === k;
                const isResizable = def.resizable !== false;
                const label = t(def.labelKey);
                return (
                  <TableHead
                    key={k}
                    className={cn(
                      "relative text-xs font-medium",
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
            {rows.map((r) => (
              <TableRow key={r.rule_id} className="cursor-pointer" onClick={() => onRowClick(r)}>
                {visibleKeys.map((k) => {
                  const width = colWidths[k] ?? COLUMNS[k].defaultWidth;
                  return (
                    <TableCell
                      key={k}
                      style={{ width, minWidth: width, maxWidth: width }}
                      className="overflow-hidden"
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
    </div>
  );
}

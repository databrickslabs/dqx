import { useEffect, useMemo, useRef, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Database,
  Search,
  Copy,
  ChevronDown,
  ChevronRight,
  X,
  Loader2,
} from "lucide-react";
import { toast } from "sonner";
import {
  useListCatalogs,
  useListSchemas,
  useListTables,
  useGetTableColumns,
  type CatalogOut,
  type SchemaOut,
  type TableOut,
  type ColumnOut,
} from "@/lib/api";

interface ColumnDiscoveryPanelProps {
  /**
   * If provided, the panel pre-selects this fully qualified table name on
   * mount (catalog.schema.table). The user can still change the selection.
   */
  initialTableFqn?: string;
  /**
   * Optional callback when a column is selected (clicked or "Use" button).
   * Defaults to copying the column name to the clipboard with a toast.
   */
  onColumnPick?: (column: ColumnOut, tableFqn: string) => void;
  /**
   * Fires when the user reaches a complete catalog.schema.table selection
   * via the panel's own dropdowns. Lets the page sync the discovery
   * choice into "Target tables" without forcing the user to pick the
   * table twice.
   *
   * Crucially, this does *not* fire for the initial pre-selection coming
   * from ``initialTableFqn`` — that's a load-time hint, not a user
   * action, and we don't want to overwrite explicit target choices from
   * an existing rule.
   */
  onTableSelect?: (tableFqn: string) => void;
  /** Persisted-collapsed state controlled externally (optional). */
  collapsed?: boolean;
  onCollapsedChange?: (collapsed: boolean) => void;
  className?: string;
  /** Whether to start collapsed when uncontrolled. Defaults to ``true``. */
  defaultCollapsed?: boolean;
  /** Render style. ``"inline"`` (default) renders a tinted bordered block
   * that fits inside another Card. ``"card"`` wraps in a sticky Card for
   * sidebar use. */
  variant?: "inline" | "card";
}

/**
 * Mini column discovery for the rule authoring flow.
 *
 * - Cascading catalog → schema → table dropdowns (independent from the
 *   page's other table pickers, so changing this doesn't affect anything
 *   the user is editing).
 * - Inline filter to narrow long column lists.
 * - Click a column (or the copy button) to copy its name to the clipboard
 *   so the author can paste it into the active "Column Name" field.
 */
export function ColumnDiscoveryPanel({
  initialTableFqn,
  onColumnPick,
  onTableSelect,
  collapsed: collapsedProp,
  onCollapsedChange,
  className,
  defaultCollapsed = true,
  variant = "inline",
}: ColumnDiscoveryPanelProps) {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [filter, setFilter] = useState("");

  const [internalCollapsed, setInternalCollapsed] = useState(defaultCollapsed);
  const collapsed = collapsedProp ?? internalCollapsed;
  const setCollapsed = (next: boolean) => {
    if (onCollapsedChange) onCollapsedChange(next);
    else setInternalCollapsed(next);
  };

  // Tracks whether the current ``catalog/schema/table`` came from a real
  // user interaction (so we know it's safe to fire ``onTableSelect``).
  // The initial pre-fill from ``initialTableFqn`` flips this to ``false``
  // explicitly, so the page can pre-load a rule's existing target table
  // without us echoing it back as a "user selected this table" signal.
  const userInitiatedRef = useRef(false);
  // Last FQN we forwarded to ``onTableSelect`` — guards against
  // double-firing when React re-runs effects with stable dependencies.
  const lastEmittedFqnRef = useRef<string>("");

  // Pre-fill from the initial FQN exactly once.
  useEffect(() => {
    if (!initialTableFqn) return;
    const parts = initialTableFqn.split(".");
    if (parts.length === 3 && parts.every(Boolean)) {
      setCatalog((prev) => prev || parts[0]);
      setSchema((prev) => prev || parts[1]);
      setTable((prev) => prev || parts[2]);
      // Mark this assignment as system-driven so the FQN-emit effect
      // below does NOT call ``onTableSelect`` for the pre-load.
      userInitiatedRef.current = false;
      lastEmittedFqnRef.current = initialTableFqn;
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Whenever a complete FQN is reached AND the change was user-driven,
  // forward the table to the page so it can sync "Target tables".
  useEffect(() => {
    if (!onTableSelect) return;
    if (!userInitiatedRef.current) return;
    if (!catalog || !schema || !table) return;
    const fqn = `${catalog}.${schema}.${table}`;
    if (fqn === lastEmittedFqnRef.current) return;
    lastEmittedFqnRef.current = fqn;
    onTableSelect(fqn);
  }, [catalog, schema, table, onTableSelect]);

  const { data: catalogsResp, isLoading: catalogsLoading } = useListCatalogs();
  const { data: schemasResp, isLoading: schemasLoading } = useListSchemas(
    catalog,
    { query: { enabled: !!catalog } },
  );
  const { data: tablesResp, isLoading: tablesLoading } = useListTables(
    catalog,
    schema,
    { query: { enabled: !!catalog && !!schema } },
  );
  const {
    data: columnsResp,
    isLoading: columnsLoading,
    isError: columnsError,
  } = useGetTableColumns(catalog, schema, table, {
    query: { enabled: !!catalog && !!schema && !!table },
  });

  const catalogs: CatalogOut[] = catalogsResp?.data ?? [];
  const schemas: SchemaOut[] = schemasResp?.data ?? [];
  const tables: TableOut[] = tablesResp?.data ?? [];
  const columns: ColumnOut[] = useMemo(
    () => columnsResp?.data ?? [],
    [columnsResp],
  );

  const filteredColumns = useMemo(() => {
    if (!filter.trim()) return columns;
    const q = filter.trim().toLowerCase();
    return columns.filter(
      (c) =>
        c.name.toLowerCase().includes(q) ||
        (c.type_name ?? "").toLowerCase().includes(q),
    );
  }, [columns, filter]);

  const tableFqn =
    catalog && schema && table ? `${catalog}.${schema}.${table}` : "";

  const handleCopy = async (col: ColumnOut) => {
    if (onColumnPick && tableFqn) {
      onColumnPick(col, tableFqn);
      return;
    }
    try {
      await navigator.clipboard.writeText(col.name);
      toast.success(`Copied "${col.name}"`, {
        description: "Paste into the column field.",
        duration: 1500,
      });
    } catch {
      toast.error("Could not copy to clipboard");
    }
  };

  const handleClearTable = () => {
    setTable("");
    setFilter("");
  };

  const header = (
    <>
      <div className="flex items-center justify-between gap-2">
        <button
          type="button"
          onClick={() => setCollapsed(!collapsed)}
          className="flex items-center gap-2 text-sm font-medium hover:text-primary transition-colors"
          aria-expanded={!collapsed}
        >
          {collapsed ? (
            <ChevronRight className="h-4 w-4" />
          ) : (
            <ChevronDown className="h-4 w-4" />
          )}
          <Database className="h-4 w-4 text-sky-600 dark:text-sky-400" />
          <span className="text-sky-900 dark:text-sky-200">
            Column discovery
          </span>
          {collapsed && (
            <span className="text-[11px] text-muted-foreground font-normal">
              (browse columns)
            </span>
          )}
        </button>
        {tableFqn && !collapsed && (
          <Badge
            variant="secondary"
            className="font-mono text-[10px] truncate max-w-[200px]"
          >
            {table}
          </Badge>
        )}
      </div>
      {!collapsed && (
        <p className="text-[11px] text-muted-foreground leading-snug pl-6">
          Quick lookup of columns in any UC table. Click a column to copy its
          name; paste into the column field of any check below.
        </p>
      )}
    </>
  );

  const body = !collapsed && (
    <div className="space-y-3 pt-3">
      {/* Cascading pickers — stack vertically so a long catalog/schema
          name never overflows into the next dropdown. ``w-full`` overrides
          the SelectTrigger's default ``w-fit`` so each picker is bounded
          and long values truncate via the trigger's built-in ``line-clamp-1``. */}
      <div className="space-y-2">
        <Select
          value={catalog}
          onValueChange={(v) => {
            userInitiatedRef.current = true;
            setCatalog(v);
            setSchema("");
            setTable("");
          }}
          disabled={catalogsLoading}
        >
          <SelectTrigger className="h-8 text-xs w-full min-w-0">
            <SelectValue
              placeholder={catalogsLoading ? "Loading…" : "Catalog"}
            />
          </SelectTrigger>
          <SelectContent>
            {catalogs.map((c) => (
              <SelectItem key={c.name} value={c.name} className="text-xs">
                {c.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select
          value={schema}
          onValueChange={(v) => {
            userInitiatedRef.current = true;
            setSchema(v);
            setTable("");
          }}
          disabled={!catalog || schemasLoading}
        >
          <SelectTrigger className="h-8 text-xs w-full min-w-0">
            <SelectValue
              placeholder={
                !catalog ? "Schema" : schemasLoading ? "Loading…" : "Schema"
              }
            />
          </SelectTrigger>
          <SelectContent>
            {schemas.map((s) => (
              <SelectItem key={s.name} value={s.name} className="text-xs">
                {s.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select
          value={table}
          onValueChange={(v) => {
            userInitiatedRef.current = true;
            setTable(v);
          }}
          disabled={!schema || tablesLoading}
        >
          <SelectTrigger className="h-8 text-xs w-full min-w-0">
            <SelectValue
              placeholder={
                !schema ? "Table" : tablesLoading ? "Loading…" : "Table"
              }
            />
          </SelectTrigger>
          <SelectContent>
            {tables.map((t) => (
              <SelectItem key={t.name} value={t.name} className="text-xs">
                {t.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Filter */}
      {table && (
        <div className="relative">
          <Search className="h-3 w-3 text-muted-foreground absolute left-2 top-1/2 -translate-y-1/2" />
          <Input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter columns by name or type…"
            className="h-8 pl-7 pr-7 text-xs"
          />
          {filter && (
            <button
              type="button"
              onClick={() => setFilter("")}
              className="absolute right-1.5 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
              aria-label="Clear filter"
            >
              <X className="h-3 w-3" />
            </button>
          )}
        </div>
      )}

      {/* Columns list */}
      {!table ? (
        <div className="text-[11px] text-muted-foreground italic py-3 text-center border rounded-md bg-background/60">
          Pick a catalog, schema, and table above to see its columns.
        </div>
      ) : columnsLoading ? (
        <div className="space-y-1.5">
          {[1, 2, 3, 4, 5].map((i) => (
            <Skeleton key={i} className="h-7 w-full" />
          ))}
        </div>
      ) : columnsError ? (
        <div className="text-[11px] text-red-500 py-2">
          Failed to load columns for {table}.
        </div>
      ) : filteredColumns.length === 0 ? (
        <div className="text-[11px] text-muted-foreground italic py-3 text-center">
          {filter
            ? `No columns match "${filter}".`
            : "This table has no columns."}
        </div>
      ) : (
        <>
          <div className="flex items-center justify-between text-[11px] text-muted-foreground">
            <span className="truncate">
              {filteredColumns.length} of {columns.length} column
              {columns.length === 1 ? "" : "s"}
              <span
                className="ml-2 font-mono text-muted-foreground/80"
                title={tableFqn}
              >
                · {tableFqn}
              </span>
            </span>
            <button
              type="button"
              onClick={handleClearTable}
              className="hover:text-foreground shrink-0 ml-2"
            >
              Reset
            </button>
          </div>
          <div className="border rounded-md overflow-hidden max-h-[260px] overflow-y-auto bg-background">
            <ul className="divide-y text-xs">
              {filteredColumns.map((col) => (
                <li
                  key={col.name}
                  className="group flex items-center gap-2 px-2 py-1.5 hover:bg-muted/50 cursor-pointer"
                  onClick={() => handleCopy(col)}
                  title={col.comment || `${col.name}: ${col.type_name}`}
                >
                  <div className="flex-1 min-w-0">
                    <div className="font-mono truncate">{col.name}</div>
                    <div className="text-[10px] text-muted-foreground truncate">
                      {col.type_name}
                      {col.nullable === false ? " · NOT NULL" : ""}
                      {col.comment ? ` · ${col.comment}` : ""}
                    </div>
                  </div>
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className="h-6 w-6 p-0 opacity-0 group-hover:opacity-100 transition-opacity"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleCopy(col);
                          }}
                          aria-label={`Copy ${col.name}`}
                        >
                          <Copy className="h-3 w-3" />
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p className="text-xs">Copy column name</p>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                </li>
              ))}
            </ul>
          </div>
        </>
      )}

      {(catalogsLoading || schemasLoading || tablesLoading) && (
        <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground">
          <Loader2 className="h-3 w-3 animate-spin" />
          Loading catalog metadata…
        </div>
      )}
    </div>
  );

  if (variant === "inline") {
    return (
      <div
        className={`border border-sky-200 dark:border-sky-800 rounded-lg p-4 bg-sky-50/50 dark:bg-sky-950/30 space-y-1 ${className ?? ""}`}
      >
        {header}
        {body}
      </div>
    );
  }

  return (
    <Card className={`sticky top-4 ${className ?? ""}`}>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">{header}</CardTitle>
      </CardHeader>
      {!collapsed && (
        <CardContent className="space-y-3 pt-0">{body}</CardContent>
      )}
    </Card>
  );
}

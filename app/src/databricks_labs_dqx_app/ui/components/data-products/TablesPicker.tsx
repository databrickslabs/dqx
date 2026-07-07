/**
 * TablesPicker — a selectable, mini variant of the Monitored Tables list,
 * ported from dqlake's `components/products/TablesPicker.tsx`.
 *
 * Kept deliberately lean (no Edit Columns / column-resize / dnd), mirroring
 * dqlake's structure:
 *   - Leading "Select" column with per-row checkboxes; header toggles the
 *     current filtered page (indeterminate when partially selected).
 *   - Search + Catalog / Schema / Steward filter Selects derived from the
 *     loaded rows (no extra API calls) — matches the app's own filter-bar
 *     convention on the Monitored Tables list (plain shadcn `Select`s, not
 *     a cmdk combobox).
 *   - Client-side pagination (the monitored-tables endpoint returns every
 *     row in one page).
 *   - Footer: "<N> selected" on the left, pagination on the right.
 *
 * Sourced from ALL monitored tables regardless of approval status — per the
 * Data Products design spec §6, unapproved rows are marked "not ready" but
 * stay addable. Props key off `binding_id` (DQX has a real binding concept
 * dqlake doesn't, so there's no need to key off the FQN).
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useListMonitoredTablesSuspense, type MonitoredTableSummaryOut } from "@/lib/api";
import selector from "@/lib/selector";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";

function TruncatedCell({ text, className }: { text: string; className?: string }) {
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
          {text}
        </TooltipContent>
      )}
    </Tooltip>
  );
}

/** Splits a `catalog.schema.table` FQN into its parts, tolerating malformed
 *  values (missing parts render as empty strings). */
function splitFqn(fqn: string): { catalog: string; schema: string } {
  const parts = fqn.split(".");
  return { catalog: parts[0] ?? "", schema: parts[1] ?? "" };
}

interface Props {
  selected: Set<string>;
  onChange: (next: Set<string>) => void;
  /** binding_ids that are already members — pre-checked, greyed, non-interactive. */
  disabledKeys?: Set<string>;
  /** Surface the loaded rows so the parent can read per-table metadata. */
  onRowsLoaded?: (rows: MonitoredTableSummaryOut[]) => void;
}

const PAGE_SIZE = 25;
const SELECT_COL_WIDTH = 48;
const ALL = "ALL";

export function TablesPicker({ selected, onChange, disabledKeys, onRowsLoaded }: Props) {
  const { t } = useTranslation();
  const { data } = useListMonitoredTablesSuspense(undefined, { ...selector<MonitoredTableSummaryOut[]>() });
  const rows = data ?? [];

  useEffect(() => {
    onRowsLoaded?.(rows);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data]);

  const [search, setSearch] = useState("");
  const [catalogFilter, setCatalogFilter] = useState<string>(ALL);
  const [schemaFilter, setSchemaFilter] = useState<string>(ALL);
  const [stewardFilter, setStewardFilter] = useState<string>(ALL);
  const [page, setPage] = useState(0);

  const catalogOptions = useMemo(
    () => Array.from(new Set(rows.map((r) => splitFqn(r.table.table_fqn).catalog))).sort(),
    [rows],
  );
  const schemaOptions = useMemo(() => {
    const scoped =
      catalogFilter === ALL ? rows : rows.filter((r) => splitFqn(r.table.table_fqn).catalog === catalogFilter);
    return Array.from(new Set(scoped.map((r) => splitFqn(r.table.table_fqn).schema))).sort();
  }, [rows, catalogFilter]);
  const stewardOptions = useMemo(
    () => Array.from(new Set(rows.map((r) => r.table.steward).filter((s): s is string => !!s))).sort(),
    [rows],
  );

  // Reset the schema filter if it falls out of range after a catalog change.
  useEffect(() => {
    if (schemaFilter !== ALL && !schemaOptions.includes(schemaFilter)) setSchemaFilter(ALL);
  }, [schemaOptions, schemaFilter]);

  useEffect(() => {
    setPage(0);
  }, [search, catalogFilter, schemaFilter, stewardFilter]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return rows.filter((r) => {
      const { catalog, schema } = splitFqn(r.table.table_fqn);
      if (catalogFilter !== ALL && catalog !== catalogFilter) return false;
      if (schemaFilter !== ALL && schema !== schemaFilter) return false;
      if (stewardFilter !== ALL && (r.table.steward ?? "") !== stewardFilter) return false;
      if (q && !r.table.table_fqn.toLowerCase().includes(q)) return false;
      return true;
    });
  }, [rows, search, catalogFilter, schemaFilter, stewardFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const pageRows = useMemo(() => filtered.slice(page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE), [filtered, page]);

  const selectablePageKeys = pageRows.map((r) => r.table.binding_id).filter((k) => !disabledKeys?.has(k));
  const allPageSelected = selectablePageKeys.length > 0 && selectablePageKeys.every((k) => selected.has(k));
  const somePageSelected = selectablePageKeys.some((k) => selected.has(k));
  const checkedState: boolean | "indeterminate" = allPageSelected
    ? true
    : somePageSelected
      ? "indeterminate"
      : false;

  function toggleAll() {
    const next = new Set(selected);
    if (allPageSelected) {
      selectablePageKeys.forEach((k) => next.delete(k));
    } else {
      selectablePageKeys.forEach((k) => next.add(k));
    }
    onChange(next);
  }

  function toggleRow(key: string) {
    if (disabledKeys?.has(key)) return;
    const next = new Set(selected);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    onChange(next);
  }

  const COL_COUNT = 5; // checkbox + table + catalog + schema + steward

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 flex-wrap">
        <Input
          placeholder={t("monitoredTables.searchTablesPlaceholder")}
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-56 h-9 text-xs"
        />
        <Select value={catalogFilter} onValueChange={setCatalogFilter}>
          <SelectTrigger className="w-44" aria-label={t("monitoredTables.colCatalog")}>
            <SelectValue placeholder={t("monitoredTables.colCatalog")} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL}>{t("monitoredTables.allCatalogs")}</SelectItem>
            {catalogOptions.map((c) => (
              <SelectItem key={c} value={c}>
                {c}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={schemaFilter} onValueChange={setSchemaFilter}>
          <SelectTrigger className="w-44" aria-label={t("monitoredTables.colSchema")}>
            <SelectValue placeholder={t("monitoredTables.colSchema")} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL}>{t("monitoredTables.allSchemas")}</SelectItem>
            {schemaOptions.map((s) => (
              <SelectItem key={s} value={s}>
                {s}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={stewardFilter} onValueChange={setStewardFilter}>
          <SelectTrigger className="w-52" aria-label={t("dataProducts.colSteward")}>
            <SelectValue placeholder={t("dataProducts.colSteward")} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL}>{t("dataProducts.allStewards")}</SelectItem>
            {stewardOptions.map((s) => (
              <SelectItem key={s} value={s}>
                {s}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="overflow-x-auto min-h-[26rem]">
        <Table className="table-fixed w-full">
          <colgroup>
            <col style={{ width: SELECT_COL_WIDTH }} />
            <col style={{ width: 360 }} />
            <col style={{ width: 160 }} />
            <col style={{ width: 160 }} />
            <col style={{ width: 200 }} />
          </colgroup>
          <TableHeader>
            <TableRow>
              <TableHead
                style={{ width: SELECT_COL_WIDTH, minWidth: SELECT_COL_WIDTH, maxWidth: SELECT_COL_WIDTH }}
                className="text-center"
              >
                <Checkbox
                  checked={checkedState}
                  onCheckedChange={toggleAll}
                  disabled={selectablePageKeys.length === 0}
                  aria-label={t("dataProducts.pickerSelectAllAria")}
                />
              </TableHead>
              <TableHead>{t("monitoredTables.colTableName")}</TableHead>
              <TableHead>{t("monitoredTables.colCatalog")}</TableHead>
              <TableHead>{t("monitoredTables.colSchema")}</TableHead>
              <TableHead>{t("dataProducts.colSteward")}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {pageRows.map((r) => {
              const key = r.table.binding_id;
              const { catalog, schema } = splitFqn(r.table.table_fqn);
              const isDisabled = disabledKeys?.has(key) ?? false;
              const isChecked = isDisabled || selected.has(key);
              const notReady = r.table.status !== "approved";
              return (
                <TableRow
                  key={key}
                  className={cn(isDisabled ? "cursor-not-allowed opacity-50" : "cursor-pointer hover:bg-muted/50")}
                  data-selected={selected.has(key) || undefined}
                  onClick={() => toggleRow(key)}
                >
                  <TableCell
                    style={{ width: SELECT_COL_WIDTH, minWidth: SELECT_COL_WIDTH, maxWidth: SELECT_COL_WIDTH }}
                    className="text-center"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <Checkbox
                      checked={isChecked}
                      disabled={isDisabled}
                      onCheckedChange={() => toggleRow(key)}
                      aria-label={t("dataProducts.pickerSelectRowAria", { table: r.table.table_fqn })}
                    />
                  </TableCell>
                  <TableCell className="overflow-hidden">
                    <span className="inline-flex items-center gap-2 max-w-full">
                      <TruncatedCell text={r.table.table_fqn} className="font-mono text-xs" />
                      {notReady && (
                        <Badge variant="outline" className="text-[10px] shrink-0">
                          {t("dataProducts.pickerNotReadyBadge")}
                        </Badge>
                      )}
                    </span>
                  </TableCell>
                  <TableCell className="overflow-hidden">
                    <TruncatedCell text={catalog} />
                  </TableCell>
                  <TableCell className="overflow-hidden">
                    <TruncatedCell text={schema} />
                  </TableCell>
                  <TableCell className="overflow-hidden">
                    <TruncatedCell text={r.table.steward ?? "—"} />
                  </TableCell>
                </TableRow>
              );
            })}
            {pageRows.length === 0 && rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={COL_COUNT} className="text-center text-muted-foreground py-8">
                  {t("dataProducts.pickerNoTables")}
                </TableCell>
              </TableRow>
            )}
            {pageRows.length === 0 && rows.length > 0 && (
              <TableRow>
                <TableCell colSpan={COL_COUNT} className="text-center text-muted-foreground py-8">
                  {t("dataProducts.pickerNoMatches")}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      <div className="flex items-center justify-between text-sm text-muted-foreground pt-2">
        <span>{t("dataProducts.pickerSelectedCount", { count: selected.size })}</span>
        {filtered.length > PAGE_SIZE && (
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              disabled={page === 0}
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              aria-label={t("dataProducts.pickerPrevPage")}
            >
              <ChevronLeft className="h-4 w-4" />
              {t("dataProducts.pickerPrevPage")}
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={page >= totalPages - 1}
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              aria-label={t("dataProducts.pickerNextPage")}
            >
              {t("dataProducts.pickerNextPage")}
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}

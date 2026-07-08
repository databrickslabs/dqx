/**
 * TablesPicker — the Table Spaces "Add tables" picker.
 *
 * Layout resurrected from main branch's old Run Rules → Table Selection
 * screen (`routes/_sidebar/runs.tsx`'s `ExecuteTab`/`RuleTable`, pre-Table-
 * Spaces): a filter toolbar (Group by / catalog / schema / an extra facet /
 * search / Select all / Clear) above one card per group, each card holding a
 * mini table of checkbox | table | count | status rows. Ported to this
 * dialog's actual data source — monitored tables, not approved rule sets —
 * so the extra facet is Steward (there's no label/severity concept on a
 * monitored table) and the "Rules" column shows `applied_rule_count`
 * instead of a checks-array length. Grouping is client-side over the single
 * `listMonitoredTables` page (matches dqlake's pattern; no separate paged
 * fetch per group).
 *
 * Preserves the pre-existing contract dqlake's port relied on:
 *   - Sourced from ALL monitored tables regardless of approval status — per
 *     the Table Spaces design spec §6, unapproved rows are marked "not
 *     ready" but stay addable.
 *   - Already-member rows (`disabledKeys`) render checked + non-interactive.
 *   - Props key off `binding_id` (DQX has a real binding concept dqlake
 *     doesn't, so there's no need to key off the FQN).
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Database, Layers, Table2 } from "lucide-react";
import { useListMonitoredTablesSuspense, type MonitoredTableSummaryOut } from "@/lib/api";
import selector from "@/lib/selector";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { StatusBadge } from "@/components/RegistryRuleBadges";
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

type GroupMode = "catalog" | "schema" | "none";

const ALL = "ALL";

export function TablesPicker({ selected, onChange, disabledKeys, onRowsLoaded }: Props) {
  const { t } = useTranslation();
  const { data } = useListMonitoredTablesSuspense(undefined, { ...selector<MonitoredTableSummaryOut[]>() });
  const rows = data ?? [];

  useEffect(() => {
    onRowsLoaded?.(rows);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data]);

  const [groupBy, setGroupBy] = useState<GroupMode>("catalog");
  const [search, setSearch] = useState("");
  const [catalogFilter, setCatalogFilter] = useState<string>(ALL);
  const [schemaFilter, setSchemaFilter] = useState<string>(ALL);
  const [stewardFilter, setStewardFilter] = useState<string>(ALL);

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

  const grouped = useMemo((): Map<string, MonitoredTableSummaryOut[]> => {
    if (groupBy === "none") {
      return filtered.length > 0
        ? new Map<string, MonitoredTableSummaryOut[]>([[t("dataProducts.pickerAllGroup"), filtered]])
        : new Map<string, MonitoredTableSummaryOut[]>();
    }
    const groups = new Map<string, MonitoredTableSummaryOut[]>();
    for (const r of filtered) {
      const { catalog, schema } = splitFqn(r.table.table_fqn);
      const key = groupBy === "catalog" ? catalog || t("dataProducts.pickerUnknownGroup") : `${catalog}.${schema}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(r);
    }
    return new Map([...groups.entries()].sort(([a], [b]) => a.localeCompare(b)));
  }, [filtered, groupBy, t]);

  function toggleRow(key: string) {
    if (disabledKeys?.has(key)) return;
    const next = new Set(selected);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    onChange(next);
  }

  function toggleGroup(groupRows: MonitoredTableSummaryOut[]) {
    const selectableKeys = groupRows.map((r) => r.table.binding_id).filter((k) => !disabledKeys?.has(k));
    const allSelected = selectableKeys.length > 0 && selectableKeys.every((k) => selected.has(k));
    const next = new Set(selected);
    if (allSelected) {
      selectableKeys.forEach((k) => next.delete(k));
    } else {
      selectableKeys.forEach((k) => next.add(k));
    }
    onChange(next);
  }

  function selectAll() {
    const selectableKeys = filtered.map((r) => r.table.binding_id).filter((k) => !disabledKeys?.has(k));
    onChange(new Set([...selected, ...selectableKeys]));
  }

  function clearAll() {
    onChange(new Set());
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 flex-wrap">
        <div className="flex items-center gap-1.5">
          <Layers className="h-3.5 w-3.5 text-muted-foreground" />
          <span className="text-xs text-muted-foreground">{t("dataProducts.pickerGroupByLabel")}</span>
          <Select value={groupBy} onValueChange={(v) => setGroupBy(v as GroupMode)}>
            <SelectTrigger className="w-[120px] h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="catalog" className="text-xs">
                <span className="flex items-center gap-1.5">
                  <Database className="h-3 w-3" /> {t("dataProducts.pickerGroupByCatalog")}
                </span>
              </SelectItem>
              <SelectItem value="schema" className="text-xs">
                <span className="flex items-center gap-1.5">
                  <Layers className="h-3 w-3" /> {t("dataProducts.pickerGroupBySchema")}
                </span>
              </SelectItem>
              <SelectItem value="none" className="text-xs">
                <span className="flex items-center gap-1.5">
                  <Table2 className="h-3 w-3" /> {t("dataProducts.pickerGroupByFlat")}
                </span>
              </SelectItem>
            </SelectContent>
          </Select>
        </div>

        <Select value={catalogFilter} onValueChange={(v) => { setCatalogFilter(v); setSchemaFilter(ALL); }}>
          <SelectTrigger className="w-40 h-8 text-xs" aria-label={t("monitoredTables.colCatalog")}>
            <SelectValue placeholder={t("monitoredTables.colCatalog")} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL} className="text-xs">
              {t("monitoredTables.allCatalogs")}
            </SelectItem>
            {catalogOptions.map((c) => (
              <SelectItem key={c} value={c} className="text-xs">
                {c}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={schemaFilter} onValueChange={setSchemaFilter}>
          <SelectTrigger className="w-40 h-8 text-xs" aria-label={t("monitoredTables.colSchema")}>
            <SelectValue placeholder={t("monitoredTables.colSchema")} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL} className="text-xs">
              {t("monitoredTables.allSchemas")}
            </SelectItem>
            {schemaOptions.map((s) => (
              <SelectItem key={s} value={s} className="text-xs">
                {s}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={stewardFilter} onValueChange={setStewardFilter}>
          <SelectTrigger className="w-44 h-8 text-xs" aria-label={t("dataProducts.colSteward")}>
            <SelectValue placeholder={t("dataProducts.colSteward")} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL} className="text-xs">
              {t("dataProducts.allStewards")}
            </SelectItem>
            {stewardOptions.map((s) => (
              <SelectItem key={s} value={s} className="text-xs">
                {s}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Input
          placeholder={t("monitoredTables.searchTablesPlaceholder")}
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-48 h-8 text-xs"
        />

        <div className="flex items-center gap-1.5 ml-auto">
          <Button variant="ghost" size="sm" className="h-8 text-xs" onClick={selectAll}>
            {t("dataProducts.pickerSelectAll")}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 text-xs"
            onClick={clearAll}
            disabled={selected.size === 0}
          >
            {t("dataProducts.pickerClear")}
          </Button>
        </div>
      </div>

      <div className="space-y-3 min-h-[20rem]">
        {filtered.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground text-sm">
            {rows.length === 0 ? t("dataProducts.pickerNoTables") : t("dataProducts.pickerNoMatches")}
          </div>
        ) : groupBy === "none" ? (
          <Table className="table-fixed w-full">
            <colgroup>
              <col style={{ width: 48 }} />
              <col />
              <col style={{ width: 100 }} />
              <col style={{ width: 140 }} />
            </colgroup>
            <TableHeader>
              <TableRow>
                <TableHead style={{ width: 48 }} className="text-center" />
                <TableHead>{t("monitoredTables.colTableName")}</TableHead>
                <TableHead className="text-right">{t("dataProducts.colRules")}</TableHead>
                <TableHead>{t("dataProducts.colStatus")}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map((r) => {
                const key = r.table.binding_id;
                const isDisabled = disabledKeys?.has(key) ?? false;
                const isChecked = isDisabled || selected.has(key);
                const notReady = r.table.status !== "approved";
                return (
                  <TableRow
                    key={key}
                    className={cn(
                      isDisabled ? "cursor-not-allowed opacity-50" : "cursor-pointer hover:bg-muted/50",
                      !isDisabled && selected.has(key) && "bg-primary/5",
                    )}
                    data-selected={selected.has(key) || undefined}
                    onClick={() => toggleRow(key)}
                  >
                    <TableCell className="text-center" onClick={(e) => e.stopPropagation()}>
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
                    <TableCell className="text-right tabular-nums text-xs">
                      {r.applied_rule_count ?? 0}
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={r.table.status} />
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        ) : (
          Array.from(grouped.entries()).map(([group, groupRows]) => {
            const selectableKeys = groupRows.map((r) => r.table.binding_id).filter((k) => !disabledKeys?.has(k));
            const allSelected = selectableKeys.length > 0 && selectableKeys.every((k) => selected.has(k));
            const someSelected = selectableKeys.some((k) => selected.has(k));
            return (
              <div key={group} className="border rounded-lg overflow-hidden">
                <div className="flex items-center gap-3 p-3 bg-muted/40 border-b">
                  <Checkbox
                    checked={allSelected ? true : someSelected ? "indeterminate" : false}
                    onCheckedChange={() => toggleGroup(groupRows)}
                    disabled={selectableKeys.length === 0}
                    aria-label={t("dataProducts.pickerSelectGroupAria", { group })}
                  />
                  <div className="flex items-center gap-2">
                    {groupBy === "catalog" && <Database className="h-3.5 w-3.5 text-muted-foreground" />}
                    {groupBy === "schema" && <Layers className="h-3.5 w-3.5 text-muted-foreground" />}
                    <span className="text-sm font-medium">{group}</span>
                  </div>
                  <Badge variant="secondary" className="ml-auto text-xs">
                    {t("dataProducts.pickerGroupTablesCount", { count: groupRows.length })}
                  </Badge>
                </div>
                <Table className="table-fixed w-full">
                  <colgroup>
                    <col style={{ width: 48 }} />
                    <col />
                    <col style={{ width: 100 }} />
                    <col style={{ width: 140 }} />
                  </colgroup>
                  <TableHeader>
                    <TableRow>
                      <TableHead style={{ width: 48 }} className="text-center" />
                      <TableHead>{t("monitoredTables.colTableName")}</TableHead>
                      <TableHead className="text-right">{t("dataProducts.colRules")}</TableHead>
                      <TableHead>{t("dataProducts.colStatus")}</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {groupRows.map((r) => {
                      const key = r.table.binding_id;
                      const isDisabled = disabledKeys?.has(key) ?? false;
                      const isChecked = isDisabled || selected.has(key);
                      const notReady = r.table.status !== "approved";
                      return (
                        <TableRow
                          key={key}
                          className={cn(
                            isDisabled ? "cursor-not-allowed opacity-50" : "cursor-pointer hover:bg-muted/50",
                            !isDisabled && selected.has(key) && "bg-primary/5",
                          )}
                          data-selected={selected.has(key) || undefined}
                          onClick={() => toggleRow(key)}
                        >
                          <TableCell className="text-center" onClick={(e) => e.stopPropagation()}>
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
                          <TableCell className="text-right tabular-nums text-xs">
                            {r.applied_rule_count ?? 0}
                          </TableCell>
                          <TableCell>
                            <StatusBadge status={r.table.status} />
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </div>
            );
          })
        )}
      </div>

      <div className="flex items-center justify-between text-sm text-muted-foreground pt-2">
        <span>{t("dataProducts.pickerSelectedCount", { count: selected.size })}</span>
      </div>
    </div>
  );
}

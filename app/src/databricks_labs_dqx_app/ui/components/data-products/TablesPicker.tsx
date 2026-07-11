/**
 * TablesPicker — the Table Spaces "Add tables" picker.
 *
 * Layout resurrected from main branch's old Run Rules → Table Selection
 * screen (`routes/_sidebar/runs.tsx`'s `ExecuteTab`/`RuleTable`, pre-Table-
 * Spaces): a filter toolbar (a "select all" toggle / Group by / catalog /
 * schema / an extra facet / search) above one card per group, each card
 * holding a mini table of checkbox | table | count | status rows. Ported to this
 * dialog's actual data source — monitored tables, not approved rule sets —
 * so the extra facet is Steward (there's no label/severity concept on a
 * monitored table) and the "Rules" column shows `applied_rule_count`
 * instead of a checks-array length. Grouping is client-side over the single
 * `listMonitoredTables` page (matches dqlake's pattern; no separate paged
 * fetch per group).
 *
 * Preserves the pre-existing contract dqlake's port relied on:
 *   - Sourced from ALL monitored tables regardless of approval status, but
 *     (P3.2 — superseding the design spec §6 "stay addable" behaviour) rows
 *     that are not approved render disabled with a "not ready" badge +
 *     tooltip and cannot be selected. Eligibility matches the backend's
 *     add_member guard exactly: status === "approved" AND version > 0 — the
 *     same predicate as `DataProductMemberOut.runnable`.
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
import { Checkbox } from "@/components/ui/checkbox";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { StatusBadge } from "@/components/RegistryRuleBadges";
import { MemberVersionPin } from "@/components/data-products/MemberVersionPin";
import { cn } from "@/lib/utils";

/**
 * Truncated single-line cell with a hover tooltip.
 *
 * The tooltip normally only appears when the displayed text overflows its
 * box. When *fullText* is supplied and differs from the (possibly
 * group-by-trimmed) displayed *text* — see item 22's FQN trimming — the
 * tooltip always shows *fullText*, regardless of overflow, so the full FQN
 * stays reachable even when the trimmed text fits comfortably.
 */
function TruncatedCell({ text, fullText, className }: { text: string; fullText?: string; className?: string }) {
  const ref = useRef<HTMLSpanElement>(null);
  const [overflow, setOverflow] = useState(false);
  const tooltipText = fullText ?? text;
  const alwaysShowTooltip = fullText !== undefined && fullText !== text;

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
      {(overflow || alwaysShowTooltip) && (
        <TooltipContent side="top" className="max-w-md text-wrap break-words text-left">
          {tooltipText}
        </TooltipContent>
      )}
    </Tooltip>
  );
}

/**
 * Trims a `catalog.schema.table` FQN to match the active group-by scope
 * (item 22) — the group card / heading already carries the trimmed-off
 * prefix, so repeating it in every row is noise:
 *   - group=catalog  → "schema.table"
 *   - group=schema   → "table"
 *   - group=none     → full FQN unchanged
 * Tolerates malformed FQNs by falling back to the full string.
 */
function trimFqnForGroup(fqn: string, groupBy: GroupMode): string {
  if (groupBy === "none") return fqn;
  const parts = fqn.split(".");
  const trimmed = groupBy === "catalog" ? parts.slice(1).join(".") : parts.slice(2).join(".");
  return trimmed || fqn;
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
  /** Per-table version pin choice keyed by binding_id (P24 item 16 — the
   *  dialog's old standalone "Version to track" section, now rendered
   *  inline per row instead). Absent/null = use latest. */
  pins: Map<string, number | null>;
  onPinChange: (bindingId: string, version: number | null) => void;
}

type GroupMode = "catalog" | "schema" | "none";

const ALL = "ALL";

/**
 * P3.2 eligibility: only approved bindings (status "approved" AND version > 0
 * — the backend's `_is_runnable` / `DataProductMemberOut.runnable` predicate)
 * can join a table space; the backend rejects the rest with a 400. A binding
 * shown as "modified" (approved with unapproved edits) is still approved
 * underneath and stays eligible.
 */
function isEligible(r: MonitoredTableSummaryOut): boolean {
  return r.table.status === "approved" && (r.table.version ?? 0) > 0;
}

export function TablesPicker({ selected, onChange, disabledKeys, onRowsLoaded, pins, onPinChange }: Props) {
  const { t } = useTranslation();
  const { data } = useListMonitoredTablesSuspense(undefined, { ...selector<MonitoredTableSummaryOut[]>() });
  const rows = data ?? [];

  useEffect(() => {
    onRowsLoaded?.(rows);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data]);

  // Non-approved bindings can't be selected (P3.2) — see isEligible above.
  const ineligibleKeys = useMemo(() => {
    const s = new Set<string>();
    for (const r of rows) if (!isEligible(r)) s.add(r.table.binding_id);
    return s;
  }, [rows]);
  const isSelectable = (key: string) => !disabledKeys?.has(key) && !ineligibleKeys.has(key);

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
    if (!isSelectable(key)) return;
    const next = new Set(selected);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    onChange(next);
  }

  function toggleGroup(groupRows: MonitoredTableSummaryOut[]) {
    const selectableKeys = groupRows.map((r) => r.table.binding_id).filter(isSelectable);
    const allSelected = selectableKeys.length > 0 && selectableKeys.every((k) => selected.has(k));
    const next = new Set(selected);
    if (allSelected) {
      selectableKeys.forEach((k) => next.delete(k));
    } else {
      selectableKeys.forEach((k) => next.add(k));
    }
    onChange(next);
  }

  // All-selected state for the global "select all" toggle — now a tri-state
  // Checkbox (P24 item 14, reversing P23-D item 19's Switch), matching each
  // group card's own checked/indeterminate Checkbox, just scoped to the
  // whole filtered set instead of one group. Checking it on adds all
  // filtered selectable rows to the existing selection (selections outside
  // the current filter are left untouched); unchecking it clears the
  // selection entirely, same as the old "Clear" button.
  const allFilteredSelectableKeys = useMemo(
    () => filtered.map((r) => r.table.binding_id).filter((k) => !disabledKeys?.has(k) && !ineligibleKeys.has(k)),
    [filtered, disabledKeys, ineligibleKeys],
  );
  const allFilteredSelected =
    allFilteredSelectableKeys.length > 0 && allFilteredSelectableKeys.every((k) => selected.has(k));
  const someFilteredSelected = allFilteredSelectableKeys.some((k) => selected.has(k));

  function toggleSelectAll(checked: boolean) {
    if (checked) {
      onChange(new Set([...selected, ...allFilteredSelectableKeys]));
    } else {
      onChange(new Set());
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 flex-wrap">
        {/* Checkbox only — the "Select all" text label was dropped (P25
            item 3d); the aria-label keeps it accessible. */}
        <Checkbox
          checked={allFilteredSelected ? true : someFilteredSelected ? "indeterminate" : false}
          onCheckedChange={() => toggleSelectAll(!allFilteredSelected)}
          disabled={allFilteredSelectableKeys.length === 0}
          aria-label={t("dataProducts.pickerSelectAllToggleAria")}
        />

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

        {/* Separator's own `data-[orientation=vertical]:h-full` utility
            outranks a plain `h-6` because an attribute-selector variant has
            higher specificity than a bare class, regardless of source order,
            and `h-full` resolves to 0 against this row's auto height —
            force it with `!h-6`. */}
        <Separator orientation="vertical" className="!h-6" />

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
                const isMember = disabledKeys?.has(key) ?? false;
                const notEligible = ineligibleKeys.has(key);
                // Members render checked; ineligible non-members render
                // unchecked but equally non-interactive (P3.2).
                const isDisabled = isMember || notEligible;
                const isChecked = isMember || selected.has(key);
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
                        {notEligible && (
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Badge variant="outline" className="text-[10px] shrink-0">
                                {t("dataProducts.pickerNotReadyBadge")}
                              </Badge>
                            </TooltipTrigger>
                            <TooltipContent side="top" className="max-w-xs text-wrap">
                              {t("dataProducts.pickerNotEligibleTooltip")}
                            </TooltipContent>
                          </Tooltip>
                        )}
                        {/* Version pin, inline right after the table name —
                            P24 item 16 (replaces the old standalone "Version
                            to track" section). Shown once the row is
                            checked, same reveal condition as "not ready". */}
                        {!isDisabled && selected.has(key) && (
                          <span className="shrink-0" onClick={(e) => e.stopPropagation()}>
                            <MemberVersionPin
                              bindingVersion={r.table.version ?? 0}
                              pinnedVersion={pins.get(key) ?? null}
                              onPinChange={(v) => onPinChange(key, v)}
                            />
                          </span>
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
            const selectableKeys = groupRows.map((r) => r.table.binding_id).filter(isSelectable);
            const allSelected = selectableKeys.length > 0 && selectableKeys.every((k) => selected.has(k));
            const someSelected = selectableKeys.some((k) => selected.has(k));
            return (
              <div key={group} className="border rounded-lg overflow-hidden">
                {/* Item 21: clicking anywhere on the top bar toggles the
                    group's selection, not just the checkbox — the checkbox
                    stays independently clickable via stopPropagation below
                    so it doesn't double-toggle. */}
                <div
                  className="flex items-center gap-3 p-3 bg-muted/40 border-b cursor-pointer hover:bg-muted/60"
                  onClick={() => toggleGroup(groupRows)}
                >
                  <span onClick={(e) => e.stopPropagation()}>
                    <Checkbox
                      checked={allSelected ? true : someSelected ? "indeterminate" : false}
                      onCheckedChange={() => toggleGroup(groupRows)}
                      disabled={selectableKeys.length === 0}
                      aria-label={t("dataProducts.pickerSelectGroupAria", { group })}
                    />
                  </span>
                  <div className="flex items-center gap-2">
                    {groupBy === "catalog" && <Database className="h-3.5 w-3.5 text-muted-foreground" />}
                    {groupBy === "schema" && <Layers className="h-3.5 w-3.5 text-muted-foreground" />}
                    <span className="text-sm font-medium">{group}</span>
                    {/* Count badge sits right next to the group name (P25
                        item 3c) instead of pushed to the far edge. */}
                    <Badge variant="secondary" className="text-xs">
                      {t("dataProducts.pickerGroupTablesCount", { count: groupRows.length })}
                    </Badge>
                  </div>
                </div>
                {/* Per-group header row (P29 item 40, reversing P25 item 3a):
                    the "#Rules"/"Status" column labels live in a TableHeader
                    directly above each group's rows so they read as that
                    group's columns, instead of floating once in the toolbar
                    detached from the values. Flat mode has its own in-table
                    header; the colgroup here matches it. */}
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
                      const isMember = disabledKeys?.has(key) ?? false;
                      const notEligible = ineligibleKeys.has(key);
                      // Members render checked; ineligible non-members render
                      // unchecked but equally non-interactive (P3.2).
                      const isDisabled = isMember || notEligible;
                      const isChecked = isMember || selected.has(key);
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
                              <TruncatedCell
                                text={trimFqnForGroup(r.table.table_fqn, groupBy)}
                                fullText={r.table.table_fqn}
                                className="font-mono text-xs"
                              />
                              {notEligible && (
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <Badge variant="outline" className="text-[10px] shrink-0">
                                      {t("dataProducts.pickerNotReadyBadge")}
                                    </Badge>
                                  </TooltipTrigger>
                                  <TooltipContent side="top" className="max-w-xs text-wrap">
                                    {t("dataProducts.pickerNotEligibleTooltip")}
                                  </TooltipContent>
                                </Tooltip>
                              )}
                              {/* Version pin, inline right after the table
                                  name — P24 item 16. Shown once the row is
                                  checked, same reveal condition as "not
                                  ready". */}
                              {!isDisabled && selected.has(key) && (
                                <span className="shrink-0" onClick={(e) => e.stopPropagation()}>
                                  <MemberVersionPin
                                    bindingVersion={r.table.version ?? 0}
                                    pinnedVersion={pins.get(key) ?? null}
                                    onPinChange={(v) => onPinChange(key, v)}
                                  />
                                </span>
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

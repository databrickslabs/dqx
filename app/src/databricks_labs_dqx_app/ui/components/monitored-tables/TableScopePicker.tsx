import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueries } from "@tanstack/react-query";
import { MultiSelectPopover } from "@/components/monitored-tables/MultiSelectPopover";
import {
  useListCatalogs,
  useListMonitoredTables,
  getListSchemasQueryOptions,
  getListTablesQueryOptions,
} from "@/lib/api";

/** Splits a "catalog.schema" scope key back into its two parts. */
function splitScope(scope: string): [string, string] {
  const dotIndex = scope.indexOf(".");
  return [scope.slice(0, dotIndex), scope.slice(dotIndex + 1)];
}

export interface TableScopePickerState {
  selectedCatalogs: string[];
  setSelectedCatalogs: (v: string[]) => void;
  selectedSchemas: string[];
  setSelectedSchemas: (v: string[]) => void;
  selectedTables: string[];
  setSelectedTables: (v: string[]) => void;
  catalogOptions: { value: string; label: string }[];
  catalogsLoading: boolean;
  schemaOptions: { value: string; label: string }[];
  schemasLoading: boolean;
  tableOptions: { value: string; label: string }[];
  tablesLoading: boolean;
  /** Resolved table FQNs: explicit table picks, else every table under the resolved schema scope. */
  effectiveFqns: string[];
  reset: () => void;
}

/**
 * Cascading catalog → schema → table multiselect state, shared by every
 * flow that lets a user scope a bulk operation to a set of tables — the
 * monitored-tables "Monitor table(s)" wizard (Phase 7D-c) and the Rules
 * Registry "Apply to tables" modal (Phase 7D-d). Extracted here instead of
 * duplicated so the two flows can't drift in behavior (e.g. the "no
 * schemas picked = every schema under selected catalogs" fallback).
 *
 * ``enabled`` should track the host dialog's open state so background
 * catalog/schema/table fetches only run while the picker is visible.
 */
export function useTableScopePicker(enabled: boolean): TableScopePickerState {
  const { t } = useTranslation();
  const [selectedCatalogs, setSelectedCatalogs] = useState<string[]>([]);
  const [selectedSchemas, setSelectedSchemas] = useState<string[]>([]);
  const [selectedTables, setSelectedTables] = useState<string[]>([]);

  const { data: catalogsResp, isLoading: catalogsLoading } = useListCatalogs({
    query: { enabled },
  });
  const catalogOptions = useMemo(
    () => (catalogsResp?.data ?? []).map((c) => ({ value: c.name, label: c.name })),
    [catalogsResp],
  );

  // `combine` gives React Query's own structural-sharing memoization of the
  // derived result, so `schemaOptions`/`schemasLoading` only get new
  // references when the underlying query data actually changes — without
  // it, `useQueries` returns a fresh array on every render, which cascades
  // into the child useEffects below re-firing forever (infinite update loop).
  const { data: schemaOptions, isLoading: schemasLoading } = useQueries({
    queries: selectedCatalogs.map((catalog) =>
      getListSchemasQueryOptions(catalog, { query: { enabled } }),
    ),
    combine: (results) => {
      const opts: { value: string; label: string }[] = [];
      selectedCatalogs.forEach((catalog, i) => {
        (results[i]?.data?.data ?? []).forEach((s) => {
          opts.push({ value: `${catalog}.${s.name}`, label: `${catalog}.${s.name}` });
        });
      });
      return {
        data: opts,
        isLoading: results.some((r) => r.isLoading),
      };
    },
  });

  // Resolved schema scope: user-picked schemas if any, otherwise every schema
  // under the selected catalogs (so picking only catalogs scopes to everything
  // beneath them).
  const resolvedSchemaScopes = useMemo(
    () => (selectedSchemas.length > 0 ? selectedSchemas : schemaOptions.map((o) => o.value)),
    [selectedSchemas, schemaOptions],
  );

  // Same `combine` memoization rationale as the schema queries above.
  const { data: rawTableOptions, isLoading: tablesLoading } = useQueries({
    queries: resolvedSchemaScopes.map((scope) => {
      const [catalog, schema] = splitScope(scope);
      return getListTablesQueryOptions(catalog, schema, { query: { enabled } });
    }),
    combine: (results) => {
      const opts: { value: string; label: string }[] = [];
      resolvedSchemaScopes.forEach((scope, i) => {
        (results[i]?.data?.data ?? []).forEach((tbl) => {
          opts.push({ value: `${scope}.${tbl.name}`, label: `${scope}.${tbl.name}` });
        });
      });
      return {
        data: opts,
        isLoading: results.some((r) => r.isLoading),
      };
    },
  });

  // Tables that already have a monitor appear pre-selected + disabled in the
  // picker below (with a tooltip explaining why) so a user can't accidentally
  // create a duplicate monitor — matching dqlake's `TablePickerInline`
  // `markMonitored` behavior. Reuses the same unfiltered list query the
  // Monitored Tables overview page runs, rather than a bespoke endpoint.
  const { data: monitoredResp } = useListMonitoredTables({}, { query: { enabled } });
  const monitoredFqns = useMemo(
    () => new Set((monitoredResp?.data ?? []).map((r) => r.table.table_fqn)),
    [monitoredResp],
  );
  const tableOptions = useMemo(
    () =>
      rawTableOptions.map((o) =>
        monitoredFqns.has(o.value)
          ? { ...o, disabled: true, disabledReason: t("monitoredTables.wizard.alreadyMonitoredTooltip") }
          : o,
      ),
    [rawTableOptions, monitoredFqns, t],
  );

  // Memoized for the same reason as `resolvedSchemaScopes` above: a fresh
  // array on every render (via `.map`) would give consumers an unstable
  // reference even when nothing selection-relevant changed.
  //
  // The "no explicit table selection" fallback must exclude already-monitored
  // (disabled) tables — they can't be explicitly selected either, so falling
  // back to the full unfiltered list would overstate the resulting "Add N
  // tables" count and submit tables that are already monitored (the backend
  // dedups them as skipped, but the count shown to the user would be wrong).
  const effectiveFqns = useMemo(
    () =>
      selectedTables.length > 0
        ? selectedTables
        : tableOptions.filter((o) => !("disabled" in o && o.disabled)).map((o) => o.value),
    [selectedTables, tableOptions],
  );

  const reset = useCallback(() => {
    setSelectedCatalogs([]);
    setSelectedSchemas([]);
    setSelectedTables([]);
  }, []);

  // Clear child selections when their parent scope narrows so state never
  // references catalogs/schemas that are no longer selected.
  //
  // Both setters return the *same* array reference (`prev`) when nothing
  // was actually filtered out — `Array.prototype.filter` always allocates
  // a new array, and handing that back unconditionally would make the
  // state "change" (by reference) on every run even when its contents are
  // identical. That matters here because the resulting state feeds back
  // into `resolvedSchemaScopes` / the table-query `useQueries` above,
  // which can retrigger these very effects — an unstable reference is
  // exactly the shape of bug that caused the intermittent "Maximum update
  // depth exceeded" loop in this picker (see the `combine` fix on the
  // queries above for the sibling half of that fix).
  useEffect(() => {
    const catalogSet = new Set(selectedCatalogs);
    setSelectedSchemas((prev) => {
      const next = prev.filter((s) => catalogSet.has(splitScope(s)[0]));
      return next.length === prev.length ? prev : next;
    });
  }, [selectedCatalogs]);

  useEffect(() => {
    const scopeSet = new Set(resolvedSchemaScopes);
    setSelectedTables((prev) => {
      const next = prev.filter((fqn) => scopeSet.has(fqn.slice(0, fqn.lastIndexOf("."))));
      return next.length === prev.length ? prev : next;
    });
  }, [resolvedSchemaScopes]);

  return {
    selectedCatalogs,
    setSelectedCatalogs,
    selectedSchemas,
    setSelectedSchemas,
    selectedTables,
    setSelectedTables,
    catalogOptions,
    catalogsLoading,
    schemaOptions,
    schemasLoading,
    tableOptions,
    tablesLoading,
    effectiveFqns,
    reset,
  };
}

/** Renders the three cascading multiselects + scope summary hint for a {@link useTableScopePicker} state. */
export function TableScopePickerFields({ state }: { state: TableScopePickerState }) {
  const { t } = useTranslation();
  return (
    <>
      <MultiSelectPopover
        label={t("monitoredTables.wizard.catalogsLabel")}
        placeholder={t("monitoredTables.wizard.catalogsPlaceholder")}
        searchPlaceholder={t("monitoredTables.wizard.searchCatalogs")}
        options={state.catalogOptions}
        selected={state.selectedCatalogs}
        onChange={state.setSelectedCatalogs}
        isLoading={state.catalogsLoading}
        emptyText={t("monitoredTables.wizard.emptyCatalogs")}
      />
      <MultiSelectPopover
        label={t("monitoredTables.wizard.schemasLabel")}
        placeholder={t("monitoredTables.wizard.schemasPlaceholder", { count: state.selectedCatalogs.length })}
        searchPlaceholder={t("monitoredTables.wizard.searchSchemas")}
        options={state.schemaOptions}
        selected={state.selectedSchemas}
        onChange={state.setSelectedSchemas}
        isLoading={state.schemasLoading}
        disabled={state.selectedCatalogs.length === 0}
        disabledHint={t("monitoredTables.wizard.selectCatalogFirst")}
        emptyText={t("monitoredTables.wizard.emptySchemas")}
      />
      <MultiSelectPopover
        label={t("monitoredTables.wizard.tablesLabel")}
        placeholder={t("monitoredTables.wizard.tablesPlaceholder", { count: state.selectedSchemas.length })}
        searchPlaceholder={t("monitoredTables.wizard.searchTables")}
        options={state.tableOptions}
        selected={state.selectedTables}
        onChange={state.setSelectedTables}
        isLoading={state.tablesLoading}
        disabled={state.selectedCatalogs.length === 0}
        disabledHint={t("monitoredTables.wizard.selectCatalogFirst")}
        emptyText={t("monitoredTables.wizard.emptyTables")}
      />
      <div className="rounded-md border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
        {state.selectedCatalogs.length === 0
          ? t("monitoredTables.wizard.scopeHintEmpty")
          : t("monitoredTables.wizard.scopeSummary", { count: state.effectiveFqns.length })}
      </div>
    </>
  );
}

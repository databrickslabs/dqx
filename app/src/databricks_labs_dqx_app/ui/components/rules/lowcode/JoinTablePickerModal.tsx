import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { MultiSelectPopover } from "@/components/monitored-tables/MultiSelectPopover";
import { useListCatalogs, useListSchemas, useListTables } from "@/lib/api";
import { buildTableFqn, parseTableFqn } from "./joinTableFqn";

/**
 * Single-table pick modal for the low-code Joins card. Browses
 * catalog → schema → table with the same MultiSelectPopover (in `single`
 * mode) used by the "Monitor a table" wizard, but scoped to picking exactly
 * one table: choosing a table builds its fully-qualified name, hands it to
 * `onSelect`, and closes the dialog.
 *
 * The catalog/schema/table cascade is local dialog state seeded from the
 * current `value` each time the dialog opens, so re-opening to "change" the
 * table starts from the previously-picked scope.
 */
export function JoinTablePickerModal({
  open,
  onOpenChange,
  value,
  onSelect,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  value: string;
  onSelect: (fqn: string) => void;
}) {
  const { t } = useTranslation();

  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");

  // Seed the cascade from the current value each time the dialog opens so
  // "change" reopens on the previously-picked scope; a closed dialog keeps
  // no stale state.
  useEffect(() => {
    if (!open) return;
    const parts = parseTableFqn(value);
    setCatalog(parts.catalog);
    setSchema(parts.schema);
    setTable(parts.table);
  }, [open, value]);

  const { data: catalogsResp, isLoading: catalogsLoading } = useListCatalogs({
    query: { enabled: open },
  });
  const { data: schemasResp, isLoading: schemasLoading } = useListSchemas(catalog, {
    query: { enabled: open && !!catalog },
  });
  const { data: tablesResp, isLoading: tablesLoading } = useListTables(catalog, schema, {
    query: { enabled: open && !!catalog && !!schema },
  });

  const catalogOptions = (catalogsResp?.data ?? []).map((c) => ({ value: c.name, label: c.name }));
  const schemaOptions = (schemasResp?.data ?? []).map((s) => ({ value: s.name, label: s.name }));
  const tableOptions = (tablesResp?.data ?? []).map((tb) => ({ value: tb.name, label: tb.name }));

  const handleCatalogChange = useCallback((values: string[]) => {
    setCatalog(values[0] ?? "");
    setSchema("");
    setTable("");
  }, []);

  const handleSchemaChange = useCallback((values: string[]) => {
    setSchema(values[0] ?? "");
    setTable("");
  }, []);

  const handleTableChange = useCallback(
    (values: string[]) => {
      const next = values[0] ?? "";
      setTable(next);
      const fqn = buildTableFqn(catalog, schema, next);
      if (fqn) {
        onSelect(fqn);
        onOpenChange(false);
      }
    },
    [catalog, schema, onSelect, onOpenChange],
  );

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>{t("rulesRegistry.lowcodeJoinPickTableTitle")}</DialogTitle>
          <DialogDescription>{t("rulesRegistry.lowcodeJoinPickTableDescription")}</DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-3 gap-2 py-2">
          <MultiSelectPopover
            label={t("monitoredTables.wizard.catalogsLabel")}
            placeholder={t("rulesRegistry.joinPickCatalogPlaceholder")}
            searchPlaceholder={t("monitoredTables.wizard.searchCatalogs")}
            options={catalogOptions}
            selected={catalog ? [catalog] : []}
            onChange={handleCatalogChange}
            isLoading={catalogsLoading}
            emptyText={t("monitoredTables.wizard.emptyCatalogs")}
            single
          />
          <MultiSelectPopover
            label={t("monitoredTables.wizard.schemasLabel")}
            placeholder={t("rulesRegistry.joinPickSchemaPlaceholder")}
            searchPlaceholder={t("monitoredTables.wizard.searchSchemas")}
            options={schemaOptions}
            selected={schema ? [schema] : []}
            onChange={handleSchemaChange}
            isLoading={schemasLoading}
            disabled={!catalog}
            disabledHint={t("monitoredTables.wizard.selectCatalogFirst")}
            emptyText={t("monitoredTables.wizard.emptySchemas")}
            single
          />
          <MultiSelectPopover
            label={t("monitoredTables.wizard.tablesLabel")}
            placeholder={t("rulesRegistry.joinPickTablePlaceholder")}
            searchPlaceholder={t("monitoredTables.wizard.searchTables")}
            options={tableOptions}
            selected={table ? [table] : []}
            onChange={handleTableChange}
            isLoading={tablesLoading}
            disabled={!schema}
            disabledHint={t("monitoredTables.wizard.selectSchemaFirst")}
            emptyText={t("monitoredTables.wizard.emptyTables")}
            single
          />
        </div>
      </DialogContent>
    </Dialog>
  );
}

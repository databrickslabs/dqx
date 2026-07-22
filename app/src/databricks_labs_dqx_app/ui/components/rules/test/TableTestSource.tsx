// TableTestSource — pick a UC table and map each rule slot to a column for the
// Rule Test tab's Table mode (P22-E). Ported in spirit from dqlake's
// `test/UcTableSource.tsx`, reusing DQX's own `CatalogBrowser` (3-level table
// picker) and `SingleColumnPicker` (family-filtered column picker) so the look
// matches the Apply Rules mapping UI. Lifts a ready payload up via `onReady`
// (null until a table is chosen and every slot is mapped).

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { SingleTableScopePicker } from "@/components/monitored-tables/SingleTableScopePicker";
import { SingleColumnPicker } from "@/components/apply-rules/ColumnPicker";
import { useGetTableColumns, type RuleSlot } from "@/lib/api";

export interface TableSourcePayload {
  table: string;
  column_mapping: Record<string, string>;
  /** Columns to bold in the result grid header (the mapped ones). */
  mappedColumns: string[];
}

function splitFqn(fqn: string): { catalog: string; schema: string; table: string } | null {
  const parts = fqn.split(".");
  if (parts.length !== 3 || parts.some((p) => !p)) return null;
  return { catalog: parts[0], schema: parts[1], table: parts[2] };
}

export function TableTestSource({
  slots,
  onReady,
}: {
  slots: RuleSlot[];
  onReady: (p: TableSourcePayload | null) => void;
}) {
  const { t } = useTranslation();
  const [fqn, setFqn] = useState("");
  const [mapping, setMapping] = useState<Record<string, string>>({});

  const parts = splitFqn(fqn);
  const detail = useGetTableColumns(parts?.catalog ?? "", parts?.schema ?? "", parts?.table ?? "", {
    query: { enabled: !!parts },
  });
  const columns = detail.data?.data ?? [];

  // Reset the mapping whenever the table changes so stale column names don't
  // survive onto a different table.
  useEffect(() => {
    setMapping({});
  }, [fqn]);

  const allMapped = slots.length === 0 || slots.every((s) => !!mapping[s.name]);
  const mappedColumns = useMemo(() => slots.map((s) => mapping[s.name]).filter(Boolean), [slots, mapping]);

  useEffect(() => {
    if (!parts || !allMapped) {
      onReady(null);
      return;
    }
    onReady({ table: fqn, column_mapping: mapping, mappedColumns });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fqn, allMapped, JSON.stringify(mapping)]);

  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <h4 className="text-sm font-medium leading-none">{t("ruleTest.pickTable")}</h4>
        <SingleTableScopePicker value={fqn} onChange={setFqn} />
      </div>
      {parts && slots.length > 0 && (
        <div className="space-y-2">
          <h4 className="text-sm font-medium leading-none">{t("ruleTest.mapColumns")}</h4>
          <div className="space-y-2">
            {slots.map((slot) => (
              <div key={slot.name} className="grid grid-cols-[160px_24px_1fr] items-center gap-3">
                <div className="flex items-center gap-2 min-w-0">
                  <span className="font-mono text-xs truncate">{`{{${slot.name}}}`}</span>
                  <span className="inline-block rounded bg-muted/60 border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground font-medium uppercase tracking-wide shrink-0">
                    {slot.family}
                  </span>
                </div>
                <span className="text-muted-foreground text-xs justify-self-center self-center">&rarr;</span>
                <SingleColumnPicker
                  slot={slot}
                  columns={columns}
                  value={mapping[slot.name]}
                  onChange={(col) => setMapping((prev) => ({ ...prev, [slot.name]: col }))}
                  excludeColumns={Object.entries(mapping)
                    .filter(([name]) => name !== slot.name)
                    .map(([, v]) => v)
                    .filter(Boolean)}
                />
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

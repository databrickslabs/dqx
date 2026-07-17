import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, X } from "lucide-react";
import { SingleTableScopePicker } from "@/components/monitored-tables/SingleTableScopePicker";
import { useGetTableColumns } from "@/lib/api";
import type { JoinAst, JoinKeyAst, JoinType } from "@/lib/lowcodeAst";
import type { LowcodeColumnRef } from "@/lib/lowcodeCompile";

const JOIN_TYPES: JoinType[] = ["INNER", "LEFT", "RIGHT", "FULL", "LEFT SEMI", "LEFT ANTI", "CROSS"];
const JOIN_LABEL: Record<JoinType, string> = {
  INNER: "INNER JOIN",
  LEFT: "LEFT JOIN",
  RIGHT: "RIGHT JOIN",
  FULL: "FULL OUTER JOIN",
  "LEFT SEMI": "LEFT SEMI JOIN",
  "LEFT ANTI": "LEFT ANTI JOIN",
  CROSS: "CROSS JOIN",
};

type Props = {
  join: JoinAst;
  declaredColumns: LowcodeColumnRef[];
  onChange: (next: JoinAst) => void;
  onDelete: () => void;
};

// Ported from dqlake's JoinBlock. The joined-table column list comes from
// DQX's `useGetTableColumns` (dqlake used `useList_uc_table_columns`); the
// target table is picked via the app's `CatalogBrowser` (dqlake used a
// TablePickerModal). Structure/interactions otherwise 1:1.
export function JoinBlock({ join, declaredColumns, onChange, onDelete }: Props) {
  const { t } = useTranslation();
  const parts = (join.target_table || "").split(".");
  const [catalog, schema, table] = parts.length === 3 ? parts : ["", "", ""];
  const { data } = useGetTableColumns(catalog, schema, table, {
    query: { enabled: Boolean(catalog && schema && table) },
  });
  const joinedCols = useMemo(() => data?.data ?? [], [data]);

  const updateKey = (i: number, key: JoinKeyAst) => {
    const next = join.keys.slice();
    next[i] = key;
    onChange({ ...join, keys: next });
  };
  const deleteKey = (i: number) => {
    if (join.keys.length === 1) return;
    const next = join.keys.slice();
    next.splice(i, 1);
    onChange({ ...join, keys: next });
  };
  const addKey = () => {
    onChange({ ...join, keys: [...join.keys, { joined_column: "", column_ref: "" }] });
  };

  const isCross = join.join_type === "CROSS";

  return (
    <div className="rounded-md border p-3 space-y-2 bg-muted/20">
      <div className="grid grid-cols-[120px_minmax(0,1.5fr)_minmax(0,1fr)_24px_minmax(0,1fr)_28px] gap-2 items-center">
        <Select value={join.join_type} onValueChange={(v) => onChange({ ...join, join_type: v as JoinType })}>
          <SelectTrigger className="h-8 w-full font-mono text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {JOIN_TYPES.map((ty) => (
              <SelectItem key={ty} value={ty}>
                {JOIN_LABEL[ty]}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <SingleTableScopePicker
          value={join.target_table || ""}
          onChange={(fqn) => onChange({ ...join, target_table: fqn })}
        />
        {!isCross ? (
          <>
            <Select
              value={join.keys[0]?.joined_column ?? ""}
              onValueChange={(v) => updateKey(0, { joined_column: v, column_ref: join.keys[0]?.column_ref ?? "" })}
              disabled={!join.target_table}
            >
              <SelectTrigger className="h-8 w-full font-mono text-xs">
                <SelectValue placeholder={t("rulesRegistry.lowcodeJoinedColumnPlaceholder")} />
              </SelectTrigger>
              <SelectContent>
                {joinedCols.map((c) => (
                  <SelectItem key={c.name} value={c.name} className="font-mono text-xs">
                    {c.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <div className="text-center text-xs text-muted-foreground">=</div>
            <Select
              value={join.keys[0]?.column_ref || ""}
              onValueChange={(v) => updateKey(0, { joined_column: join.keys[0]?.joined_column ?? "", column_ref: v })}
            >
              <SelectTrigger className="h-8 w-full font-mono text-xs">
                <SelectValue placeholder={t("rulesRegistry.lowcodeColumnRefPlaceholder")} />
              </SelectTrigger>
              <SelectContent>
                {declaredColumns.map((c) => (
                  <SelectItem key={c.name} value={c.name} className="font-mono text-xs">{`{{${c.name}}}`}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </>
        ) : (
          <div className="col-span-3 text-xs text-muted-foreground italic">{t("rulesRegistry.lowcodeNoCrossKeys")}</div>
        )}
        <Button type="button" variant="ghost" size="sm" className="h-8 w-8 p-0" onClick={onDelete}>
          <X className="h-4 w-4" />
        </Button>
      </div>

      {!isCross &&
        join.keys.slice(1).map((key, idx) => {
          const i = idx + 1;
          return (
            <div
              key={i}
              className="grid grid-cols-[120px_minmax(0,1.5fr)_minmax(0,1fr)_24px_minmax(0,1fr)_28px] gap-2 items-center"
            >
              <span className="text-[10px] font-semibold uppercase tracking-wider text-primary text-right pr-2">
                AND
              </span>
              <div />
              <Select
                value={key.joined_column || ""}
                onValueChange={(v) => updateKey(i, { ...key, joined_column: v })}
                disabled={!join.target_table}
              >
                <SelectTrigger className="h-8 w-full font-mono text-xs">
                  <SelectValue placeholder={t("rulesRegistry.lowcodeJoinedColumnPlaceholder")} />
                </SelectTrigger>
                <SelectContent>
                  {joinedCols.map((c) => (
                    <SelectItem key={c.name} value={c.name} className="font-mono text-xs">
                      {c.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <div className="text-center text-xs text-muted-foreground">=</div>
              <Select value={key.column_ref || ""} onValueChange={(v) => updateKey(i, { ...key, column_ref: v })}>
                <SelectTrigger className="h-8 w-full font-mono text-xs">
                  <SelectValue placeholder={t("rulesRegistry.lowcodeColumnRefPlaceholder")} />
                </SelectTrigger>
                <SelectContent>
                  {declaredColumns.map((c) => (
                    <SelectItem key={c.name} value={c.name} className="font-mono text-xs">{`{{${c.name}}}`}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button type="button" variant="ghost" size="sm" className="h-8 w-8 p-0" onClick={() => deleteKey(i)}>
                <X className="h-4 w-4" />
              </Button>
            </div>
          );
        })}

      {!isCross && (
        <div className="pl-[128px]">
          <Button type="button" variant="ghost" size="sm" className="h-7 text-xs" onClick={addKey}>
            <Plus className="mr-1 h-3 w-3" /> {t("rulesRegistry.lowcodeAddKey")}
          </Button>
        </div>
      )}
    </div>
  );
}

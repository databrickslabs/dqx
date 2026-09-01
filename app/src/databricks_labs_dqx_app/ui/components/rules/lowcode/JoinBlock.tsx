import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ChevronDown, Plus, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { JoinTablePickerModal } from "@/components/rules/lowcode/JoinTablePickerModal";
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
  // A freshly-added join has no target table yet — open the table picker
  // immediately so "Add join" lands the user straight in the modal. Existing
  // joins (target_table set) start closed. (Initial state only; JoinsBuilder
  // keys each block, so a new block mounts with this fresh.)
  const [pickerOpen, setPickerOpen] = useState(() => !join.target_table);
  const parts = (join.target_table || "").split(".");
  const [catalog, schema, table] = parts.length === 3 ? parts : ["", "", ""];
  const { data } = useGetTableColumns(catalog, schema, table, {
    query: { enabled: Boolean(catalog && schema && table) },
  });
  const joinedCols = useMemo(() => data?.data ?? [], [data]);
  // The input side of a join key must be a declared `{{slot}}` (an INPUT column),
  // never a column sourced from a joined-to table — that would be invalid SQL.
  // Joined-table columns carry dotted names (e.g. "catalog.schema.table.col"),
  // declared slots are bare (e.g. "email"), so exclude any dotted name here.
  const columnRefOptions = useMemo(
    () => declaredColumns.filter((c) => !c.name.includes(".")),
    [declaredColumns],
  );

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
        {/* Selected table shows in a dropdown-style box (matching the JOIN-type
            Select next to it); clicking anywhere on it re-opens the pre-seeded
            table picker modal. Empty state shows the placeholder in the same box. */}
        <button
          type="button"
          data-slot="select-trigger"
          data-size="sm"
          onClick={() => setPickerOpen(true)}
          title={join.target_table || undefined}
          className="border-input dark:bg-input/30 dark:hover:bg-input/50 flex h-8 w-full items-center justify-between gap-2 rounded-md border bg-transparent px-3 py-1 font-mono text-xs whitespace-nowrap shadow-xs outline-none"
        >
          <span className={cn("truncate", join.target_table ? "text-foreground" : "text-muted-foreground")}>
            {join.target_table || t("rulesRegistry.lowcodeJoinSelectTable")}
          </span>
          <ChevronDown className="size-4 opacity-50 shrink-0" />
        </button>
        <JoinTablePickerModal
          open={pickerOpen}
          onOpenChange={setPickerOpen}
          value={join.target_table || ""}
          onSelect={(fqn) => onChange({ ...join, target_table: fqn })}
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
                {columnRefOptions.map((c) => (
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
                  {columnRefOptions.map((c) => (
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

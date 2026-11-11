// THROWAWAY prototype — condition row builder modelled on rules/lowcode/LowcodeRow + FilterBuilder.
import { Plus, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { cn } from "@/lib/utils";
import { MOCK_PRINCIPALS } from "../mock";
import {
  COLUMN_PREDICATES,
  OBJECT_LABEL,
  PREDICATE_LABEL,
  TABLE_OBJECTS,
  TABLE_PREDICATES,
  defaultRow,
  slotNameHint,
  valueKind,
  type ConditionExpr,
  type ConditionRow,
  type Predicate,
  type TableObject,
} from "./model";
import { TagSingleSelect } from "./tag-picker";

function CombinatorPill({ value, onToggle }: { value: "AND" | "OR"; onToggle: () => void }) {
  return (
    <div className="relative inline-grid grid-cols-2 rounded-full bg-muted p-0.5">
      <span
        aria-hidden
        className="absolute top-0.5 bottom-0.5 left-0.5 w-[calc(50%-2px)] rounded-full bg-primary transition-transform duration-200 ease-out"
        style={{ transform: `translateX(${value === "OR" ? "100%" : "0%"})` }}
      />
      {(["AND", "OR"] as const).map((c) => (
        <button
          key={c}
          type="button"
          onClick={onToggle}
          className={cn(
            "relative z-10 rounded-full px-2 py-0.5 text-[10px] font-semibold transition-colors duration-200",
            value === c ? "text-primary-foreground" : "text-muted-foreground hover:text-foreground",
          )}
        >
          {c}
        </button>
      ))}
    </div>
  );
}

function ValueCell({
  row,
  onChange,
  slotName,
}: {
  row: ConditionRow;
  onChange: (v: string) => void;
  slotName?: string;
}) {
  const kind = valueKind(row.predicate);
  if (kind === "tag") {
    return <TagSingleSelect value={row.value} onChange={onChange} mode="any" placeholder="Select tag" />;
  }
  if (kind === "owner") {
    return (
      <Select value={row.value || undefined} onValueChange={onChange}>
        <SelectTrigger className="h-8 w-full text-xs">
          <SelectValue placeholder="Select principal" />
        </SelectTrigger>
        <SelectContent>
          {MOCK_PRINCIPALS.map((o) => (
            <SelectItem key={o} value={o} className="font-mono text-xs">
              {o}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    );
  }
  const like = row.predicate === "name_like";
  const placeholder = slotName ? slotNameHint(slotName, like) : like ? "%_tmp" : "sales";
  return (
    <Input
      value={row.value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      className="h-8 font-mono text-xs"
    />
  );
}

/**
 * Rows joined by AND/OR. Table level: [object] [predicate] [value]. Column level (*slotName* set):
 * the object is the column itself, so rows are [predicate] [value].
 */
export function ConditionBuilder({
  expr,
  onChange,
  level,
  slotName,
}: {
  expr: ConditionExpr;
  onChange: (next: ConditionExpr) => void;
  level: "table" | "column";
  /** Column conditions: slot name used for name placeholders. */
  slotName?: string;
}) {
  const isTable = level === "table";
  const predicates = isTable ? TABLE_PREDICATES : COLUMN_PREDICATES;

  const update = (i: number, row: ConditionRow) => {
    const rows = expr.rows.slice();
    rows[i] = row;
    onChange({ rows });
  };
  const remove = (i: number) => {
    const rows = expr.rows.filter((_, j) => j !== i);
    if (rows.length > 0) rows[0] = { ...rows[0], combinator: null };
    onChange({ rows });
  };
  const add = () =>
    onChange({ rows: [...expr.rows, defaultRow(isTable ? "table" : "column", expr.rows.length === 0 ? null : "AND")] });

  const setPredicate = (row: ConditionRow, predicate: Predicate): ConditionRow => ({
    ...row,
    predicate,
    value: valueKind(predicate) === valueKind(row.predicate) ? row.value : "",
  });

  return (
    <div className="rounded-lg border bg-card p-3">
      <div className="mb-2 text-[11px] font-semibold tracking-[0.08em] text-muted-foreground uppercase">Conditions</div>
      <div className="space-y-1.5">
        {expr.rows.map((row, i) => (
          <div
            key={i}
            className={cn(
              "grid items-center gap-2",
              isTable
                ? "grid-cols-[64px_7rem_10rem_minmax(0,1fr)_32px] max-lg:grid-cols-[56px_1fr_1fr_1fr_32px]"
                : "grid-cols-[64px_10rem_minmax(0,1fr)_32px] max-lg:grid-cols-[56px_1fr_1fr_32px]",
            )}
          >
            {i === 0 ? (
              <span className="text-[11px] font-semibold tracking-[0.08em] text-muted-foreground uppercase">If</span>
            ) : (
              <div className="justify-self-end pr-1">
                <CombinatorPill
                  value={row.combinator ?? "AND"}
                  onToggle={() => update(i, { ...row, combinator: (row.combinator ?? "AND") === "AND" ? "OR" : "AND" })}
                />
              </div>
            )}
            {isTable && (
              <Select value={row.object} onValueChange={(o) => update(i, { ...row, object: o as TableObject })}>
                <SelectTrigger className="h-8 w-full text-xs" aria-label="Object">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {TABLE_OBJECTS.map((o) => (
                    <SelectItem key={o} value={o} className="text-xs">
                      {OBJECT_LABEL[o]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
            <Select value={row.predicate} onValueChange={(p) => update(i, setPredicate(row, p as Predicate))}>
              <SelectTrigger className="h-8 w-full text-xs" aria-label="Condition">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {predicates.map((p) => (
                  <SelectItem key={p} value={p} className="text-xs">
                    {PREDICATE_LABEL[p]}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <ValueCell row={row} slotName={slotName} onChange={(v) => update(i, { ...row, value: v })} />
            {expr.rows.length > 1 ? (
              <Button type="button" variant="ghost" size="icon-sm" aria-label="Remove condition" onClick={() => remove(i)}>
                <X className="h-4 w-4" />
              </Button>
            ) : (
              <span aria-hidden />
            )}
          </div>
        ))}
        <Button type="button" variant="outline" size="sm" className="mt-1 h-7 text-xs" onClick={add}>
          <Plus className="mr-1 h-3.5 w-3.5" /> Add condition
        </Button>
      </div>
    </div>
  );
}

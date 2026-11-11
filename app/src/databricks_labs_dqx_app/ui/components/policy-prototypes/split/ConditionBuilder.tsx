// THROWAWAY prototype — condition row builder in the LowcodeRow pattern:
// [combinator] [subject] [operator] [value] [x], plus a {x} expression view.
import { useState } from "react";
import { Braces, Plus, Rows3, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import {
  MOCK_PRINCIPALS,
  OPERATOR_LABEL,
  SUBJECT_LABEL,
  operatorsFor,
  printExpr,
  type ConditionExpr,
  type ConditionRow,
  type ConditionSubject,
} from "../mock";
import { TagSinglePicker } from "./TagPickers";

const COLUMN_TYPES = ["string", "int", "bigint", "double", "decimal", "date", "timestamp", "boolean"];

function Combinator({ value, onToggle }: { value: "AND" | "OR"; onToggle: () => void }) {
  return (
    <div className="relative inline-grid grid-cols-2 rounded-full bg-muted p-0.5">
      <span
        aria-hidden
        className="absolute bottom-0.5 left-0.5 top-0.5 w-[calc(50%-2px)] rounded-full bg-primary transition-transform duration-200 ease-out"
        style={{ transform: `translateX(${value === "OR" ? "100%" : "0%"})` }}
      />
      {(["AND", "OR"] as const).map((c) => (
        <button
          key={c}
          type="button"
          onClick={onToggle}
          className={cn(
            "relative z-10 rounded-full px-2 py-0.5 text-[10px] font-semibold transition-colors",
            value === c ? "text-primary-foreground" : "text-muted-foreground hover:text-foreground",
          )}
        >
          {c}
        </button>
      ))}
    </div>
  );
}

function ValueField({ row, onChange }: { row: ConditionRow; onChange: (v: string) => void }) {
  if (row.subject.endsWith("_tag")) {
    const valueMode = row.operator.includes("tag_value");
    return (
      <TagSinglePicker
        value={row.value}
        onChange={onChange}
        mode={valueMode ? "values" : "keys"}
        placeholder={valueMode ? "key=value" : "Tag key"}
      />
    );
  }
  if (row.subject === "table_owner") {
    return (
      <Select value={row.value || undefined} onValueChange={onChange}>
        <SelectTrigger size="sm" className="w-full text-xs">
          <SelectValue placeholder="Principal" />
        </SelectTrigger>
        <SelectContent>
          {MOCK_PRINCIPALS.map((p) => (
            <SelectItem key={p} value={p} className="text-xs">
              {p}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    );
  }
  if (row.subject === "column_type") {
    return (
      <Select value={row.value || undefined} onValueChange={onChange}>
        <SelectTrigger size="sm" className="w-full font-mono text-xs">
          <SelectValue placeholder="Type" />
        </SelectTrigger>
        <SelectContent>
          {COLUMN_TYPES.map((t) => (
            <SelectItem key={t} value={t} className="font-mono text-xs">
              {t}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    );
  }
  const like = row.operator === "like" || row.operator === "not_like";
  return (
    <Input
      value={row.value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={like ? "e.g. %_tmp" : "Name"}
      className="h-8 font-mono text-xs"
    />
  );
}

export function ConditionBuilder({
  expr,
  onChange,
  subjects,
}: {
  expr: ConditionExpr;
  onChange: (expr: ConditionExpr) => void;
  subjects: ConditionSubject[];
}) {
  const [textView, setTextView] = useState(false);

  const update = (i: number, patch: Partial<ConditionRow>) => {
    const rows = expr.rows.slice();
    const next = { ...rows[i], ...patch };
    if (patch.subject && !operatorsFor(patch.subject).includes(next.operator)) {
      next.operator = operatorsFor(patch.subject)[0];
      next.value = "";
    }
    if (patch.operator && patch.operator.includes("tag") && rows[i].operator.includes("tag_value") !== patch.operator.includes("tag_value")) {
      next.value = "";
    }
    rows[i] = next;
    onChange({ rows });
  };
  const add = () => {
    const subject = subjects.includes("column_tag") ? "column_tag" : "table_tag";
    onChange({
      rows: [...expr.rows, { combinator: expr.rows.length === 0 ? null : "AND", subject, operator: "has_tag", value: "" }],
    });
  };
  const remove = (i: number) => {
    const rows = expr.rows.filter((_, j) => j !== i);
    if (rows[0]) rows[0] = { ...rows[0], combinator: null };
    onChange({ rows });
  };

  return (
    <div className="space-y-2 rounded-md border bg-background/50 p-2">
      <div className="flex items-center justify-between">
        <span className="text-[11px] font-medium text-muted-foreground">
          {textView ? "Expression" : `${expr.rows.length} condition${expr.rows.length === 1 ? "" : "s"}`}
        </span>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              type="button"
              variant={textView ? "secondary" : "ghost"}
              size="sm"
              className="h-7 gap-1 px-2 font-mono text-xs"
              onClick={() => setTextView((v) => !v)}
              aria-pressed={textView}
            >
              {textView ? <Rows3 className="h-3.5 w-3.5" /> : <Braces className="h-3.5 w-3.5" />}
              {textView ? "Builder" : "{x}"}
            </Button>
          </TooltipTrigger>
          <TooltipContent>{textView ? "Back to the visual builder" : "Show as expression"}</TooltipContent>
        </Tooltip>
      </div>

      {textView ? (
        <div className="space-y-1">
          <textarea
            readOnly
            value={expr.rows.length ? printExpr(expr) : ""}
            placeholder="No conditions yet"
            rows={3}
            className="w-full resize-y rounded-md border bg-muted/30 px-2.5 py-2 font-mono text-xs leading-relaxed outline-none"
          />
          <p className="text-[11px] text-muted-foreground">
            Functions: hasTag, hasTagValue, ownerIs, nameIs, nameLike, typeIs. LIKE wildcards only. (Read-only in this prototype.)
          </p>
        </div>
      ) : (
        <div className="space-y-1">
          {expr.rows.map((row, i) => (
            <div key={i} className="grid grid-cols-[52px_8.5rem_minmax(7.5rem,10rem)_minmax(0,1fr)_28px] items-center gap-1.5">
              {i === 0 ? (
                <span className="justify-self-start text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">Where</span>
              ) : (
                <div className="justify-self-start">
                  <Combinator
                    value={row.combinator ?? "AND"}
                    onToggle={() => update(i, { combinator: (row.combinator ?? "AND") === "AND" ? "OR" : "AND" })}
                  />
                </div>
              )}
              <Select value={row.subject} onValueChange={(v) => update(i, { subject: v as ConditionSubject })}>
                <SelectTrigger size="sm" className="w-full text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {subjects.map((s) => (
                    <SelectItem key={s} value={s} className="text-xs">
                      {SUBJECT_LABEL[s]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={row.operator} onValueChange={(v) => update(i, { operator: v as ConditionRow["operator"] })}>
                <SelectTrigger size="sm" className="w-full text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {operatorsFor(row.subject).map((o) => (
                    <SelectItem key={o} value={o} className="text-xs">
                      {OPERATOR_LABEL[o]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <div className="min-w-0">
                <ValueField row={row} onChange={(v) => update(i, { value: v })} />
              </div>
              <Button type="button" variant="ghost" size="sm" className="h-8 w-7 p-0" onClick={() => remove(i)} aria-label="Remove condition">
                <X className="h-4 w-4" />
              </Button>
            </div>
          ))}
          <Button type="button" variant="outline" size="sm" className="mt-1 h-7 text-xs" onClick={add}>
            <Plus className="mr-1 h-3.5 w-3.5" /> Add condition
          </Button>
        </div>
      )}
    </div>
  );
}

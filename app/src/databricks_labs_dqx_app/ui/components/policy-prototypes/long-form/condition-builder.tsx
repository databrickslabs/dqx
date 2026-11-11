// THROWAWAY prototype — condition row builder modelled on rules/lowcode/LowcodeRow + FilterBuilder.
import { useState } from "react";
import { Braces, ListTree, Plus, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { MOCK_PRINCIPALS } from "../mock";
import {
  SUBJECT_LABEL,
  defaultRow,
  isTagSubject,
  operatorLabel,
  operatorsFor,
  printExpr,
  slotNameHint,
  type ConditionExpr,
  type ConditionRow,
  type Subject,
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
  if (isTagSubject(row.subject)) {
    return <TagSingleSelect value={row.value} onChange={onChange} mode="any" placeholder="Select tag" />;
  }
  if (row.subject === "table_owner") {
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
  const like = row.operator === "like" || row.operator === "not_like";
  const base = row.subject === "column_name" && slotName ? slotName : "orders";
  return (
    <Input
      value={row.value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={slotNameHint(base, like)}
      className="h-8 font-mono text-xs"
    />
  );
}

/** Rows of [subject] [operator] [value] joined by AND/OR, with a {x} toggle to an expression view. */
export function ConditionBuilder({
  expr,
  onChange,
  subjects,
  slotName,
  invalid,
}: {
  expr: ConditionExpr;
  onChange: (next: ConditionExpr) => void;
  subjects: Subject[];
  /** Column conditions: slot name used for name placeholders. */
  slotName?: string;
  invalid?: boolean;
}) {
  const [exprView, setExprView] = useState(false);
  const [text, setText] = useState("");

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
  const add = () => onChange({ rows: [...expr.rows, defaultRow(subjects[0], expr.rows.length === 0 ? null : "AND")] });

  const toggleView = () => {
    if (!exprView) setText(printExpr(expr));
    setExprView((v) => !v);
  };

  return (
    <div className={cn("rounded-lg border bg-card p-3", invalid && "border-destructive")}>
      <div className="mb-2 flex items-center justify-between">
        <span className="text-[11px] font-semibold tracking-[0.08em] text-muted-foreground uppercase">
          {exprView ? "Expression" : "Conditions"}
        </span>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              type="button"
              variant={exprView ? "secondary" : "ghost"}
              size="sm"
              onClick={toggleView}
              className="h-7 gap-1.5 px-2 font-mono text-xs"
              aria-pressed={exprView}
            >
              {exprView ? <ListTree className="h-3.5 w-3.5" /> : <Braces className="h-3.5 w-3.5" />}
              {exprView ? "Builder" : "{x}"}
            </Button>
          </TooltipTrigger>
          <TooltipContent>{exprView ? "Edit with the builder" : "Edit as expression"}</TooltipContent>
        </Tooltip>
      </div>

      {exprView ? (
        <div className="space-y-2">
          <Textarea
            value={text}
            onChange={(e) => setText(e.target.value)}
            rows={3}
            spellCheck={false}
            className="font-mono text-xs leading-relaxed"
          />
          <p className="text-[11px] leading-relaxed text-muted-foreground">
            Functions: <code className="font-mono">hasTag(level, 'key')</code>,{" "}
            <code className="font-mono">hasTagValue(level, 'key', 'value')</code>,{" "}
            <code className="font-mono">ownerIs('principal')</code>,{" "}
            <code className="font-mono">nameIs(level, 'name')</code>,{" "}
            <code className="font-mono">nameLike(level, '%pattern')</code>. Join with AND / OR; prefix NOT to negate.
          </p>
        </div>
      ) : (
        <div className="space-y-1.5">
          {expr.rows.map((row, i) => (
            <div
              key={i}
              className="grid grid-cols-[64px_9rem_10rem_minmax(0,1fr)_32px] items-center gap-2 max-lg:grid-cols-[56px_1fr_1fr_1fr_32px]"
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
              <Select
                value={row.subject}
                onValueChange={(s) => update(i, defaultRow(s as Subject, row.combinator))}
              >
                <SelectTrigger className="h-8 w-full text-xs">
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
              <Select
                value={row.operator}
                onValueChange={(op) => update(i, { ...row, operator: op as ConditionRow["operator"], value: "" })}
              >
                <SelectTrigger className="h-8 w-full text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {operatorsFor(row.subject).map((op) => (
                    <SelectItem key={op} value={op} className="text-xs">
                      {operatorLabel(row.subject, op)}
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
      )}
    </div>
  );
}

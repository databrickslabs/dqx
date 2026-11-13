// Condition row builder (IF / AND-OR rows) and governed-tag inputs for policy conditions.
// Table rows: [Catalog|Schema|Table] [predicate] [value]. Column rows: [predicate] [value].
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ChevronsUpDown, Plus, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { TagPicker } from "@/components/apply-rules/TagPicker";
import { cn } from "@/lib/utils";
import {
  COLUMN_PREDICATES,
  TABLE_OBJECTS,
  TABLE_PREDICATES,
  defaultRow,
  slotPlaceholder,
  valueKind,
  type DraftRow,
  type Predicate,
  type TableObject,
} from "./policy-form-state";

function TagChip({ tag, onRemove, disabled }: { tag: string; onRemove: () => void; disabled?: boolean }) {
  const { t } = useTranslation();
  return (
    <span className="inline-flex items-center gap-1 rounded-md border bg-muted/50 py-0.5 pr-1 pl-2 font-mono text-xs">
      {tag}
      {!disabled && (
        <button
          type="button"
          onClick={onRemove}
          aria-label={t("rulePolicies.tags.remove", { tag })}
          className="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
        >
          <X className="h-3 w-3" />
        </button>
      )}
    </span>
  );
}

/** Chips + "Add tag" popover over the governed tag list. Bare keys match any value of that key. */
export function TagMultiSelect({
  value,
  onChange,
  invalid,
  disabled,
}: {
  value: string[];
  onChange: (next: string[]) => void;
  invalid?: boolean;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const remove = (tag: string) => onChange(value.filter((v) => v !== tag));
  return (
    <div
      className={cn(
        "flex min-h-10 flex-wrap items-center gap-1.5 rounded-md border bg-background px-2 py-1.5",
        invalid && "border-destructive",
      )}
    >
      {value.map((tag) => (
        <TagChip key={tag} tag={tag} onRemove={() => remove(tag)} disabled={disabled} />
      ))}
      {!disabled && (
        <Popover open={open} onOpenChange={setOpen}>
          <PopoverTrigger asChild>
            <Button type="button" variant="ghost" size="sm" className="h-7 gap-1 px-2 text-xs text-muted-foreground">
              <Plus className="h-3.5 w-3.5" />
              {value.length === 0 ? t("rulePolicies.tags.select") : t("rulePolicies.tags.add")}
            </Button>
          </PopoverTrigger>
          <PopoverContent align="start" className="w-80 p-0">
            {open && (
              <TagPicker
                selected={value}
                onSelect={(tag) => {
                  if (!value.includes(tag)) onChange([...value, tag]);
                  setOpen(false);
                }}
              />
            )}
          </PopoverContent>
        </Popover>
      )}
    </div>
  );
}

/** Single governed tag, styled like a select trigger, for condition rows. */
function TagSingleSelect({
  value,
  onChange,
  invalid,
  disabled,
}: {
  value: string;
  onChange: (next: string) => void;
  invalid?: boolean;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          type="button"
          disabled={disabled}
          aria-invalid={invalid || undefined}
          className={cn(
            "flex h-8 w-full min-w-0 items-center justify-between gap-2 rounded-md border bg-background px-3 text-left text-xs shadow-xs",
            "enabled:hover:bg-muted/30 disabled:cursor-not-allowed disabled:opacity-50",
            invalid && "border-destructive",
          )}
        >
          <span className={cn("truncate font-mono", !value && "font-sans text-muted-foreground")}>
            {value || t("rulePolicies.conditions.selectTag")}
          </span>
          <ChevronsUpDown className="h-3.5 w-3.5 shrink-0 opacity-50" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-80 p-0">
        {open && (
          <TagPicker
            selected={value ? [value] : []}
            onSelect={(tag) => {
              onChange(tag);
              setOpen(false);
            }}
          />
        )}
      </PopoverContent>
    </Popover>
  );
}

function CombinatorPill({
  value,
  onToggle,
  disabled,
}: {
  value: "AND" | "OR";
  onToggle: () => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
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
          disabled={disabled}
          aria-pressed={value === c}
          onClick={() => {
            if (value !== c) onToggle();
          }}
          className={cn(
            "relative z-10 rounded-full px-2 py-0.5 text-[10px] font-semibold transition-colors duration-200",
            value === c ? "text-primary-foreground" : "text-muted-foreground enabled:hover:text-foreground",
          )}
        >
          {c === "AND" ? t("rulePolicies.conditions.and") : t("rulePolicies.conditions.or")}
        </button>
      ))}
    </div>
  );
}

function ValueCell({
  row,
  onChange,
  slotName,
  invalid,
  disabled,
}: {
  row: DraftRow;
  onChange: (v: string) => void;
  slotName?: string;
  invalid: boolean;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const kind = valueKind(row.predicate);
  if (kind === "tag") {
    return <TagSingleSelect value={row.value} onChange={onChange} invalid={invalid} disabled={disabled} />;
  }
  const placeholder =
    kind === "owner"
      ? t("rulePolicies.conditions.ownerPlaceholder")
      : slotName
        ? slotPlaceholder(slotName, row.predicate)
        : row.predicate === "name_like"
          ? t("rulePolicies.conditions.likePlaceholder")
          : t("rulePolicies.conditions.namePlaceholder");
  return (
    <Input
      value={row.value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      aria-label={t("rulePolicies.conditions.value")}
      aria-invalid={invalid || undefined}
      disabled={disabled}
      maxLength={512}
      className={cn("h-8 text-xs", kind === "name" && "font-mono")}
    />
  );
}

/** Rows joined by AND/OR. Column level (*slotName* set) rows are about the column itself. */
export function ConditionRows({
  rows,
  onChange,
  level,
  slotName,
  showErrors,
  disabled,
}: {
  rows: DraftRow[];
  onChange: (next: DraftRow[]) => void;
  level: "table" | "column";
  slotName?: string;
  /** Highlight rows with an empty value. */
  showErrors?: boolean;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const isTable = level === "table";
  const predicates = isTable ? TABLE_PREDICATES : COLUMN_PREDICATES;

  const update = (i: number, row: DraftRow) => onChange(rows.map((r, j) => (j === i ? row : r)));
  const remove = (i: number) => {
    const next = rows.filter((_, j) => j !== i);
    if (next.length > 0) next[0] = { ...next[0], combinator: null };
    onChange(next);
  };
  const add = () => onChange([...rows, defaultRow(isTable ? "table" : "column", rows.length === 0 ? null : "AND")]);
  const setPredicate = (row: DraftRow, predicate: Predicate): DraftRow => ({
    ...row,
    predicate,
    value: valueKind(predicate) === valueKind(row.predicate) ? row.value : "",
  });

  return (
    <div className="rounded-lg border bg-card p-3">
      <div className="mb-2 text-[11px] font-semibold tracking-[0.08em] text-muted-foreground uppercase">
        {t("rulePolicies.conditions.heading")}
      </div>
      <div className="space-y-1.5">
        {rows.map((row, i) => (
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
              <span className="text-[11px] font-semibold tracking-[0.08em] text-muted-foreground uppercase">
                {t("rulePolicies.conditions.if")}
              </span>
            ) : (
              <div className="justify-self-end pr-1">
                <CombinatorPill
                  value={row.combinator ?? "AND"}
                  disabled={disabled}
                  onToggle={() => update(i, { ...row, combinator: (row.combinator ?? "AND") === "AND" ? "OR" : "AND" })}
                />
              </div>
            )}
            {isTable && (
              <Select
                value={row.object}
                disabled={disabled}
                onValueChange={(o) => update(i, { ...row, object: o as TableObject })}
              >
                <SelectTrigger className="h-8 w-full text-xs" aria-label={t("rulePolicies.conditions.object")}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {TABLE_OBJECTS.map((o) => (
                    <SelectItem key={o} value={o} className="text-xs">
                      {t(`rulePolicies.conditions.objects.${o}`)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
            <Select
              value={row.predicate}
              disabled={disabled}
              onValueChange={(p) => update(i, setPredicate(row, p as Predicate))}
            >
              <SelectTrigger className="h-8 w-full text-xs" aria-label={t("rulePolicies.conditions.predicate")}>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {predicates.map((p) => (
                  <SelectItem key={p} value={p} className="text-xs">
                    {t(`rulePolicies.conditions.predicates.${p}`)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <ValueCell
              row={row}
              slotName={slotName}
              invalid={!!showErrors && !row.value.trim()}
              disabled={disabled}
              onChange={(v) => update(i, { ...row, value: v })}
            />
            {rows.length > 1 && !disabled ? (
              <Button
                type="button"
                variant="ghost"
                size="icon-sm"
                aria-label={t("rulePolicies.conditions.remove")}
                onClick={() => remove(i)}
              >
                <X className="h-4 w-4" />
              </Button>
            ) : (
              <span aria-hidden />
            )}
          </div>
        ))}
        {!disabled && rows.length < 50 && (
          <Button type="button" variant="outline" size="sm" className="mt-1 h-7 text-xs" onClick={add}>
            <Plus className="mr-1 h-3.5 w-3.5" /> {t("rulePolicies.conditions.add")}
          </Button>
        )}
      </div>
    </div>
  );
}

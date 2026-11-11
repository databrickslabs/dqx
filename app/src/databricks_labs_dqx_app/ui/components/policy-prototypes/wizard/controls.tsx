// THROWAWAY prototype — shared controls for the wizard-sheet variant (B).
// Mock lookalikes of TagPicker / RulesPicker / LowcodeRow; no API calls.
import { useMemo, useState, type ReactNode } from "react";
import { ArrowLeft, ArrowRight, Braces, Check, Info, Plus, Rows3, Search, Tag, X } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from "@/components/ui/command";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import {
  MOCK_GOVERNED_TAGS,
  MOCK_PRINCIPALS,
  MOCK_RULES,
  OPERATOR_LABEL,
  SUBJECT_LABEL,
  operatorsFor,
  printExpr,
  type ConditionExpr,
  type ConditionOperator,
  type ConditionRow,
  type ConditionSubject,
  type MockRule,
} from "../mock";

// ── Layout primitives ───────────────────────────────────────────────────

export function Section({
  title,
  description,
  required,
  children,
  aside,
}: {
  title: string;
  description?: ReactNode;
  required?: boolean;
  children: ReactNode;
  aside?: ReactNode;
}) {
  return (
    <section className="space-y-3">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-0.5">
          <h3 className="text-sm font-semibold">
            {title}
            {required && <span className="ml-0.5 text-destructive">*</span>}
          </h3>
          {description && <p className="text-xs text-muted-foreground">{description}</p>}
        </div>
        {aside}
      </div>
      {children}
    </section>
  );
}

export function FieldError({ message }: { message?: string }) {
  if (!message) return null;
  return <p className="text-xs font-medium text-destructive">{message}</p>;
}

export function InfoLine({ children }: { children: ReactNode }) {
  return (
    <p className="flex items-start gap-1.5 text-xs text-muted-foreground">
      <Info className="mt-px h-3.5 w-3.5 shrink-0" aria-hidden />
      <span>{children}</span>
    </p>
  );
}

export interface TileOption<T extends string> {
  value: T;
  label: string;
  icon: ReactNode;
  info: ReactNode;
}

/** ABAC-style choice tiles: icon above label, selected = primary tint, info line below. */
export function ChoiceTiles<T extends string>({
  options,
  value,
  onChange,
  size = "md",
}: {
  options: TileOption<T>[];
  value: T;
  onChange: (v: T) => void;
  size?: "md" | "sm";
}) {
  const active = options.find((o) => o.value === value);
  return (
    <div className="space-y-2">
      <div
        role="radiogroup"
        className={cn("grid gap-2", options.length === 3 ? "grid-cols-3" : "grid-cols-2")}
      >
        {options.map((o) => {
          const on = o.value === value;
          return (
            <button
              key={o.value}
              type="button"
              role="radio"
              aria-checked={on}
              onClick={() => onChange(o.value)}
              className={cn(
                "flex flex-col items-center justify-center gap-1.5 rounded-lg border text-center font-medium transition-colors",
                size === "md" ? "min-h-20 px-3 py-3 text-sm" : "min-h-16 px-2 py-2 text-xs",
                "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                on
                  ? "border-primary bg-primary/10 text-foreground"
                  : "border-border bg-card text-muted-foreground hover:border-foreground/30 hover:text-foreground",
              )}
            >
              <span className={cn("[&>svg]:h-5 [&>svg]:w-5", on ? "text-primary" : "text-muted-foreground")}>
                {o.icon}
              </span>
              {o.label}
            </button>
          );
        })}
      </div>
      {active && <InfoLine>{active.info}</InfoLine>}
    </div>
  );
}

export function RuleStatusBadge({ status }: { status: MockRule["status"] }) {
  if (status === "approved")
    return (
      <Badge variant="outline" className="border-emerald-500/40 text-emerald-700 dark:text-emerald-300">
        Approved
      </Badge>
    );
  return (
    <Badge variant="outline" className="border-amber-500/40 text-amber-700 dark:text-amber-300">
      {status === "draft" ? "Draft" : "Pending approval"}
    </Badge>
  );
}

export function slotList(rule: MockRule): string {
  return rule.slots.length === 0 ? "Whole table" : rule.slots.map((s) => `{{${s.name}}}`).join(", ");
}

// ── Governed-tag picker lookalike ───────────────────────────────────────

/** Body of the two-level tag drilldown (key → values), fed by MOCK_GOVERNED_TAGS. */
function TagPickerBody({
  selected,
  onSelect,
  mode,
}: {
  selected: string[];
  onSelect: (tag: string) => void;
  /** "any": key or key=value; "key": bare keys only; "value": key=value only. */
  mode: "any" | "key" | "value";
}) {
  const [view, setView] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const q = search.trim().toLowerCase();
  const groups = MOCK_GOVERNED_TAGS.filter((g) => (mode === "value" ? g.values.length > 0 : true));
  const active = groups.find((g) => g.key === view);

  return (
    <Command shouldFilter={false}>
      <CommandInput
        value={search}
        onValueChange={setSearch}
        placeholder={view ? `Search ${view} values…` : "Search governed tags…"}
        className="h-9 text-xs"
      />
      <CommandList className="max-h-64">
        {view === null ? (
          <>
            <CommandEmpty>
              <span className="text-xs text-muted-foreground">No tags found</span>
            </CommandEmpty>
            <CommandGroup>
              {groups
                .filter((g) => !q || g.key.includes(q) || g.values.some((v) => v.includes(q)))
                .map((g) => {
                  const drill = mode !== "key" && g.values.length > 0;
                  const bareSelectable = mode !== "value";
                  return (
                    <CommandItem
                      key={g.key}
                      value={g.key}
                      onSelect={() => {
                        if (bareSelectable) onSelect(g.key);
                        else {
                          setSearch("");
                          setView(g.key);
                        }
                      }}
                      className="items-center gap-2 text-xs"
                    >
                      <Check
                        className={cn("h-3 w-3 shrink-0", selected.includes(g.key) ? "opacity-100" : "opacity-0")}
                      />
                      <span className="font-mono">{g.key}</span>
                      {g.description && (
                        <span className="min-w-0 flex-1 truncate text-muted-foreground">{g.description}</span>
                      )}
                      {drill && (
                        <button
                          type="button"
                          aria-label={`Show values of ${g.key}`}
                          onClick={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            setSearch("");
                            setView(g.key);
                          }}
                          className="ml-auto flex shrink-0 items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground"
                        >
                          {g.values.length} <ArrowRight className="h-3 w-3" />
                        </button>
                      )}
                    </CommandItem>
                  );
                })}
            </CommandGroup>
          </>
        ) : (
          <>
            <button
              type="button"
              onClick={() => {
                setView(null);
                setSearch("");
              }}
              className="flex w-full items-center gap-1.5 border-b px-2 py-1.5 text-[11px] font-medium text-muted-foreground hover:text-foreground"
            >
              <ArrowLeft className="h-3 w-3" />
              <span className="font-mono">{view}</span>
            </button>
            <CommandGroup>
              {(active?.values ?? [])
                .filter((v) => !q || v.includes(q))
                .map((v) => {
                  const full = `${view}=${v}`;
                  return (
                    <CommandItem key={full} value={full} onSelect={() => onSelect(full)} className="gap-2 text-xs">
                      <Check className={cn("h-3 w-3", selected.includes(full) ? "opacity-100" : "opacity-0")} />
                      <span className="font-mono">{v}</span>
                    </CommandItem>
                  );
                })}
            </CommandGroup>
          </>
        )}
      </CommandList>
    </Command>
  );
}

export function TagChip({ tag, onRemove }: { tag: string; onRemove?: () => void }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-md border bg-muted/50 px-1.5 py-0.5 font-mono text-xs">
      <Tag className="h-3 w-3 text-muted-foreground" aria-hidden />
      {tag}
      {onRemove && (
        <button
          type="button"
          onClick={onRemove}
          aria-label={`Remove ${tag}`}
          className="text-muted-foreground hover:text-foreground"
        >
          <X className="h-3 w-3" />
        </button>
      )}
    </span>
  );
}

/** Multi-select of governed tags (keys and key=value), shown as chips. */
export function TagMultiPicker({
  value,
  onChange,
  invalid,
}: {
  value: string[];
  onChange: (tags: string[]) => void;
  invalid?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const toggle = (tag: string) =>
    onChange(value.includes(tag) ? value.filter((t) => t !== tag) : [...value, tag]);
  return (
    <div
      className={cn(
        "flex min-h-10 flex-wrap items-center gap-1.5 rounded-md border bg-background px-2 py-1.5",
        invalid && "border-destructive",
      )}
    >
      {value.map((t) => (
        <TagChip key={t} tag={t} onRemove={() => toggle(t)} />
      ))}
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button type="button" variant="ghost" size="sm" className="h-7 gap-1 px-2 text-xs">
            <Plus className="h-3.5 w-3.5" /> {value.length ? "Add tag" : "Select tags"}
          </Button>
        </PopoverTrigger>
        <PopoverContent align="start" className="w-80 p-0">
          <TagPickerBody selected={value} onSelect={toggle} mode="any" />
        </PopoverContent>
      </Popover>
      {value.length > 1 && <span className="ml-auto text-[11px] text-muted-foreground">matches any</span>}
    </div>
  );
}

/** Single tag value used as a builder value cell. */
function TagValueCell({
  value,
  onChange,
  mode,
}: {
  value: string;
  onChange: (v: string) => void;
  mode: "key" | "value";
}) {
  const [open, setOpen] = useState(false);
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className={cn("h-8 w-full justify-start font-mono text-xs", !value && "text-muted-foreground")}
        >
          <Tag className="h-3 w-3" />
          <span className="truncate">{value || (mode === "key" ? "Pick tag" : "Pick tag value")}</span>
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-80 p-0">
        <TagPickerBody
          selected={value ? [value] : []}
          mode={mode}
          onSelect={(t) => {
            onChange(t);
            setOpen(false);
          }}
        />
      </PopoverContent>
    </Popover>
  );
}

// ── Condition builder (LowcodeRow lookalike) ────────────────────────────

const COLUMN_TYPES = ["STRING", "INT", "BIGINT", "DOUBLE", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN"];

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

function ValueCell({ row, onChange }: { row: ConditionRow; onChange: (v: string) => void }) {
  if (row.subject.endsWith("_tag")) {
    const mode = row.operator === "has_tag" || row.operator === "not_has_tag" ? "key" : "value";
    return <TagValueCell value={row.value} onChange={onChange} mode={mode} />;
  }
  if (row.subject === "column_type") {
    return (
      <Select value={row.value} onValueChange={onChange}>
        <SelectTrigger className="h-8 w-full font-mono text-xs">
          <SelectValue placeholder="Pick type" />
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
  if (row.subject === "table_owner") {
    return (
      <Select value={row.value} onValueChange={onChange}>
        <SelectTrigger className="h-8 w-full text-xs">
          <SelectValue placeholder="Pick owner" />
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
  const like = row.operator === "like" || row.operator === "not_like";
  return (
    <Input
      value={row.value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={like ? "e.g. %_email" : "Exact name"}
      className="h-8 font-mono text-xs"
    />
  );
}

export function ConditionBuilder({
  expr,
  onChange,
  subjects,
  invalid,
}: {
  expr: ConditionExpr;
  onChange: (next: ConditionExpr) => void;
  subjects: ConditionSubject[];
  invalid?: boolean;
}) {
  const [exprView, setExprView] = useState(false);

  const update = (i: number, patch: Partial<ConditionRow>) => {
    const rows = expr.rows.slice();
    const next = { ...rows[i], ...patch };
    if (patch.subject && patch.subject !== rows[i].subject) {
      const ops = operatorsFor(patch.subject);
      if (!ops.includes(next.operator)) next.operator = ops[0];
      next.value = "";
    }
    if (patch.operator && patch.operator !== rows[i].operator && next.subject.endsWith("_tag")) {
      // Switching between key and key=value operators invalidates the value shape.
      const wasValue = rows[i].operator.includes("tag_value");
      const isValue = patch.operator.includes("tag_value");
      if (wasValue !== isValue) next.value = "";
    }
    rows[i] = next;
    onChange({ rows });
  };
  const add = () =>
    onChange({
      rows: [
        ...expr.rows,
        {
          combinator: expr.rows.length === 0 ? null : "AND",
          subject: subjects.includes("column_tag") ? "column_tag" : "table_tag",
          operator: "has_tag_value",
          value: "",
        },
      ],
    });
  const remove = (i: number) => {
    const rows = expr.rows.slice();
    rows.splice(i, 1);
    if (rows.length) rows[0] = { ...rows[0], combinator: null };
    onChange({ rows });
  };

  return (
    <div className={cn("rounded-md border bg-muted/20 p-3", invalid && "border-destructive")}>
      <div className="mb-2 flex items-center justify-between">
        <span className="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">
          {exprView ? "Expression" : "Conditions"}
        </span>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              type="button"
              variant={exprView ? "secondary" : "ghost"}
              size="sm"
              className="h-7 gap-1 px-2 font-mono text-xs"
              onClick={() => setExprView((v) => !v)}
              aria-pressed={exprView}
            >
              {exprView ? <Rows3 className="h-3.5 w-3.5" /> : <Braces className="h-3.5 w-3.5" />}
              {exprView ? "Rows" : "{x}"}
            </Button>
          </TooltipTrigger>
          <TooltipContent>{exprView ? "Edit as rows" : "Show as expression"}</TooltipContent>
        </Tooltip>
      </div>

      {exprView ? (
        <pre className="min-h-16 whitespace-pre-wrap break-words rounded-md border bg-background px-3 py-2 font-mono text-xs leading-relaxed">
          {expr.rows.length ? printExpr(expr) : <span className="text-muted-foreground">No conditions yet</span>}
        </pre>
      ) : (
        <div className="space-y-1.5">
          {expr.rows.map((row, i) => (
            <div key={i} className="grid grid-cols-[4rem_9rem_10.5rem_minmax(0,1fr)_2rem] items-center gap-2">
              {i === 0 ? (
                <span className="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">If</span>
              ) : (
                <div className="justify-self-end">
                  <CombinatorPill
                    value={row.combinator ?? "AND"}
                    onToggle={() => update(i, { combinator: (row.combinator ?? "AND") === "AND" ? "OR" : "AND" })}
                  />
                </div>
              )}
              <Select value={row.subject} onValueChange={(v) => update(i, { subject: v as ConditionSubject })}>
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
              <Select value={row.operator} onValueChange={(v) => update(i, { operator: v as ConditionOperator })}>
                <SelectTrigger className="h-8 w-full text-xs">
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
              <ValueCell row={row} onChange={(v) => update(i, { value: v })} />
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="h-8 w-8 p-0"
                onClick={() => remove(i)}
                aria-label="Remove condition"
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
          ))}
          <Button type="button" variant="outline" size="sm" className="mt-1 h-7 text-xs" onClick={add}>
            <Plus className="mr-1 h-3.5 w-3.5" /> Add condition
          </Button>
        </div>
      )}
      {!exprView && expr.rows.some((r) => r.operator === "like" || r.operator === "not_like") && (
        <p className="mt-2 text-[11px] text-muted-foreground">
          Use % for any characters and _ for one character.
        </p>
      )}
    </div>
  );
}

export function emptyExpr(subjects: ConditionSubject[]): ConditionExpr {
  return {
    rows: [
      {
        combinator: null,
        subject: subjects.includes("column_tag") ? "column_tag" : "table_tag",
        operator: "has_tag_value",
        value: "",
      },
    ],
  };
}

// ── Add rules dialog (AddRulesDialog + RulesPicker lookalike) ───────────

export function AddRulesDialog({
  open,
  onOpenChange,
  existing,
  onAdd,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  existing: string[];
  onAdd: (ids: string[]) => void;
}) {
  const [search, setSearch] = useState("");
  const [picked, setPicked] = useState<Set<string>>(new Set());
  const rows = useMemo(() => {
    const q = search.trim().toLowerCase();
    return MOCK_RULES.filter(
      (r) => !q || r.name.toLowerCase().includes(q) || r.dimension.includes(q) || r.rule_id.includes(q),
    );
  }, [search]);

  const close = (next: boolean) => {
    if (!next) {
      setPicked(new Set());
      setSearch("");
    }
    onOpenChange(next);
  };
  const toggle = (id: string) => {
    if (existing.includes(id)) return;
    setPicked((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  return (
    <Dialog open={open} onOpenChange={close}>
      <DialogContent className="w-[min(92vw,820px)] !max-w-[min(92vw,820px)]">
        <DialogHeader>
          <DialogTitle>Add rules</DialogTitle>
          <DialogDescription>Pick rules from the registry. Only approved rules are attached to tables.</DialogDescription>
        </DialogHeader>
        <div className="min-w-0 space-y-3">
          <div className="relative max-w-xs">
            <Search className="absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search rules"
              className="h-8 pl-7 text-xs"
            />
          </div>
          <div className="max-h-[24rem] min-h-[16rem] overflow-y-auto rounded-md border">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-background">
                <tr className="border-b text-left text-xs text-muted-foreground">
                  <th className="w-9 px-2 py-2" />
                  <th className="px-2 py-2 font-medium">Name</th>
                  <th className="px-2 py-2 font-medium">Dimension</th>
                  <th className="px-2 py-2 font-medium">Columns</th>
                  <th className="px-2 py-2 font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const inPolicy = existing.includes(r.rule_id);
                  const checked = inPolicy || picked.has(r.rule_id);
                  return (
                    <tr
                      key={r.rule_id}
                      onClick={() => toggle(r.rule_id)}
                      aria-disabled={inPolicy || undefined}
                      className={cn(
                        "border-b last:border-0",
                        inPolicy ? "cursor-not-allowed opacity-60" : "cursor-pointer hover:bg-muted/50",
                        checked && !inPolicy && "bg-primary/5",
                      )}
                    >
                      <td className="px-2 py-2" onClick={(e) => e.stopPropagation()}>
                        <Checkbox
                          checked={checked}
                          disabled={inPolicy}
                          onCheckedChange={() => toggle(r.rule_id)}
                          aria-label={`Select ${r.name}`}
                        />
                      </td>
                      <td className="px-2 py-2">
                        <div className="font-medium">{r.name}</div>
                        <div className="text-xs text-muted-foreground">
                          {inPolicy ? "Already in this policy" : r.description}
                        </div>
                      </td>
                      <td className="px-2 py-2 text-xs capitalize">{r.dimension}</td>
                      <td className="px-2 py-2 font-mono text-xs text-muted-foreground">{slotList(r)}</td>
                      <td className="px-2 py-2">
                        <div className="flex flex-col items-start gap-1">
                          <RuleStatusBadge status={r.status} />
                          {r.status !== "approved" && (
                            <span className="text-[10px] text-muted-foreground">Applies once approved</span>
                          )}
                        </div>
                      </td>
                    </tr>
                  );
                })}
                {rows.length === 0 && (
                  <tr>
                    <td colSpan={5} className="py-8 text-center text-sm text-muted-foreground">
                      No rules match your search
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => close(false)}>
            Cancel
          </Button>
          <Button
            disabled={picked.size === 0}
            onClick={() => {
              onAdd([...picked]);
              close(false);
            }}
          >
            {picked.size === 0 ? "Add rules" : `Add ${picked.size} ${picked.size === 1 ? "rule" : "rules"}`}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

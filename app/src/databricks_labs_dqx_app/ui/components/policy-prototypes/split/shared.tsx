// THROWAWAY prototype — variant C ("Split preview") shared bits: choice tiles,
// collapsible form sections, status badges and draft helpers. Mock data only.
import { useState, type ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { Check, ChevronDown, Info } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  MOCK_RULES,
  type ColumnCondition,
  type ConditionExpr,
  type MockRule,
  type MockSlot,
  type PolicyDraft,
  type PolicyScope,
  type TableCondition,
} from "../mock";

// ── Draft helpers ────────────────────────────────────────────────────────────

export function ruleById(id: string): MockRule | undefined {
  return MOCK_RULES.find((r) => r.rule_id === id);
}

export function scopeSummary(scope: PolicyScope[]): string {
  const valid = scope.filter((s) => s.catalog);
  if (valid.length === 0) return "No scope";
  return valid.map((s) => (s.schema === "*" ? `${s.catalog}.*` : `${s.catalog}.${s.schema}`)).join(", ");
}

export function exprComplete(expr: ConditionExpr): boolean {
  return expr.rows.length > 0 && expr.rows.every((r) => r.value.trim() !== "" && (!r.operator.includes("tag_value") || /=.+/.test(r.value)));
}

export function tableConditionComplete(c: TableCondition): boolean {
  if (c.kind === "none") return true;
  if (c.kind === "tags") return c.tags.length > 0;
  return exprComplete(c.expr);
}

export function columnConditionComplete(c: ColumnCondition | undefined): boolean {
  if (!c) return false;
  if (c.kind === "tags") return c.tags.length > 0;
  return exprComplete(c.expr);
}

export const DEFAULT_COLUMN_CONDITION: ColumnCondition = { kind: "tags", tags: [] };

export function slotCondition(draft: PolicyDraft, ruleId: string, slot: string): ColumnCondition {
  return draft.column_conditions[ruleId]?.[slot] ?? DEFAULT_COLUMN_CONDITION;
}

export function slotsOf(draft: PolicyDraft): { rule: MockRule; slot: MockSlot }[] {
  return draft.rule_ids.flatMap((id) => {
    const rule = ruleById(id);
    return rule ? rule.slots.map((slot) => ({ rule, slot })) : [];
  });
}

export function joinList(items: string[], conj = "and"): string {
  if (items.length <= 1) return items.join("");
  return `${items.slice(0, -1).join(", ")} ${conj} ${items[items.length - 1]}`;
}

export function cardinalityLabel(slot: MockSlot): string {
  return slot.cardinality === "one" ? "one column" : "one or more columns";
}

/** Key for "did the preview inputs change" — name/description don't affect matching. */
export function previewKey(draft: PolicyDraft): string {
  const { name: _n, description: _d, ...rest } = draft;
  return JSON.stringify(rest);
}

// ── Choice tiles (ABAC-style) ────────────────────────────────────────────────

export interface TileOption<T extends string> {
  value: T;
  label: string;
  icon: LucideIcon;
  info: string;
}

export function ChoiceTiles<T extends string>({
  options,
  value,
  onChange,
  size = "md",
}: {
  options: TileOption<T>[];
  value: T;
  onChange: (v: T) => void;
  size?: "sm" | "md";
}) {
  const selected = options.find((o) => o.value === value);
  return (
    <div className="space-y-2">
      <div role="radiogroup" className={cn("grid gap-2", options.length === 3 ? "grid-cols-3" : "grid-cols-2")}>
        {options.map((o) => {
          const on = o.value === value;
          const Icon = o.icon;
          return (
            <button
              key={o.value}
              type="button"
              role="radio"
              aria-checked={on}
              onClick={() => onChange(o.value)}
              className={cn(
                "flex flex-col items-center justify-center gap-1.5 rounded-lg border px-2 text-center transition-colors",
                size === "md" ? "min-h-[76px] py-3" : "min-h-[64px] py-2.5",
                on
                  ? "border-primary bg-primary/10 text-foreground"
                  : "bg-card text-muted-foreground hover:border-foreground/30 hover:text-foreground",
              )}
            >
              <Icon className={cn("h-5 w-5", on ? "text-primary" : "text-muted-foreground")} />
              <span className="text-xs font-medium leading-tight">{o.label}</span>
            </button>
          );
        })}
      </div>
      {selected && (
        <p className="flex items-start gap-1.5 text-xs text-muted-foreground">
          <Info className="mt-px h-3.5 w-3.5 shrink-0" />
          <span>{selected.info}</span>
        </p>
      )}
    </div>
  );
}

// ── Collapsible section ──────────────────────────────────────────────────────

export type SectionState = "complete" | "incomplete" | "error" | "optional";

export function Section({
  title,
  description,
  state,
  summary,
  defaultOpen = true,
  children,
}: {
  title: string;
  description: string;
  state: SectionState;
  /** Shown in the header while collapsed. */
  summary?: string;
  defaultOpen?: boolean;
  children: ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <section className="rounded-lg border bg-card">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        className="flex w-full items-start gap-3 px-4 py-3 text-left"
      >
        <StatusDot state={state} />
        <div className="min-w-0 flex-1">
          <div className="text-sm font-semibold">{title}</div>
          {open ? (
            <div className="text-xs text-muted-foreground">{description}</div>
          ) : (
            summary && <div className="truncate text-xs text-muted-foreground">{summary}</div>
          )}
        </div>
        <ChevronDown className={cn("mt-0.5 h-4 w-4 shrink-0 text-muted-foreground transition-transform", !open && "-rotate-90")} />
      </button>
      {open && <div className="space-y-3 px-4 pb-4 pl-11">{children}</div>}
    </section>
  );
}

function StatusDot({ state }: { state: SectionState }) {
  if (state === "complete") {
    return (
      <span className="mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded-full bg-primary text-primary-foreground">
        <Check className="h-2.5 w-2.5" strokeWidth={3} />
      </span>
    );
  }
  return (
    <span className="mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center" aria-label={state}>
      <span
        className={cn(
          "h-2 w-2 rounded-full",
          state === "error" && "bg-destructive",
          state === "incomplete" && "bg-amber-500",
          state === "optional" && "bg-muted-foreground/40",
        )}
      />
    </span>
  );
}

// ── Small badges ─────────────────────────────────────────────────────────────

const TONE = {
  emerald: "bg-emerald-500/10 text-emerald-700 ring-emerald-500/30 dark:text-emerald-300",
  sky: "bg-sky-500/10 text-sky-700 ring-sky-500/30 dark:text-sky-300",
  amber: "bg-amber-500/10 text-amber-700 ring-amber-500/30 dark:text-amber-300",
  muted: "bg-muted text-muted-foreground ring-border",
  primary: "bg-primary/10 text-primary ring-primary/30",
} as const;

export function Pill({ tone, children, className }: { tone: keyof typeof TONE; children: ReactNode; className?: string }) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 whitespace-nowrap rounded-full px-2 py-0.5 text-[10px] font-medium ring-1",
        TONE[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

export function ActionPill({ action }: { action: PolicyDraft["action"] }) {
  return action === "auto_apply" ? <Pill tone="emerald">Apply automatically</Pill> : <Pill tone="sky">Suggest only</Pill>;
}

export function FamilyBadge({ family }: { family: string }) {
  return (
    <span className="inline-block shrink-0 rounded border border-border bg-muted/60 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
      {family}
    </span>
  );
}

export function FieldLabel({ children, required, htmlFor }: { children: ReactNode; required?: boolean; htmlFor?: string }) {
  return (
    <label htmlFor={htmlFor} className="text-xs font-medium">
      {children}
      {required && <span className="ml-0.5 text-destructive">*</span>}
    </label>
  );
}

export function FieldError({ children }: { children?: ReactNode }) {
  if (!children) return null;
  return <p className="text-xs text-destructive">{children}</p>;
}

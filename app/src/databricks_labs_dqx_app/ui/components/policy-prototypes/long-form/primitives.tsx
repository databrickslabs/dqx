// THROWAWAY prototype — shared layout primitives for variant A (long form).
import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { Info } from "lucide-react";
import { cn } from "@/lib/utils";
import type { MockRule } from "../mock";

/** ABAC-style section: title + description on the left third, fields on the right two thirds. */
export function FormSection({
  id,
  title,
  description,
  required,
  children,
}: {
  id?: string;
  title: string;
  description: ReactNode;
  required?: boolean;
  children: ReactNode;
}) {
  return (
    <section id={id} className="grid scroll-mt-24 gap-x-10 gap-y-4 py-8 md:grid-cols-3">
      <div className="space-y-1.5">
        <h3 className="text-base font-semibold">
          {title}
          {required && <span className="ml-0.5 text-destructive">*</span>}
        </h3>
        <div className="text-sm leading-relaxed text-muted-foreground">{description}</div>
      </div>
      <div className="min-w-0 space-y-4 md:col-span-2">{children}</div>
    </section>
  );
}

export interface TileOption<T extends string> {
  value: T;
  label: string;
  icon: LucideIcon;
  info?: ReactNode;
}

/** Large bordered choice tiles with an icon above the label and an info line for the selected option. */
export function ChoiceTiles<T extends string>({
  value,
  onChange,
  options,
  size = "lg",
  ariaLabel,
}: {
  value: T;
  onChange: (next: T) => void;
  options: TileOption<T>[];
  size?: "lg" | "sm";
  ariaLabel: string;
}) {
  const selected = options.find((o) => o.value === value);
  return (
    <div className="space-y-2.5">
      <div
        role="radiogroup"
        aria-label={ariaLabel}
        className={cn("grid gap-3", options.length === 3 ? "sm:grid-cols-3" : "sm:grid-cols-2")}
      >
        {options.map((opt) => {
          const on = opt.value === value;
          const Icon = opt.icon;
          return (
            <button
              key={opt.value}
              type="button"
              role="radio"
              aria-checked={on}
              onClick={() => onChange(opt.value)}
              className={cn(
                "flex flex-col items-center justify-center gap-2 rounded-lg border bg-card text-center text-sm font-medium transition-colors",
                "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/50",
                size === "lg" ? "min-h-24 px-4 py-4" : "min-h-16 px-3 py-2.5 text-xs",
                on
                  ? "border-primary bg-primary/10 text-foreground ring-1 ring-primary"
                  : "text-muted-foreground hover:border-foreground/30 hover:bg-muted/40 hover:text-foreground",
              )}
            >
              <Icon className={cn(size === "lg" ? "h-5 w-5" : "h-4 w-4", on && "text-primary")} aria-hidden />
              {opt.label}
            </button>
          );
        })}
      </div>
      {selected?.info && (
        <p className="flex items-start gap-1.5 text-xs text-muted-foreground">
          <Info className="mt-px h-3.5 w-3.5 shrink-0" aria-hidden />
          <span>{selected.info}</span>
        </p>
      )}
    </div>
  );
}

export function FieldError({ message }: { message?: string }) {
  if (!message) return null;
  return <p className="text-xs font-medium text-destructive">{message}</p>;
}

export function StatusBadge({ status }: { status: MockRule["status"] }) {
  const label = status === "approved" ? "Approved" : status === "draft" ? "Draft" : "Pending approval";
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium whitespace-nowrap ring-1",
        status === "approved"
          ? "bg-emerald-500/10 text-emerald-700 ring-emerald-500/30 dark:text-emerald-300"
          : "bg-amber-500/10 text-amber-700 ring-amber-500/30 dark:text-amber-300",
      )}
    >
      {label}
    </span>
  );
}

/** Same styling as FamilyBadge in apply-rules/MappingChips. */
export function FamilyBadge({ family }: { family: string }) {
  return (
    <span className="inline-block shrink-0 rounded border border-border bg-muted/60 px-1.5 py-0.5 text-[10px] font-medium tracking-wide text-muted-foreground uppercase">
      {family}
    </span>
  );
}

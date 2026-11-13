// Layout primitives for the policy form: ABAC-style two-column sections and choice tiles.
import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { Info } from "lucide-react";
import { cn } from "@/lib/utils";

/** Title + description on the left third, fields on the right two thirds. */
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

/** Bordered radio tiles; *xs* is the compact single-line variant used inside cards. */
export function ChoiceTiles<T extends string>({
  value,
  onChange,
  options,
  size = "lg",
  ariaLabel,
  disabled,
}: {
  value: T;
  onChange: (next: T) => void;
  options: TileOption<T>[];
  size?: "lg" | "xs";
  ariaLabel: string;
  disabled?: boolean;
}) {
  const selected = options.find((o) => o.value === value);
  return (
    <div className="space-y-2.5">
      <div
        role="radiogroup"
        aria-label={ariaLabel}
        className={cn(
          "grid",
          size === "xs" ? "gap-2" : "gap-3",
          options.length === 3 ? "sm:grid-cols-3" : "sm:grid-cols-2",
        )}
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
              disabled={disabled}
              onClick={() => onChange(opt.value)}
              className={cn(
                "flex items-center justify-center rounded-lg border bg-card text-center text-sm font-medium transition-colors",
                "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/50 disabled:cursor-not-allowed",
                size === "lg" ? "min-h-24 flex-col gap-2 px-4 py-4" : "min-h-10 gap-2 px-3 py-2 text-xs",
                on
                  ? "border-primary bg-primary/10 text-foreground ring-1 ring-primary"
                  : "text-muted-foreground enabled:hover:border-foreground/30 enabled:hover:bg-muted/40 enabled:hover:text-foreground",
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

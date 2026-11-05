import { useState } from "react";
import { useTranslation } from "react-i18next";
import { LockKeyhole, LockOpen } from "lucide-react";
import { cn } from "@/lib/utils";
import type { RuleParameter } from "@/lib/api";
import { parameterInputFamily } from "@/lib/apply-time-params";
import { TypedCell } from "@/components/rules/test/TypedCell";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

// No native number spinner: it's tiny, crowds the value and the lock icon,
// and stepping by 1 isn't useful for thresholds or limits.
const CELL_CLASS =
  "h-6 w-full bg-transparent px-2 font-mono text-xs outline-none focus:bg-accent/40 placeholder:text-muted-foreground/50 appearance-none border-0 [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none";

// Content-sized bounds (in characters) for the non-`fill` value box. Without an
// explicit width an <input> falls back to its ~20-character default.
const VALUE_MIN_CH = 6;
const VALUE_MAX_CH = 40;
const LIST_DRAFT_MIN_CH = 3;

function clampCh(length: number, min: number, max: number): number {
  return Math.min(Math.max(length, min), max);
}

export type ParameterLock = {
  locked: boolean;
  /** When set, the icon toggles lock state (registry authoring). Omit on apply-rules. */
  onToggle?: (locked: boolean) => void;
};

function LockAffordance({ locked, onToggle }: ParameterLock) {
  const { t } = useTranslation();
  const label = locked
    ? t("rulesRegistry.parametersPanelCannotOverrideTooltip")
    : t("rulesRegistry.parametersPanelCanOverrideTooltip");
  const icon = locked ? <LockKeyhole className="h-3 w-3" /> : <LockOpen className="h-3 w-3" />;
  const node = onToggle ? (
    <button
      type="button"
      className="inline-flex h-5 w-5 shrink-0 items-center justify-center rounded text-muted-foreground hover:text-foreground"
      aria-label={label}
      aria-pressed={locked}
      onMouseDown={(event) => event.stopPropagation()}
      onClick={(event) => {
        event.stopPropagation();
        onToggle(!locked);
      }}
    >
      {icon}
    </button>
  ) : (
    <span className="inline-flex h-5 w-5 shrink-0 items-center justify-center text-muted-foreground" aria-label={label}>
      {icon}
    </span>
  );
  return (
    <Tooltip>
      <TooltipTrigger asChild>{node}</TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs text-xs">
        {label}
      </TooltipContent>
    </Tooltip>
  );
}

function splitListItems(raw: string): string[] {
  return raw
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function ListValueInput({
  value,
  onChange,
  disabled,
  placeholder,
  lock,
  fill,
}: {
  value: string;
  onChange: (raw: string) => void;
  disabled?: boolean;
  placeholder?: string;
  lock?: ParameterLock;
  fill?: boolean;
}) {
  const items = splitListItems(value);
  const [draft, setDraft] = useState("");
  const commitDraft = (raw: string) => {
    const next = raw.trim().replace(/,$/, "");
    if (!next) {
      setDraft("");
      return;
    }
    onChange([...items, next].join(", "));
    setDraft("");
  };
  return (
    <div
      className={cn(
        "flex min-h-6 flex-wrap items-center gap-1 rounded-md border bg-background px-1.5 py-0.5",
        fill ? "w-full min-w-0" : "min-w-[6.5rem] max-w-[min(100%,28rem)]",
        disabled && "opacity-60",
      )}
    >
      {items.map((item, index) => (
        <span
          key={`${item}-${index}`}
          className="inline-flex items-center gap-0.5 rounded border border-border bg-muted/60 px-1.5 py-0 font-mono text-[11px] text-foreground"
        >
          {item}
          {!disabled && (
            <button
              type="button"
              className="leading-none text-muted-foreground hover:text-foreground"
              aria-label={`Remove ${item}`}
              onClick={() => onChange(items.filter((_, i) => i !== index).join(", "))}
            >
              ×
            </button>
          )}
        </span>
      ))}
      {!disabled && (
        <input
          value={draft}
          placeholder={items.length === 0 ? placeholder : ""}
          className={cn(
            "h-5 bg-transparent font-mono text-xs outline-none placeholder:text-muted-foreground/50",
            fill && "min-w-[5rem] flex-1",
          )}
          // Grow with the draft (or the placeholder when the list is empty)
          // instead of the input's default ~20-character width.
          style={
            fill
              ? undefined
              : {
                  width: `${
                    clampCh(
                      Math.max(draft.length, items.length === 0 ? (placeholder ?? "").length : 0),
                      LIST_DRAFT_MIN_CH,
                      VALUE_MAX_CH,
                    ) + 1
                  }ch`,
                }
          }
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === ",") {
              event.preventDefault();
              commitDraft(draft);
            }
            if (event.key === "Backspace" && draft === "" && items.length > 0) {
              onChange(items.slice(0, -1).join(", "));
            }
          }}
          onBlur={() => commitDraft(draft)}
        />
      )}
      {lock ? <span className="ml-auto"><LockAffordance {...lock} /></span> : null}
    </div>
  );
}

/** Compact typed control for a registry/apply-time parameter value. */
export function ParameterValueInput({
  type,
  value,
  onChange,
  disabled,
  invalid,
  placeholder,
  className,
  lock,
  fill,
  "aria-label": ariaLabel,
}: {
  type: RuleParameter["type"];
  value: string;
  onChange: (raw: string) => void;
  disabled?: boolean;
  invalid?: boolean;
  placeholder?: string;
  className?: string;
  lock?: ParameterLock;
  /** Stretch to the parent width instead of a fixed min-width (apply-rule mapping). */
  fill?: boolean;
  "aria-label"?: string;
}) {
  const { t } = useTranslation();
  const family = parameterInputFamily(type);
  const nonePlaceholder = placeholder ?? t("rulesRegistry.parametersPanelDefaultPlaceholder");
  // Size the box to its value (or placeholder) between the min/max bounds.
  // `fill` stretches to the parent.
  const valueCh = clampCh((value || nonePlaceholder).length, VALUE_MIN_CH, VALUE_MAX_CH);
  const cellStyle = fill ? undefined : { width: `calc(${valueCh}ch + 1rem)` };
  if (type === "list" || type === "ref_column") {
    return (
      <span className={cn(fill ? "inline-flex w-full min-w-0" : "inline-flex min-w-[12rem] max-w-full", invalid && "[&>div]:border-destructive", className)} aria-label={ariaLabel}>
        <ListValueInput
          value={value}
          disabled={disabled}
          placeholder={t("rulesRegistry.parametersPanelListPlaceholder")}
          onChange={onChange}
          lock={lock}
          fill={fill}
        />
      </span>
    );
  }
  return (
    <span
      className={cn(
        "inline-flex h-6 items-center rounded-md border bg-background pr-0.5",
        fill ? "w-full min-w-0" : "max-w-full shrink-0",
        disabled && "opacity-60",
        invalid && "border-destructive",
        className,
      )}
      aria-label={ariaLabel}
    >
      <TypedCell
        family={family}
        value={value === "" ? null : value}
        disabled={disabled}
        placeholder={nonePlaceholder}
        className={cn(CELL_CLASS, fill && "min-w-0")}
        noMinWidth={fill}
        style={cellStyle}
        onChange={(next) => onChange(next ?? "")}
      />
      {lock ? <LockAffordance {...lock} /> : null}
    </span>
  );
}

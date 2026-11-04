import { useState } from "react";
import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import type { RuleParameter } from "@/lib/api";
import { parameterInputFamily } from "@/lib/apply-time-params";
import { TypedCell } from "@/components/rules/test/TypedCell";

const CELL_CLASS =
  "h-6 w-full bg-transparent px-2 font-mono text-xs outline-none focus:bg-accent/40 placeholder:text-muted-foreground/50 appearance-none border-0";

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
}: {
  value: string;
  onChange: (raw: string) => void;
  disabled?: boolean;
  placeholder?: string;
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
        "flex min-h-6 min-w-[12rem] max-w-full flex-wrap items-center gap-1 rounded-md border bg-background px-1.5 py-0.5",
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
          className="h-5 min-w-[5rem] flex-1 bg-transparent font-mono text-xs outline-none placeholder:text-muted-foreground/50"
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
  "aria-label": ariaLabel,
}: {
  type: RuleParameter["type"];
  value: string;
  onChange: (raw: string) => void;
  disabled?: boolean;
  invalid?: boolean;
  placeholder?: string;
  className?: string;
  "aria-label"?: string;
}) {
  const { t } = useTranslation();
  const family = parameterInputFamily(type);
  const nonePlaceholder = placeholder ?? t("rulesRegistry.parametersPanelDefaultPlaceholder");
  if (type === "list" || type === "ref_column") {
    return (
      <span className={cn("inline-flex min-w-[12rem] max-w-full", invalid && "[&>div]:border-destructive", className)} aria-label={ariaLabel}>
        <ListValueInput
          value={value}
          disabled={disabled}
          placeholder={t("rulesRegistry.parametersPanelListPlaceholder")}
          onChange={onChange}
        />
      </span>
    );
  }
  return (
    <span
      className={cn(
        "inline-flex h-6 w-28 shrink-0 items-center overflow-hidden rounded-md border bg-background",
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
        className={CELL_CLASS}
        onChange={(next) => onChange(next ?? "")}
      />
    </span>
  );
}

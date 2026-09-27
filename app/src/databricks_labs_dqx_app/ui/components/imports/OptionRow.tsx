import { useId } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { HelpTooltip } from "@/components/HelpTooltip";
import { cn } from "@/lib/utils";

export function OptionRow({
  checked,
  onChange,
  disabled,
  label,
  hint,
  compact = false,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  disabled?: boolean;
  label: string;
  hint: string;
  /** Keep the row to a single line, moving *hint* into a ``?`` tooltip. For
   *  settings whose label already says it, where a wrapped grey sentence costs
   *  more room than the explanation is worth. */
  compact?: boolean;
}) {
  const checkboxId = useId();

  if (compact) {
    return (
      // Not a <label> wrapper: the tooltip trigger is a button, and a click on
      // it inside a label would toggle the checkbox.
      <div
        className={cn(
          "flex items-center gap-2 rounded-lg border p-3 transition-colors",
          disabled ? "opacity-60" : "hover:bg-muted/30",
        )}
      >
        <Checkbox
          id={checkboxId}
          checked={checked}
          onCheckedChange={(v) => onChange(Boolean(v))}
          disabled={disabled}
        />
        <label
          htmlFor={checkboxId}
          className={cn(
            "min-w-0 flex-1 truncate select-none text-sm font-medium",
            disabled ? "cursor-not-allowed" : "cursor-pointer",
          )}
          title={label}
        >
          {label}
        </label>
        <HelpTooltip text={hint} />
      </div>
    );
  }

  return (
    <label
      className={cn(
        "flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-colors",
        disabled ? "opacity-60 cursor-not-allowed" : "hover:bg-muted/30",
      )}
    >
      <Checkbox
        checked={checked}
        onCheckedChange={(v) => onChange(Boolean(v))}
        disabled={disabled}
        className="mt-0.5"
      />
      <div className="flex-1 space-y-1">
        <div className="text-sm font-medium">{label}</div>
        <div className="text-xs text-muted-foreground">{hint}</div>
      </div>
    </label>
  );
}

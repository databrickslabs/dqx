import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import type { RuleParameter } from "@/lib/api";
import { parameterInputFamily } from "@/lib/apply-time-params";
import { TypedCell } from "@/components/rules/test/TypedCell";

const CELL_CLASS =
  "h-6 w-full bg-transparent px-2 font-mono text-xs outline-none focus:bg-accent/40 placeholder:text-muted-foreground/50 appearance-none border-0";

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
        placeholder={placeholder ?? t("rulesRegistry.parametersPanelDefaultPlaceholder")}
        className={CELL_CLASS}
        onChange={(next) => onChange(next ?? "")}
      />
    </span>
  );
}

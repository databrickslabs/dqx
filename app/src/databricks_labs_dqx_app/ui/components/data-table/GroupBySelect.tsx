import type { LucideIcon } from "lucide-react";
import { Layers } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { cn } from "@/lib/utils";
import { FILTER_TRIGGER_CLASS } from "./filter-bar";

export interface GroupBySelectOption<V extends string> {
  value: V;
  label: string;
  /** Shown beside the label in the option list and the selected value; omit for a text-only select. */
  icon?: LucideIcon;
}

export interface GroupBySelectProps<V extends string> {
  value: V;
  onChange: (value: V) => void;
  options: readonly GroupBySelectOption<V>[];
  className?: string;
}

/**
 * The shared "Group by" control: a muted Layers icon and label, then a compact
 * select. Options with an *icon* show it beside the label (the collection
 * tables picker); overview filter bars pass text-only options. Used everywhere
 * grouping is offered so it looks the same; the trigger shares the filter
 * pills' size token.
 */
export function GroupBySelect<V extends string>({ value, onChange, options, className }: GroupBySelectProps<V>) {
  const { t } = useTranslation();
  const label = t("common.groupBy");
  return (
    <div className={cn("flex items-center gap-1.5", className)}>
      <Layers className="h-3.5 w-3.5 text-muted-foreground" aria-hidden />
      <span className="text-xs text-muted-foreground">{label}</span>
      <Select
        value={value}
        onValueChange={(next) => {
          const option = options.find((o) => o.value === next);
          if (option) onChange(option.value);
        }}
      >
        <SelectTrigger className={FILTER_TRIGGER_CLASS} aria-label={label}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {options.map(({ value: optionValue, label: optionLabel, icon: Icon }) => (
            <SelectItem key={optionValue} value={optionValue} className="text-xs">
              {Icon ? (
                <span className="flex items-center gap-1.5">
                  <Icon className="h-3 w-3" /> {optionLabel}
                </span>
              ) : (
                optionLabel
              )}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

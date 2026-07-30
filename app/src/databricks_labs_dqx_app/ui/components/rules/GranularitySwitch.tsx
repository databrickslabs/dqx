import { useTranslation } from "react-i18next";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import type { RuleGranularity } from "@/lib/lowcodeCompile";

/**
 * Row-level / table-level indicator for the rule being authored, built as the
 * same sliding two-way pill as {@link PredicatePolaritySwitch} so the two
 * semantic axes of a rule ("what counts as a pass", "what gets a verdict") read
 * as the same kind of control.
 *
 * Granularity is a property of EVERY rule but only sometimes the author's to
 * pick, so it renders as a control only where the pick is real:
 *
 *   • a native check runs at the granularity DQX registered it at (33 of the
 *     exposed checks are row-level, 12 dataset-level) — derived;
 *   • the visual builder derives it from what was built, since joins and
 *     grouping compile to merge keys that keep the check row-level — derived;
 *   • raw SQL holding a full query genuinely chooses, because presence of
 *     `merge_columns` is the whole difference — live, this component.
 *
 * Derived states render {@link GranularityTag} instead: a disabled two-way pill
 * invites a click it will always refuse, whereas a tag reads as the status it
 * actually is. Never render either from a stored flag — derive it from what will
 * actually run (`bodyGranularity`, or the check's `rule_type`) so the label
 * cannot drift from DQX's behaviour.
 */
export function GranularitySwitch({
  value,
  onChange,
  disabled,
  disabledReason,
}: {
  value: RuleGranularity;
  onChange?: (next: RuleGranularity) => void;
  disabled?: boolean;
  disabledReason?: string;
}) {
  const { t } = useTranslation();
  const options: { value: RuleGranularity; label: string; activeTone: string; hoverTone: string }[] = [
    {
      value: "row",
      label: t("rulesRegistry.granularityRowLevel"),
      activeTone: "bg-sky-500/15 text-sky-700 dark:text-sky-300 ring-1 ring-sky-500/40",
      hoverTone: "hover:bg-sky-500/10 hover:text-sky-700 dark:hover:text-sky-300",
    },
    {
      value: "dataset",
      label: t("rulesRegistry.granularityDatasetLevel"),
      activeTone: "bg-muted text-foreground ring-1 ring-border",
      hoverTone: "hover:bg-muted/60 hover:text-foreground",
    },
  ];
  const activeIndex = options.findIndex((o) => o.value === value);
  const active = options[activeIndex] ?? options[0];
  const invert = () => onChange?.(value === "row" ? "dataset" : "row");

  const control = (
    // Same 28px pill as the polarity switch and the rule-type chip: these read
    // as one family of controls, so they share height, text size and radius.
    <div
      className="relative inline-grid h-7 grid-cols-2 items-stretch rounded-full border bg-muted/30 p-1 text-xs"
      role="radiogroup"
      aria-label={t("rulesRegistry.granularityLabel")}
    >
      <span
        aria-hidden
        className={cn(
          "absolute top-1 bottom-1 left-1 w-[calc(50%-4px)] rounded-full transition-all duration-200 ease-out",
          active.activeTone,
        )}
        style={{ transform: `translateX(${activeIndex === 1 ? "100%" : "0%"})` }}
      />
      {options.map((opt) => {
        const on = value === opt.value;
        return (
          <button
            key={opt.value}
            type="button"
            role="radio"
            aria-checked={on}
            disabled={disabled}
            // Clicking either segment inverts, matching the polarity switch —
            // the whole pill behaves as one toggle.
            onClick={invert}
            className={cn(
              "relative z-10 flex items-center justify-center rounded-full px-3 font-medium whitespace-nowrap transition-colors duration-200 ease-out",
              on ? "text-foreground" : "text-muted-foreground",
              !on && !disabled && opt.hoverTone,
              "disabled:cursor-not-allowed disabled:opacity-70",
            )}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );

  if (disabledReason) {
    return (
      <TooltipProvider delayDuration={200}>
        <Tooltip>
          {/* `span` wrapper keeps the tooltip working over disabled buttons,
              which don't emit pointer events themselves. */}
          <TooltipTrigger asChild>
            <span className={cn("inline-flex", disabled && "cursor-not-allowed")}>{control}</span>
          </TooltipTrigger>
          <TooltipContent className="max-w-xs">{disabledReason}</TooltipContent>
        </Tooltip>
      </TooltipProvider>
    );
  }
  return control;
}

/** Tone per granularity for the tag. Keep in step with the switch's
 *  `activeTone` above (same hue per level, different class shape) so one level
 *  isn't two colours across the two components. Dataset-level is NEUTRAL grey:
 *  it is a property of the check, not a problem with it, and a saturated colour
 *  next to a check name in the picker reads as a warning. */
const GRANULARITY_TONE: Record<RuleGranularity, string> = {
  row: "text-sky-700 dark:text-sky-300 bg-sky-500/10 ring-sky-500/30",
  dataset: "text-muted-foreground bg-muted ring-border",
};

/**
 * Read-only granularity indicator: what the rule WILL run at, where that is not
 * the author's choice. Used on the Condition header for native checks and the
 * visual builder, and in the check picker to mark dataset-level checks before
 * one is chosen (so the level is known before selecting rather than after).
 *
 * *reason* explains WHY it isn't a choice and becomes the tooltip.
 */
export function GranularityTag({
  value,
  reason,
  className,
}: {
  value: RuleGranularity;
  reason?: string;
  className?: string;
}) {
  const { t } = useTranslation();
  const tag = (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium whitespace-nowrap ring-1",
        GRANULARITY_TONE[value],
        className,
      )}
    >
      {value === "row" ? t("rulesRegistry.granularityRowLevel") : t("rulesRegistry.granularityDatasetLevel")}
    </span>
  );
  if (!reason) return tag;
  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="inline-flex">{tag}</span>
        </TooltipTrigger>
        <TooltipContent className="max-w-xs">{reason}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

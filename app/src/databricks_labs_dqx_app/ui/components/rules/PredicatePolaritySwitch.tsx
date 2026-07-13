import { useTranslation } from "react-i18next";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

/** DQX-native polarity value — `"pass"` when a TRUE predicate means the row
 * passed, `"fail"` when a TRUE predicate means the row failed. Matches the
 * `polarity` field persisted on `RegistryRuleOut`/`CreateRegistryRuleIn`. */
export type Polarity = "pass" | "fail";

/**
 * Sliding two-way toggle ported from dqlake's `PredicatePolaritySwitch` —
 * "row PASSES" / "row FAILS". Unlike a plain on/off `Switch`, both states
 * are always visible as labeled segments; the sliding highlight glides between
 * the two (and cross-fades its colour) rather than jumping. Clicking ANYWHERE
 * on the pill inverts the current value (item 13) — you don't have to hit the
 * opposite segment.
 *
 * When `disabled` and a `disabledReason` is supplied, the whole control is
 * wrapped in a tooltip explaining why it can't be changed (item 11 — a native
 * check with no `negate` argument shows the switcher frozen at its inherent
 * polarity with "Not supported by this rule").
 */
export function PredicatePolaritySwitch({
  value,
  onChange,
  disabled,
  disabledReason,
}: {
  value: Polarity;
  onChange: (next: Polarity) => void;
  disabled?: boolean;
  disabledReason?: string;
}) {
  const { t } = useTranslation();
  const options: { value: Polarity; label: string; activeTone: string; hoverTone: string }[] = [
    {
      value: "pass",
      label: t("rulesRegistry.polarityPassOption"),
      activeTone: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 ring-1 ring-emerald-500/40",
      hoverTone: "hover:bg-emerald-500/10 hover:text-emerald-700 dark:hover:text-emerald-300",
    },
    {
      value: "fail",
      label: t("rulesRegistry.polarityFailOption"),
      activeTone: "bg-rose-500/15 text-rose-700 dark:text-rose-300 ring-1 ring-rose-500/40",
      hoverTone: "hover:bg-rose-500/10 hover:text-rose-700 dark:hover:text-rose-300",
    },
  ];
  const activeIndex = options.findIndex((o) => o.value === value);
  const active = options[activeIndex] ?? options[0];
  const invert = () => onChange(value === "pass" ? "fail" : "pass");

  const control = (
    <div
      className="relative inline-grid grid-cols-2 items-stretch rounded-full border bg-muted/30 p-1 text-xs"
      role="radiogroup"
      aria-label={t("rulesRegistry.polarityAriaLabel")}
    >
      {/* Sliding indicator, translated between slots so a polarity flip
          glides; `transition-all` also cross-fades its colour (emerald <->
          rose) rather than snapping (item 13). */}
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
            // Clicking anywhere on the pill inverts — both segments flip to the
            // opposite value, so the whole control behaves as one toggle.
            onClick={invert}
            className={cn(
              "relative z-10 rounded-full px-3 py-1 font-medium transition-colors duration-200 ease-out",
              on ? "text-foreground" : "text-muted-foreground",
              !on && !disabled && opt.hoverTone,
              "disabled:cursor-not-allowed disabled:opacity-50",
            )}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );

  if (disabled && disabledReason) {
    return (
      <TooltipProvider delayDuration={200}>
        <Tooltip>
          {/* `span` wrapper keeps the tooltip working over disabled buttons,
              which don't emit pointer events themselves. */}
          <TooltipTrigger asChild>
            <span className="inline-flex cursor-not-allowed">{control}</span>
          </TooltipTrigger>
          <TooltipContent>{disabledReason}</TooltipContent>
        </Tooltip>
      </TooltipProvider>
    );
  }
  return control;
}

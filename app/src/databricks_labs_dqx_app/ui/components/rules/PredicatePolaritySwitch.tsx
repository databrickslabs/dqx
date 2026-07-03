import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";

/** DQX-native polarity value — `"pass"` when a TRUE predicate means the row
 * passed, `"fail"` when a TRUE predicate means the row failed. Matches the
 * `polarity` field persisted on `RegistryRuleOut`/`CreateRegistryRuleIn`. */
export type Polarity = "pass" | "fail";

/**
 * Sliding two-way toggle ported from dqlake's `PredicatePolaritySwitch` —
 * "row PASSES" / "row FAILS". Unlike a plain on/off `Switch`, both states
 * are always visible as labeled segments; clicking the segment opposite the
 * current selection inverts it (clicking the already-active segment is a
 * no-op), and a sliding highlight glides between the two rather than
 * cross-fading.
 */
export function PredicatePolaritySwitch({
  value,
  onChange,
  disabled,
}: {
  value: Polarity;
  onChange: (next: Polarity) => void;
  disabled?: boolean;
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

  return (
    <div
      className="relative inline-grid grid-cols-2 items-stretch rounded-full border bg-muted/30 p-1 text-xs"
      role="radiogroup"
      aria-label={t("rulesRegistry.polarityAriaLabel")}
    >
      {/* Sliding indicator, translated between slots so a polarity flip
          glides rather than cross-fades. */}
      <span
        aria-hidden
        className={cn(
          "absolute top-1 bottom-1 left-1 w-[calc(50%-4px)] rounded-full transition-transform duration-200 ease-out",
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
            onClick={() => {
              // Clicking either segment sets it directly, which for a
              // two-valued toggle is equivalent to inverting whenever the
              // opposite segment is clicked.
              if (!on) onChange(opt.value);
            }}
            className={cn(
              "relative z-10 rounded-full px-3 py-1 font-medium transition-colors duration-200 ease-out",
              on ? "text-foreground" : "text-muted-foreground",
              !on && opt.hoverTone,
              "disabled:cursor-not-allowed disabled:opacity-50",
            )}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}

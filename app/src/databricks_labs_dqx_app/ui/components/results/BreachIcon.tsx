import { useTranslation } from "react-i18next";
import { AlertTriangle } from "lucide-react";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

/**
 * A small warning triangle shown when a results group has a threshold breach.
 * Renders nothing when criticality is null/undefined or an unrecognised value.
 * The icon is always amber (item 45 — no variable colour for threshold fails);
 * error vs warn is still conveyed by the tooltip text, not the colour.
 */
export function BreachIcon({
  criticality,
  className,
}: {
  /** Accepts the raw API string so callers need no cast. Only "error"/"warn"
   *  render; any other value (null, undefined, unknown string) renders nothing. */
  criticality: string | null | undefined;
  className?: string;
}) {
  const { t } = useTranslation();

  if (criticality !== "error" && criticality !== "warn") return null;

  const isError = criticality === "error";
  const tooltip = isError
    ? t("resultsUi.breachErrorTooltip")
    : t("resultsUi.breachWarnTooltip");

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          className={cn(
            "inline-flex shrink-0 items-center",
            // Always amber (item 45): the warning triangle no longer varies
            // colour by criticality — a deeper amber in light and a brighter
            // amber in dark, with a heavier stroke so the small triangle reads
            // clearly against tinted rows. error vs warn lives in the tooltip.
            "text-amber-600 dark:text-amber-300",
            className,
          )}
          aria-label={tooltip}
        >
          <AlertTriangle className="h-3.5 w-3.5" strokeWidth={2.5} />
        </span>
      </TooltipTrigger>
      <TooltipContent side="top">{tooltip}</TooltipContent>
    </Tooltip>
  );
}

import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import { AI_GRADIENT_URL } from "@/lib/ai-style";

// Ported from dqlake's `components/results/GenieIcon.tsx` (the spec).
// Sanctioned deviations: the gradient def id comes from AI_GRADIENT_URL
// (this app's `dqx-ai-gradient`, defined once in routes/__root.tsx) and the
// accessible title is a t() string.

/**
 * Databricks Genie mark — the first-party icon: a panel/window with a sparkle
 * in its upper-right.
 *
 * Renders filled with the shared AI gradient by default (the gradient def
 * lives in __root.tsx), so it reads as a single mark rather than a stroked
 * outline. Callers can override the fill (e.g. `[&_path]:fill-white` on the
 * dark "Ask Genie" launcher).
 */
export function GenieIcon({
  className,
  title,
}: {
  className?: string;
  title?: string;
}) {
  const { t } = useTranslation();
  const resolvedTitle = title ?? t("genie.iconTitle");
  return (
    <svg
      viewBox="0 0 16 16"
      fill={AI_GRADIENT_URL}
      role="img"
      aria-label={resolvedTitle}
      className={cn("shrink-0", className)}
    >
      <title>{resolvedTitle}</title>
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M0 2.75A.75.75 0 0 1 .75 2H8v1.5H1.5v9h13V10H16v3.25a.75.75 0 0 1-.75.75H.75a.75.75 0 0 1-.75-.75zm12.987-.14a.75.75 0 0 0-1.474 0l-.137.728a1.93 1.93 0 0 1-1.538 1.538l-.727.137a.75.75 0 0 0 0 1.474l.727.137c.78.147 1.39.758 1.538 1.538l.137.727a.75.75 0 0 0 1.474 0l.137-.727c.147-.78.758-1.39 1.538-1.538l.727-.137a.75.75 0 0 0 0-1.474l-.727-.137a1.93 1.93 0 0 1-1.538-1.538z"
      />
    </svg>
  );
}

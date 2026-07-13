import type * as React from "react";
import { useEffect, useState } from "react";
import { motion, useReducedMotion } from "motion/react";
import { useTranslation } from "react-i18next";
import { ArrowDown, ArrowUp, CircleHelp } from "lucide-react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { countUpValue } from "@/components/home/statFormat";

/**
 * Compact overall-score badge. Background colour interpolates red→green by
 * pass rate. No data fetching — callers pass the numbers in.
 */
export function scoreColor(passRate: number | null): string {
  if (passRate == null) return "hsl(0 0% 60%)";
  return `hsl(${Math.round(passRate * 120)} 70% 42%)`;
}

/**
 * Animate a number from `from` → `target` (easeOutCubic via rAF), re-running
 * whenever `from`/`target` change so the score always lands exactly on the real
 * value. `from` defaults to 0 (count up); pass 100 to count down toward a lower
 * score. Honours `prefers-reduced-motion`: reduced-motion users get the final
 * value with no ticking. `countUpValue(1, t)` is the shared easeOutCubic 0..1
 * progress fraction (same curve as the home "At a Glance" cards).
 */
function useCountUp(target: number, from = 0, durationMs = 900): number {
  const reduce = useReducedMotion();
  const [value, setValue] = useState(target);
  useEffect(() => {
    if (reduce) {
      setValue(target);
      return;
    }
    let raf = 0;
    let startTs = 0;
    const tick = (now: number) => {
      if (!startTs) startTs = now;
      const t = Math.min(1, (now - startTs) / durationMs);
      setValue(from + (target - from) * countUpValue(1, t));
      if (t < 1) raf = requestAnimationFrame(tick);
    };
    setValue(from);
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [target, from, durationMs, reduce]);
  return reduce ? target : value;
}

export function ScoreBox({
  passRate,
  failedTests,
  totalTests,
  trend,
  info,
  label,
}: {
  passRate: number | null;
  failedTests: number;
  totalTests: number;
  /** Heading above the percentage. Defaults to "Overall score"; the data-product
   *  view passes "Average score" (its score is the mean of member tables). */
  label?: string;
  /** Direction of this run's score vs the previous run: "up" (improved), "down"
   *  (fell), or null/undefined (equal, or no previous run). Renders a small
   *  green up- / red down-arrow beside the percentage. */
  trend?: "up" | "down" | null;
  /** When set, a muted help "?" icon follows the "X failed of Y tests"
   *  subtitle; hovering it shows this content in a tooltip. */
  info?: React.ReactNode;
}) {
  const { t } = useTranslation();
  const reduce = useReducedMotion();
  // B2-83: make the count-up directional off the same `trend` signal that drives
  // the up/down arrow. A declined score (trend "down") counts DOWN from 100%;
  // an improved/equal score, or a first run with no comparison (trend up/null),
  // counts UP from 0% as before. Either way the rAF curve lands exactly on the
  // real value at t≥1 (reduced-motion → static).
  const animatedPct = useCountUp(
    passRate == null ? 0 : passRate * 100,
    trend === "down" ? 100 : 0,
  );
  const pct = passRate == null ? "—" : `${animatedPct.toFixed(1)}%`;
  const bg = scoreColor(passRate);
  return (
    <div
      className="rounded-lg p-4 text-center text-white"
      style={{ background: bg }}
      data-testid="score-box"
    >
      <div className="text-[11px] uppercase tracking-wide opacity-85">
        {label ?? t("resultsUi.overallScoreLabel")}
      </div>
      <div className="flex items-center justify-center gap-1.5 leading-none">
        {/* Fade + scale the big number in on mount (skipped for reduced-motion).
            tabular-nums keeps the width stable while the digits tick up. */}
        <motion.span
          className="text-4xl font-bold tabular-nums"
          initial={reduce ? false : { opacity: 0, scale: 0.85 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.4, ease: "easeOut" }}
        >
          {pct}
        </motion.span>
        {trend === "up" && (
          <ArrowUp
            className="h-6 w-6"
            // Full-contrast solid green-600 (not a muted tint) so the
            // direction reads at a glance against the coloured score box.
            style={{ color: "#16a34a" }}
            aria-label={t("resultsUi.scoreTrendUpAria")}
            data-testid="score-trend-up"
          />
        )}
        {trend === "down" && (
          <ArrowDown
            className="h-6 w-6"
            // Full-contrast solid red-600 (not a muted tint).
            style={{ color: "#dc2626" }}
            aria-label={t("resultsUi.scoreTrendDownAria")}
            data-testid="score-trend-down"
          />
        )}
      </div>
      {totalTests > 0 && (
        <div className="mt-1.5 flex items-center justify-center gap-1 text-xs opacity-90">
          <span>
            {t("resultsUi.failedOfTests", {
              failed: failedTests.toLocaleString(),
              total: totalTests.toLocaleString(),
            })}
          </span>
          {info && (
            <TooltipProvider delayDuration={200}>
              <Tooltip>
                <TooltipTrigger asChild>
                  <button
                    type="button"
                    aria-label={t("resultsUi.whatDoTheseMeanAria")}
                    className="text-white/80 hover:text-white"
                  >
                    <CircleHelp className="h-3.5 w-3.5" />
                  </button>
                </TooltipTrigger>
                <TooltipContent side="top" className="max-w-xs">
                  {info}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          )}
        </div>
      )}
    </div>
  );
}
